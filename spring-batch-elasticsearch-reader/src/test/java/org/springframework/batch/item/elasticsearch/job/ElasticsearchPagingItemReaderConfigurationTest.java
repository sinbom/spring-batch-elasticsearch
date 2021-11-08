package org.springframework.batch.item.elasticsearch.job;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.item.elasticsearch.ElasticsearchIntegrationTestContext;
import org.springframework.batch.item.elasticsearch.TestDomain;
import org.springframework.batch.item.elasticsearch.TestMigrationDomain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElasticsearchPagingItemReaderConfigurationTest extends ElasticsearchIntegrationTestContext {

    @Test
    public void 정상적으로_값을읽어_이관한다() throws Exception {
        // given
        LocalDate created = LocalDate.of(1996, 9, 17);
        String indexName = "test";
        String migratedIndexName = "test";
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        TestDomain expected2 = new TestDomain("test2", 2, LocalDateTime.now());
        TestDomain expected3 = new TestDomain("test3", 3, LocalDateTime.now());
        List<TestDomain> expectedItems = Arrays.asList(
                expected,
                expected2,
                expected3
        );

        BulkRequest bulkRequest = new BulkRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        for (TestDomain expectedItem : expectedItems) {
            IndexRequest indexRequest = new IndexRequest(indexName)
                    .source(objectMapper.writeValueAsString(expectedItem), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("created", DATE_FORMATTER.format(created))
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        SearchRequest searchRequest = new SearchRequest(migratedIndexName)
                .source(
                        SearchSourceBuilder
                                .searchSource()
                                .size(expectedItems.size())
                                .query(QueryBuilders.matchAllQuery())
                                .sort("name", SortOrder.ASC)
                );

        SearchHit[] actualItems = restHighLevelClient
                .search(searchRequest, RequestOptions.DEFAULT)
                .getHits()
                .getHits();

        TestMigrationDomain actual = objectMapper.readValue(actualItems[0].getSourceAsString(), TestMigrationDomain.class);
        TestMigrationDomain actual2 = objectMapper.readValue(actualItems[1].getSourceAsString(), TestMigrationDomain.class);
        TestMigrationDomain actual3 = objectMapper.readValue(actualItems[2].getSourceAsString(), TestMigrationDomain.class);

        // then
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(expectedItems.size(), actualItems.length);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected2.getName(), actual2.getName());
        assertEquals(expected3.getName(), actual3.getName());
    }

}
