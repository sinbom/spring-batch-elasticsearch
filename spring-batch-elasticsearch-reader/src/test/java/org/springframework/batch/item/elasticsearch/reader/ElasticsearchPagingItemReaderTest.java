package org.springframework.batch.item.elasticsearch.reader;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.elasticsearch.TestDomain;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElasticsearchPagingItemReaderTest extends ElasticsearchTestBase {

    @Test
    @DisplayName("reader 정상적으로 조회")
    public void reader() throws Exception {
        // given
        int chunkSize = 1;
        String indexName = "test";
        TestDomain expected = new TestDomain("test", 1);
        TestDomain expected2 = new TestDomain("test2", 2);
        TestDomain expected3 = new TestDomain("test3", 3);
        IndexRequest indexRequest = new IndexRequest(indexName)
                .source(objectMapper.writeValueAsString(expected), XContentType.JSON);
        IndexRequest indexRequest2 = new IndexRequest(indexName)
                .source(objectMapper.writeValueAsString(expected2), XContentType.JSON);
        IndexRequest indexRequest3 = new IndexRequest(indexName)
                .source(objectMapper.writeValueAsString(expected3), XContentType.JSON);
        BulkRequest bulkRequest = new BulkRequest()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .add(indexRequest, indexRequest2, indexRequest3);
        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .sort("name", SortOrder.ASC);
        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                chunkSize,
                searchSourceBuilder,
                TestDomain.class,
                indexName
        );

        // when
        TestDomain actual = reader.read();
        TestDomain actual2 = reader.read();
        TestDomain actual3 = reader.read();

        // then
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected2.getName(), actual2.getName());
        assertEquals(expected3.getName(), actual3.getName());
    }

}
