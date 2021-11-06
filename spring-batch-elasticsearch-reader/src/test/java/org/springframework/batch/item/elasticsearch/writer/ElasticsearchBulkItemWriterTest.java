package org.springframework.batch.item.elasticsearch.writer;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.elasticsearch.ElasticsearchTestContext;
import org.springframework.batch.item.elasticsearch.TestDomain;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ElasticsearchBulkItemWriterTest extends ElasticsearchTestContext {

    @Test
    public void writer가_정상적으로_값을저장한다() throws Exception {
        // given
        String indexName = "test";
        TestDomain expected = new TestDomain("test", 1);
        TestDomain expected2 = new TestDomain("test2", 2);
        TestDomain expected3 = new TestDomain("test3", 3);
        List<TestDomain> expectedItems = Arrays.asList(
                expected,
                expected2,
                expected3
        );

        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                TestDomain.class,
                indexName
        )
                .waitUntil(true);

        // when
        writer.write(expectedItems);

        SearchRequest searchRequest = new SearchRequest(indexName)
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

        TestDomain actual = objectMapper.readValue(actualItems[0].getSourceAsString(), TestDomain.class);
        TestDomain actual2 = objectMapper.readValue(actualItems[1].getSourceAsString(), TestDomain.class);
        TestDomain actual3 = objectMapper.readValue(actualItems[2].getSourceAsString(), TestDomain.class);

        // then
        assertEquals(expectedItems.size(), actualItems.length);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected2.getName(), actual2.getName());
        assertEquals(expected3.getName(), actual3.getName());
    }

    @Test
    public void writer가_값이_없는경우_값을_저장하지않는다() throws Exception {
        // given
        String indexName = "test";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

        restHighLevelClient
                .indices()
                .create(createIndexRequest, RequestOptions.DEFAULT);

        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                TestDomain.class,
                indexName
        )
                .waitUntil(true);

        // when
        writer.write(Collections.emptyList());

        SearchRequest searchRequest = new SearchRequest(indexName)
                .source(
                        SearchSourceBuilder
                                .searchSource()
                                .query(QueryBuilders.matchAllQuery())
                                .sort("name", SortOrder.ASC)
                );

        SearchHit[] actualItems = restHighLevelClient
                .search(searchRequest, RequestOptions.DEFAULT)
                .getHits()
                .getHits();

        // then
        assertEquals(actualItems.length, 0);
    }

}
