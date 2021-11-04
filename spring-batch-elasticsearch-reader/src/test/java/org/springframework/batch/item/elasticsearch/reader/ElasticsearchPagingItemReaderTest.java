package org.springframework.batch.item.elasticsearch.reader;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.elasticsearch.TestDomain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ElasticsearchPagingItemReaderTest extends ElasticsearchTestContext {

    @Test
    public void reader가_정상적으로_오름차순_값을반환한다() throws Exception {
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

        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name", SortOrder.ASC),
                TestDomain.class,
                indexName
        );

        // when
        TestDomain actual = reader.read();
        TestDomain actual2 = reader.read();
        TestDomain actual3 = reader.read();
        TestDomain actual4 = reader.read();

        // then
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected2.getName(), actual2.getName());
        assertEquals(expected3.getName(), actual3.getName());
        assertNull(actual4);
    }

    @Test
    public void reader가_정상적으로_내림차순_값을반환한다() throws Exception {
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

        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name", SortOrder.DESC),
                TestDomain.class,
                indexName
        );

        // when
        TestDomain actual = reader.read();
        TestDomain actual2 = reader.read();
        TestDomain actual3 = reader.read();
        TestDomain actual4 = reader.read();

        // then
        assertEquals(expected3.getName(), actual.getName());
        assertEquals(expected2.getName(), actual2.getName());
        assertEquals(expected.getName(), actual3.getName());
        assertNull(actual4);
    }

    @Test
    public void reader가_값이_없는경우_null을_반환한다() throws Exception {
        // given
        int chunkSize = 1;
        String indexName = "test";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

        restHighLevelClient
                .indices()
                .create(createIndexRequest, RequestOptions.DEFAULT);

        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name"),
                TestDomain.class,
                indexName
        );

        // when
        TestDomain actual = reader.read();

        // then
        assertNull(actual);
    }

}
