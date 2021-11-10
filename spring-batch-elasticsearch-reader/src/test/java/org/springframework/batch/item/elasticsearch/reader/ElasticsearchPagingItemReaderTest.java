package org.springframework.batch.item.elasticsearch.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.elasticsearch.ElasticsearchTestContext;
import org.springframework.batch.item.elasticsearch.TestDomain;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchPagingItemReaderTest extends ElasticsearchTestContext {

    @Test
    public void reader가_정상적으로_오름차순_값을반환한다() throws Exception {
        // given
        int chunkSize = 1;
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        TestDomain expected2 = new TestDomain("test2", 2, LocalDateTime.now());
        TestDomain expected3 = new TestDomain("test3", 3, LocalDateTime.now());
        IndexRequest indexRequest = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected), XContentType.JSON);
        IndexRequest indexRequest2 = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected2), XContentType.JSON);
        IndexRequest indexRequest3 = new IndexRequest(TEST_INDEX_NAME)
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
                TEST_INDEX_NAME
        );

        reader.open(new ExecutionContext());

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
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        TestDomain expected2 = new TestDomain("test2", 2, LocalDateTime.now());
        TestDomain expected3 = new TestDomain("test3", 3, LocalDateTime.now());
        IndexRequest indexRequest = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected), XContentType.JSON);
        IndexRequest indexRequest2 = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected2), XContentType.JSON);
        IndexRequest indexRequest3 = new IndexRequest(TEST_INDEX_NAME)
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
                TEST_INDEX_NAME
        );

        reader.open(new ExecutionContext());

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
    public void reader가_doJumpToPage_구현내용이_존재하지않아_호출하기전과_동일한_값을반환한다() throws Exception {
        // given
        int chunkSize = 1;
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        TestDomain expected2 = new TestDomain("test2", 2, LocalDateTime.now());
        TestDomain expected3 = new TestDomain("test3", 3, LocalDateTime.now());
        List<TestDomain> expectedItems = Arrays.asList(
                expected,
                expected2,
                expected3
        );
        IndexRequest indexRequest = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected), XContentType.JSON);
        IndexRequest indexRequest2 = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected2), XContentType.JSON);
        IndexRequest indexRequest3 = new IndexRequest(TEST_INDEX_NAME)
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
                TEST_INDEX_NAME
        );

        reader.open(new ExecutionContext());

        // when
        reader.doJumpToPage(expectedItems.size());
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
    public void reader가_값이_없는경우_null을_반환한다() throws Exception {
        // given
        int chunkSize = 1;
        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name"),
                TestDomain.class,
                TEST_INDEX_NAME
        );
        reader.open(new ExecutionContext());

        // when
        TestDomain actual = reader.read();

        // then
        assertNull(actual);
    }

    @Test
    public void reader가_값을_읽다가_입출력예외가_발생하면_실패한다() throws IOException {
        // given
        int chunkSize = 1;
        RestHighLevelClient mockedRestHighLevelClient = mock(RestHighLevelClient.class);

        when(
                mockedRestHighLevelClient.search(
                        any(SearchRequest.class),
                        eq(RequestOptions.DEFAULT)
                )
        )
                .thenThrow(IOException.class);

        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                mockedRestHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name"),
                TestDomain.class,
                TEST_INDEX_NAME
        );

        reader.open(new ExecutionContext());

        // when & then
        assertThrows(
                ElasticsearchException.class,
                reader::read
        );
    }

    @Test
    public void reader가_값을_읽다가_직렬화에러가_발생하면_실패한다() throws IOException {
        // given
        int chunkSize = 1;
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        IndexRequest indexRequest = new IndexRequest(TEST_INDEX_NAME)
                .source(objectMapper.writeValueAsString(expected), XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        ObjectMapper mockedObjectMapper = mock(ObjectMapper.class);

        when(
                mockedObjectMapper.readValue(
                        any(String.class),
                        eq(TestDomain.class)
                )
        )
                .thenThrow(JsonProcessingException.class);

        ElasticsearchPagingItemReader<TestDomain> reader = new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(QueryBuilders.matchAllQuery())
                        .sort("name"),
                mockedObjectMapper,
                TestDomain.class,
                TEST_INDEX_NAME
        );

        reader.open(new ExecutionContext());

        // when & then
        assertThrows(
                RuntimeException.class,
                reader::read
        );
    }

}
