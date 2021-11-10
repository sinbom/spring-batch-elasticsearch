package org.springframework.batch.item.elasticsearch.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.elasticsearch.ElasticsearchTestContext;
import org.springframework.batch.item.elasticsearch.TestDomain;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchBulkItemWriterTest extends ElasticsearchTestContext {

    @Test
    public void writer가_정상적으로_값을저장한다() throws Exception {
        // given
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        TestDomain expected2 = new TestDomain("test2", 2, LocalDateTime.now());
        TestDomain expected3 = new TestDomain("test3", 3, LocalDateTime.now());
        List<TestDomain> expectedItems = Arrays.asList(
                expected,
                expected2,
                expected3
        );

        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                TestDomain.class,
                TEST_INDEX_NAME
        )
                .timeoutMillis(60000)
                .waitUntil(true);

        // when
        writer.write(expectedItems);

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX_NAME)
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
        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                TestDomain.class,
                TEST_INDEX_NAME
        )
                .timeoutMillis(60000)
                .waitUntil(true);

        // when
        writer.write(Collections.emptyList());

        SearchRequest searchRequest = new SearchRequest(TEST_INDEX_NAME)
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

    @Test
    public void writer가_저장하다가_실패한값이_있는경우_실패한다() throws IOException {
        // given
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        List<TestDomain> expectedItems = Collections.singletonList(expected);
        RestHighLevelClient mockedRestHighLevelClient = mock(RestHighLevelClient.class);
        BulkResponse bulkResponse = mock(BulkResponse.class);

        when(
                mockedRestHighLevelClient.bulk(
                        any(),
                        any()
                )
        )
                .thenReturn(bulkResponse);
        when(bulkResponse.hasFailures())
                .thenReturn(true);

        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                mockedRestHighLevelClient,
                TestDomain.class,
                TEST_INDEX_NAME
        );

        // when & then
        assertThrows(
                ElasticsearchException.class,
                () -> writer.write(expectedItems)
        );
    }

    @Test
    public void writer가_값을_저장하다가_직렬화에러가_발생하면_실패한다() throws Exception {
        // given
        TestDomain expected = new TestDomain("test", 1, LocalDateTime.now());
        List<TestDomain> expectedItems = Collections.singletonList(expected);
        ObjectMapper mockedObjectMapper = mock(ObjectMapper.class);

        when(mockedObjectMapper.writeValueAsString(any(TestDomain.class)))
                .thenThrow(JsonProcessingException.class);

        ElasticsearchBulkItemWriter<TestDomain> writer = new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                mockedObjectMapper,
                TestDomain.class,
                TEST_INDEX_NAME
        );

        // when & then
        assertThrows(
                JsonProcessingException.class,
                () -> writer.write(expectedItems)
        );
    }

}
