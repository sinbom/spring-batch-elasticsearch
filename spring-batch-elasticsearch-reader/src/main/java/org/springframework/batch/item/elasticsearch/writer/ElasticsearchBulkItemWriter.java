package org.springframework.batch.item.elasticsearch.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class ElasticsearchBulkItemWriter<T> implements ItemWriter<T>, InitializingBean {

    private final RestHighLevelClient restHighLevelClient;

    private final ObjectMapper objectMapper;

    private final Class<T> domainClass;

    private final String index;

    private TimeValue timeout;

    private boolean waitUntil;

    public ElasticsearchBulkItemWriter(RestHighLevelClient restHighLevelClient, Class<T> domainClass, String index) {
        this(
                restHighLevelClient,
                new ObjectMapper()
                        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                        .registerModule(new JavaTimeModule()),
                domainClass,
                index
        );
    }

    public ElasticsearchBulkItemWriter(RestHighLevelClient restHighLevelClient, ObjectMapper objectMapper,
                                       Class<T> domainClass, String index) {
        this.restHighLevelClient = restHighLevelClient;
        this.domainClass = domainClass;
        this.objectMapper = objectMapper;
        this.index = index;
    }

    public ElasticsearchBulkItemWriter<T> timeoutMillis(long timeOutMillis) {
        this.timeout = TimeValue.timeValueMillis(timeOutMillis);

        return this;
    }

    public ElasticsearchBulkItemWriter<T> waitUntil(boolean waitUntil) {
        this.waitUntil = waitUntil;

        return this;
    }

    @Override
    public void write(List<? extends T> items) throws Exception {
        if (!CollectionUtils.isEmpty(items)) {
            BulkRequest bulkRequest = new BulkRequest();

            if (timeout != null) {
                bulkRequest.timeout(timeout);
            }

            if (waitUntil) {
                bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            }

            for (T item : items) {
                IndexRequest indexRequest = new IndexRequest(index)
                        .source(objectMapper.writeValueAsString(item), XContentType.JSON);
                bulkRequest.add(indexRequest);
            }

            BulkResponse bulkResponse = restHighLevelClient
                    .bulk(bulkRequest, RequestOptions.DEFAULT);

            if (bulkResponse.hasFailures()) {
                throw new ElasticsearchException("Bulk response includes failure writing.");
            }
        }
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(restHighLevelClient, "restHighLevelClient is must be not null");
        Assert.notNull(domainClass, "domain class is must be not null");
        Assert.hasText(index, "index name is must not be null or empty");
    }

}
