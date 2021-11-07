package org.springframework.batch.item.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.springframework.batch.item.elasticsearch.config.ElasticsearchTestConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
@SpringBootTest(classes = ElasticsearchTestConfiguration.class)
public abstract class ElasticsearchTestContext {

    @Autowired
    protected RestHighLevelClient restHighLevelClient;

    protected ObjectMapper objectMapper = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .registerModule(new JavaTimeModule());

    @BeforeAll
    public static void putIndexTemplate(@Autowired RestHighLevelClient restHighLevelClient) throws IOException {
        XContentBuilder mapping = XContentFactory
                .jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
        PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest("test-template")
                .patterns(Collections.singletonList("test*"))
                .mapping(mapping);

        AcknowledgedResponse acknowledgedResponse = restHighLevelClient
                .indices()
                .putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);

        assertTrue(acknowledgedResponse.isAcknowledged());
    }

    @BeforeEach
    public void deleteAllIndices() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
        AcknowledgedResponse delete = restHighLevelClient
                .indices()
                .delete(deleteIndexRequest, RequestOptions.DEFAULT);

        assertTrue(delete.isAcknowledged());
    }

}
