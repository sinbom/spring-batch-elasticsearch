package org.springframework.batch.item.elasticsearch.reader;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.springframework.batch.item.elasticsearch.ElasticsearchTestConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@Disabled
@SpringBootTest(classes = ElasticsearchTestConfiguration.class)
public abstract class ElasticsearchTestBase {

    @Autowired
    protected RestHighLevelClient restHighLevelClient;

    @BeforeEach
    public void deleteAllIndices() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("*");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

}
