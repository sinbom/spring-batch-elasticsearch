package org.springframework.batch.item.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Configuration
public class ElasticsearchTestConfiguration {

    private static final String USERNAME = "elastic";

    private static final String PASSWORD = "elastic";

    private static final String ELASTICSEARCH_VERSION = "7.10.1";

    private static final String ELASTICSEARCH_DOCKER_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

    private final ElasticsearchContainer elasticsearchContainer;

    public ElasticsearchTestConfiguration() {
        this.elasticsearchContainer = createElasticsearchContainer();
        this.elasticsearchContainer.start();
    }

    private ElasticsearchContainer createElasticsearchContainer() {
        DockerImageName dockerImageName = createDockerImage();

        return new ElasticsearchContainer(dockerImageName)
                .withPassword(PASSWORD);
    }

    private DockerImageName createDockerImage() {
        return DockerImageName
                .parse(ELASTICSEARCH_DOCKER_IMAGE)
                .withTag(ELASTICSEARCH_VERSION);
    }

    private CredentialsProvider createCredentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(USERNAME, PASSWORD);

        credentialsProvider.setCredentials(AuthScope.ANY, credentials);

        return credentialsProvider;
    }

    @Bean
    public RestHighLevelClient createRestHighLevelClient() {
        CredentialsProvider credentialsProvider = createCredentialsProvider();
        RestClientBuilder restClientBuilder = RestClient
                .builder(HttpHost.create(elasticsearchContainer.getHttpHostAddress()))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(restClientBuilder);
    }

}
