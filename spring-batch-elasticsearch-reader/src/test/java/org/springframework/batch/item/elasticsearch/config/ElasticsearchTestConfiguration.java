package org.springframework.batch.item.elasticsearch.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

@Configuration
public class ElasticsearchTestConfiguration {

    private static final String USERNAME = "elastic";

    private static final String PASSWORD = "elastic";

    private static final String ELASTICSEARCH_VERSION = "7.10.1";

    private static final String ELASTICSEARCH_DOCKER_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch";

    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER;

    static {
        ELASTICSEARCH_CONTAINER = createElasticsearchContainer();
        ELASTICSEARCH_CONTAINER.start();
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        return new ElasticsearchContainer(createDockerImage())
                .withPassword(PASSWORD);
    }

    private static DockerImageName createDockerImage() {
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
        RestClientBuilder restClientBuilder = RestClient
                .builder(HttpHost.create(ELASTICSEARCH_CONTAINER.getHttpHostAddress()))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(createCredentialsProvider()));

        return new RestHighLevelClient(restClientBuilder);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .registerModule(new JavaTimeModule());
    }

}
