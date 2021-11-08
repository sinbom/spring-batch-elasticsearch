package org.springframework.batch.item.elasticsearch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.item.elasticsearch.config.ElasticsearchPagingItemReaderConfiguration;
import org.springframework.batch.item.elasticsearch.config.ElasticsearchTestConfiguration;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.time.format.DateTimeFormatter;

@TestPropertySource(properties = "chunkSize=1")
@EnableAutoConfiguration
@EnableBatchProcessing
@SpringBatchTest
@SpringBootTest(
        classes = {
                ElasticsearchTestConfiguration.class,
                ElasticsearchPagingItemReaderConfiguration.class
        }
)
public abstract class ElasticsearchIntegrationTestContext extends ElasticsearchTestContext {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    protected JobLauncherTestUtils jobLauncherTestUtils;

}
