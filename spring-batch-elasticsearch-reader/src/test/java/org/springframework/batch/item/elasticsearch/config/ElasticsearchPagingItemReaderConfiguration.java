package org.springframework.batch.item.elasticsearch.config;

import lombok.RequiredArgsConstructor;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.elasticsearch.TestDomain;
import org.springframework.batch.item.elasticsearch.TestMigrationDomain;
import org.springframework.batch.item.elasticsearch.reader.ElasticsearchPagingItemReader;
import org.springframework.batch.item.elasticsearch.writer.ElasticsearchBulkItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ElasticsearchPagingItemReaderConfiguration {

    public static final String JOB_NAME = "elasticsearchPagingItemReaderJob";

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final RestHighLevelClient restHighLevelClient;

    private final ElasticsearchPagingItemReaderJobParameter jobParameter;

    @Value("${chunkSize:1000}")
    private int chunkSize;

    @JobScope
    @Bean(JOB_NAME + "Parameter")
    public ElasticsearchPagingItemReaderJobParameter jobParameter() {
        return new ElasticsearchPagingItemReaderJobParameter();
    }

    @Bean(JOB_NAME)
    public Job job() {
        return jobBuilderFactory
                .get(JOB_NAME)
                .start(step())
                .build();
    }

    @Bean(JOB_NAME + "Step")
    public Step step() {
        return stepBuilderFactory
                .get(JOB_NAME + "Step")
                .<TestDomain, TestMigrationDomain>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @StepScope
    @Bean(JOB_NAME + "Reader")
    public ItemReader<TestDomain> reader() {
        return new ElasticsearchPagingItemReader<>(
                restHighLevelClient,
                SearchSourceBuilder
                        .searchSource()
                        .size(chunkSize)
                        .query(
                                QueryBuilders
                                        .rangeQuery("created")
                                        .gte(jobParameter.getCreated())
                        )
                        .sort("name", SortOrder.ASC),
                TestDomain.class,
                "test"
        );
    }

    private ItemProcessor<TestDomain, TestMigrationDomain> processor() {
        return TestMigrationDomain::new;
    }

    @Bean(JOB_NAME + "Writer")
    public ItemWriter<TestMigrationDomain> writer() {
        return new ElasticsearchBulkItemWriter<>(
                restHighLevelClient,
                TestMigrationDomain.class,
                "test_migration"
        )
                .waitUntil(true);
    }

}
