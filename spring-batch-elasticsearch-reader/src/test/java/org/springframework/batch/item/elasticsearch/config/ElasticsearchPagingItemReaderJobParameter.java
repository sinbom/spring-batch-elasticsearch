package org.springframework.batch.item.elasticsearch.config;

import lombok.Getter;
import org.springframework.batch.item.elasticsearch.ElasticsearchIntegrationTestContext;
import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDate;

@Getter
public class ElasticsearchPagingItemReaderJobParameter {

    private LocalDate created;

    @Value("#{jobParameters[created]}")
    public void setCreateDate(String created) {
        this.created = LocalDate.parse(created, ElasticsearchIntegrationTestContext.DATE_FORMATTER);
    }

}
