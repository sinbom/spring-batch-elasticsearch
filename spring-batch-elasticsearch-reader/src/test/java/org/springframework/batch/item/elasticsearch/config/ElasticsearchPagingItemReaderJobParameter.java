package org.springframework.batch.item.elasticsearch.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;

import java.time.LocalDate;

@Getter
public class ElasticsearchPagingItemReaderJobParameter {

    @Value("#{jobParameters[startDate]}")
    private LocalDate localDate;

}
