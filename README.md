# Spring Batch ElasticsearchItemReader & Writer

[![Build Status](https://app.travis-ci.com/sinbom/spring-batch-elasticsearch.svg?branch=master)](https://app.travis-ci.com/sinbom/spring-batch-elasticsearch)
[![Dependency Status](https://jitpack.io/v/sinbom/spring-batch-elasticsearch.svg)](https://jitpack.io/#sinbom/spring-batch-elasticsearch)
[![Coverage Status](https://coveralls.io/repos/github/sinbom/spring-batch-elasticsearch/badge.svg?branch=master)](https://coveralls.io/github/sinbom/spring-batch-elasticsearch?branch=master)

## Requires

* Java 8
* Spring Batch
* RHLC(rest-high-level-client)

## Dependency

### gradle

```groovy
allprojects {
    repositories {
        maven { url 'https://jitpack.io' }
    }
}

dependencies {
    implementation 'com.github.sinbom:spring-batch-elasticsearch:1.0.0'
}
```

### maven
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.sinbom</groupId>
    <artifactId>spring-batch-elasticsearch</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Guide

### ElasticsearchPagingItemReader

```java
@Bean
public ItemReader<Domain> reader() {
    return new ElasticsearchPagingItemReader<>(
            restHighLevelClient,
            SearchSourceBuilder
                .searchSource()
                .size(chunkSize)
                .query(QueryBuilders.matchAllQuery())
                .sort("name", SortOrder.ASC),
            Domain.class,
            "index_name"
    );
}
```

### ElasticsearchBulkItemWriter

```java
@Bean
public ItemWriter<Domain> writer() {
    return new ElasticsearchBulkItemWriter<>(
            restHighLevelClient,
            Domain.class,
            "index_name"
    );
}
```

