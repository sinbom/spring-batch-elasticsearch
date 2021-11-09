# Spring Batch ElasticsearchItemReader & Writer

[![Build Status](https://app.travis-ci.com/sinbom/spring-batch-elasticsearch.svg?branch=master)](https://app.travis-ci.com/sinbom/spring-batch-elasticsearch)
[![Dependency Status](https://jitpack.io/v/sinbom/spring-batch-elasticsearch.svg)](https://jitpack.io/#sinbom/spring-batch-elasticsearch)

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
    implementation 'com.github.sinbom:spring-batch-elasticsearch:master-SNAPSHOT'
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
    <version>master-SNAPSHOT</version>
</dependency>
```

## ElasticsearchPagingItemReader

```java
@Bean
public ElasticsearchPagingItemReader<TestDomain> reader() {
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

## ElasticsearchBulkItemWriter

```java
@Bean
public ItemWriter<TestMigrationDomain> writer() {
    return new ElasticsearchBulkItemWriter<>(
            restHighLevelClient,
            Domain.class,
            "index_name"
    );
}
```

