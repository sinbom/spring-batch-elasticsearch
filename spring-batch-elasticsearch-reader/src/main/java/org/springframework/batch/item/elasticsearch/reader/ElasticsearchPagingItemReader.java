package org.springframework.batch.item.elasticsearch.reader;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class ElasticsearchPagingItemReader<T> extends AbstractPagingItemReader<T> {

    private final RestHighLevelClient restHighLevelClient;

    private final SearchSourceBuilder searchSourceBuilder;

    private final String[] indices;

    private Object[] sortValues;

    public ElasticsearchPagingItemReader(RestHighLevelClient restHighLevelClient, int pageSize,
                                         SearchSourceBuilder searchSourceBuilder, String... indices) {
        setName(ClassUtils.getShortName(getClass()));
        setPageSize(pageSize);
        this.restHighLevelClient = restHighLevelClient;
        this.searchSourceBuilder = searchSourceBuilder.size(pageSize);
        this.indices = indices;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doReadPage() {
        SearchRequest searchRequest = createQuery();

        initResults();
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sortValues = hit.getSortValues();
                results.add((T) hit);
            }
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private SearchRequest createQuery() {
        if (sortValues != null && sortValues.length > 0) {
            searchSourceBuilder.searchAfter(sortValues);
        }

        return new SearchRequest(indices)
                .source(searchSourceBuilder);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(restHighLevelClient, "restHighLevelClient is must be not null");
        Assert.notNull(searchSourceBuilder, "searchSourceBuilder is must be not null");
        Assert.notEmpty(indices, "index names is must be not empty");
        Assert.notEmpty(searchSourceBuilder.sorts(), "sort key is must be not empty");
    }

    @Override
    protected void doJumpToPage(int itemIndex) {
    }

    protected void initResults() {
        if (CollectionUtils.isEmpty(results)) {
            results = new CopyOnWriteArrayList<>();
        } else {
            results.clear();
        }
    }

}
