package com.atguigu.gmallrealtime.mapper.impl;

import com.atguigu.gmallrealtime.mapper.PublisherMapper;
import com.atguigu.gmallrealtime.pojo.NameValue;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.sql.Array;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    // ES索引前缀
    private static final String dauIndexNamePre = "gmall_dau_info_";
    private static final String orderIndexNamePre = "gmall_order_wide_";
    // 创建ES客户端
    @Autowired
    RestHighLevelClient esClient;

    /**
     * 明细统计
     */
    @Override
    public Map<String, Object> detailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {

        HashMap<String, Object> resultMap = new HashMap<>();

        // 索引名
        String indexName = orderIndexNamePre + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 明细字段
        searchSourceBuilder.fetchSource(new String[]{
                "create_time",
                "order_price",
                "province_name",
                "sku_name",
                "sku_num",
                "total_amount",
                "user_age",
                "user_gender"
        }, null);

        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);

        // 分页
        searchSourceBuilder.from(pageNo);
        searchSourceBuilder.size(pageSize);

        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            // 获取商品总数
            long total = searchResponse.getHits().getTotalHits().value;

            // 获取详细信息
            SearchHit[] hits = searchResponse.getHits().getHits();
            if(hits != null && hits.length > 0) {
                ArrayList<Map<String, Object>> list = new ArrayList<>();
                for (SearchHit hit : hits) {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    // 获取高亮
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    HighlightField highlightField = highlightFields.get("sku_name");
                    Text[] fragments = highlightField.getFragments();
                    sourceAsMap.put("sku_name", fragments[0].toString());
                    list.add(sourceAsMap);
                }

                // 返回结果
                resultMap.put("total", total);
                resultMap.put("detail", list);
            }
        } catch (ElasticsearchStatusException es) {
            if(es.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " not exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询失败");
        }

        return resultMap;
    }

    /**
     * 性别年龄统计
     */
    @Override
    public List<NameValue> statsByItem(String itemName, String date, String type) {

        List<NameValue> nameValueList = new ArrayList<>(200);

        // 索引名
        String indexName = orderIndexNamePre + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 不查询明细
        searchSourceBuilder.size(0);

        // query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);

        // group
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupBy" + type).field(type).size(200);

        // sum
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("totalAmount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms aggregation = aggregations.get("groupBy" + type);
            List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
            if(buckets != null && buckets.size() > 0) {

                for (Terms.Bucket bucket : buckets) {

                    String key = bucket.getKeyAsString();
                    Aggregations bucketAggregations = bucket.getAggregations();
                    ParsedSum parsedSum = bucketAggregations.get("totalAmount");
                    double sumValue = parsedSum.getValue();
                    nameValueList.add(new NameValue(key, sumValue));
                }

                return nameValueList;
            }
        } catch (ElasticsearchStatusException es) {
            if(es.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " not exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询失败");
        }

        return nameValueList;
    }

    /**
     * 日活统计
     */
    @Override
    public Map<String, Object> dauRealtime(String td) {

        HashMap<String, Object> map = new HashMap<>();
        // 总数
        Long dauTotal = getDauTotal(td);
        map.put("dauTotal", dauTotal);

        // 昨日数据
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        map.put("dauYd", getTdData(ydLd.toString()));

        // 今日数据
        map.put("dauTd", getTdData(td));

        return map;
    }

    /**
     * 查询当日数据
     */
    public Map<String, Long> getTdData(String td) {

        HashMap<String, Long> tdMap = new HashMap<>();

        // 索引名
        String indexName = dauIndexNamePre + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupByHr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            ParsedStringTerms parsedStringTerms = searchResponse.getAggregations().get("groupByHr");
            List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
            if(buckets != null && buckets.size() > 0) {
                for (Terms.Bucket bucket : buckets) {
                    String hr = bucket.getKeyAsString();
                    long count = bucket.getDocCount();
                    tdMap.put(hr, count);
                }
                return tdMap;
            }
        } catch (ElasticsearchStatusException es) {
            if(es.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " not exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询失败");
        }

        return tdMap;
    }

    /**
     * 获取总条数
     */
    public Long getDauTotal(String td) {

        // 索引名
        String indexName = dauIndexNamePre + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 不展示明细数据
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse.getHits().getTotalHits().value;
        } catch (ElasticsearchStatusException es) {
            if(es.status() == RestStatus.NOT_FOUND) {
                log.warn(indexName + " not exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("es 查询失败");
        }

        return 0L;
    }
}
