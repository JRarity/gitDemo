package com.jra.restapi;

import com.google.gson.Gson;
import com.jra.pojo.Item;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Create with Itellij IDEA.
 *
 * @Author JRarity
 * @Date 2019/8/19 20:23
 * @Version 1.0
 */
public class RestHighLevelClientDemo {

    private RestHighLevelClient client = null;

    private Gson gson = new Gson();

    @Before
    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9201, "http"),
                        new HttpHost("localhost", 9202, "http"),
                        new HttpHost("localhost", 9203, "http")));

    }

    // 添加或者修改数据
    @Test
    public void addDocument() throws IOException {
        // 对象来封装信息
        Item item = new Item(1L, "小米6XProw手机", "手机", "小米", 1199.0, "www.jra.com/1.jpg");
        // 创建用来创建文档的请求对象
        IndexRequest indexRequest = new IndexRequest("leyou", "item", item.getId().toString());
        // 使用gson把对象转换为json字符串
        String itemJson = gson.toJson(item);
        indexRequest.source(itemJson, XContentType.JSON);
        // 使用客户端创建
        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    //删除数据
    @Test
    public void deleteDocument() throws IOException {
        // 创建用来删除文档的请求对象
        DeleteRequest deleteRequest = new DeleteRequest("leyou", "item", "1");
        // 使用客户端删除
        client.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void addDocuments() throws IOException {
        // 准备文档数据：
        List<Item> list = new ArrayList<>();
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));

        // 创建批量导入文档的请求对象
        BulkRequest bulkRequest = new BulkRequest();
        for (Item item : list) {
            bulkRequest.add(new IndexRequest("leyou", "item", item.getId().toString()).source(gson.toJson(item),
                    XContentType.JSON));
        }

        // 使用客户端导入数据
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    //查询数据
    @Test
    public void queryDocument() throws IOException {
        // 构建大的查询对象，所有的查询方式都可以使用   可以指定索引库的名称
        SearchRequest searchRequest = new SearchRequest("leyou");
        // 用来构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建查询方式
        // matchAll 查询所有
        // searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // term 查询
        // searchSourceBuilder.query(QueryBuilders.termQuery("title","小米"));
        // 分词查询
        // searchSourceBuilder.query(QueryBuilders.matchQuery("title","小米手机"));
        // 区间查询
        // searchSourceBuilder.query(QueryBuilders.rangeQuery("price").gte(1000).lte(5000));
        // 组合查询
        // searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("title","小米"))
        // .must(QueryBuilders.rangeQuery("price").gte(1000).lte(5000)));
        // 容错查询
        // searchSourceBuilder.query(QueryBuilders.fuzzyQuery("title","大米").fuzziness(Fuzziness.ONE));
        // 模糊查询
        searchSourceBuilder.query(QueryBuilders.wildcardQuery("title", "*手*"));

        // 把查询方式放入searchRequest
        searchRequest.source(searchSourceBuilder);
        // 执行查询 返回searchResponse
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 结果处理
        SearchHits searchResponseHits = searchResponse.getHits();
        System.out.println("查询总条数:" + searchResponseHits.getTotalHits());
        SearchHit[] searchHits = searchResponseHits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            Item item = gson.fromJson(sourceAsString, Item.class);
            System.out.println(item);
        }
    }

    //过滤,分页,排序
    @Test
    public void otherDocument() throws IOException {
        // 构建大的查询对象，所有的查询方式都可以使用   可以指定索引库的名称
        SearchRequest searchRequest = new SearchRequest("leyou");
        // 用来构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建查询方式
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 过滤显示字段
        // searchSourceBuilder.fetchSource(new String[]{"id","title"},null);
        // 过滤数据
        // searchSourceBuilder.postFilter(QueryBuilders.boolQuery()
        //         .must(QueryBuilders.rangeQuery("price").gte(1000).lte(5000))
        //         .must(QueryBuilders.termQuery("brand","小米")));
        // 分页查询
        // searchSourceBuilder.from(0).size(3);
        // 排序
        searchSourceBuilder.sort("price", SortOrder.DESC);
        // 把查询方式放入searchRequest
        searchRequest.source(searchSourceBuilder);
        // 执行查询 返回searchResponse
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 结果处理
        SearchHits searchResponseHits = searchResponse.getHits();
        System.out.println("查询总条数:" + searchResponseHits.getTotalHits());
        SearchHit[] searchHits = searchResponseHits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            // Item item = gson.fromJson(sourceAsString, Item.class);
            // System.out.println(item);
            System.out.println(sourceAsString);
        }
    }

    // 高亮显示
    @Test
    public void highLightDocument() throws IOException {
        // 构建大的查询对象，所有的查询方式都可以使用   可以指定索引库的名称
        SearchRequest searchRequest = new SearchRequest("leyou");
        // 用来构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建查询方式
        searchSourceBuilder.query(QueryBuilders.termQuery("title", "手机"));
        // 高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span style='color:red'>").postTags("</span>").field("title");
        searchSourceBuilder.highlighter(highlightBuilder);
        // 把查询方式放入searchRequest
        searchRequest.source(searchSourceBuilder);
        // 执行查询 返回searchResponse
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 结果处理
        SearchHits searchResponseHits = searchResponse.getHits();
        System.out.println("查询总条数:" + searchResponseHits.getTotalHits());
        SearchHit[] searchHits = searchResponseHits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            Item item = gson.fromJson(sourceAsString, Item.class);

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.getFragments();
            if (fragments != null && fragments.length != 0) {
                String title_highlight = fragments[0].toString();
                item.setTitle(title_highlight);
            }

            System.out.println(item);
        }
    }

    // 聚合查询
    @Test
    public void aggregationsDocument() throws IOException {
        // 构建大的查询对象，所有的查询方式都可以使用   可以指定索引库的名称
        SearchRequest searchRequest = new SearchRequest("leyou");
        // 用来构建查询方式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构建查询方式
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 聚合查询
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("brand_aggs").field("brand");
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        // 把查询方式放入searchRequest
        searchRequest.source(searchSourceBuilder);
        // 执行查询 返回searchResponse
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 结果处理
        Terms terms = searchResponse.getAggregations().get("brand_aggs");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKeyAsString() + " : " + bucket.getDocCount());
        }
    }

    @After
    public void end() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
