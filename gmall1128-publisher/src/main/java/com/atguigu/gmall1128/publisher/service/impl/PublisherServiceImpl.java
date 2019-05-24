package com.atguigu.gmall1128.publisher.service.impl;

import com.atguigu.gmall1128.common.constant.GmallConstant;
import com.atguigu.gmall1128.publisher.bean.AggRangeOpt;
import com.atguigu.gmall1128.publisher.bean.SaleInfo;
import com.atguigu.gmall1128.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.Range;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
//        String query = "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"logDate\": \"2019-05-21\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHoursMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合操作
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Map dauHourMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //从bucket中获取 分时数据
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                dauHourMap.put( bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        sourceBuilder.query(boolQueryBuilder);
        //聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        sourceBuilder.aggregation(sumBuilder);

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Double sum_totalamount=0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
              sum_totalamount = searchResult.getAggregations().getSumAggregation("sum_totalamount").getSum();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return sum_totalamount;
    }





    @Override
    public Map getOrderAmountHoursMap(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        sourceBuilder.query(boolQueryBuilder);
        //聚合  把聚合操作嵌入分组操作中
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        termsBuilder.subAggregation(sumBuilder);

        sourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Map<String ,Double> orderAmountHourMap=new HashMap<>();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                Double sum_totalamount = bucket.getSumAggregation("sum_totalamount").getSum(); //归总值
                orderAmountHourMap.put( bucket.getKey(), sum_totalamount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return orderAmountHourMap;
    }

    @Override
    public SaleInfo getSaleInfo(String date, String keyword, int pageNo, int pagesize, String aggFieldName, int aggSize,List<AggRangeOpt> aggRangeList ) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //过滤
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));

        sourceBuilder.query(boolQueryBuilder);
        //聚合
        if(aggRangeList==null||aggRangeList.size()==0){
            TermsBuilder termAggs = AggregationBuilders.terms("groupby_" + aggFieldName).field(aggFieldName).size(aggSize);
            sourceBuilder.aggregation(termAggs);
        }else{
            RangeBuilder rangeBuilder = AggregationBuilders.range("groupby_" + aggFieldName).field(aggFieldName);
            for (AggRangeOpt aggRangeOpt : aggRangeList) {
                rangeBuilder.addRange(aggRangeOpt.getKey(),aggRangeOpt.getFrom(),aggRangeOpt.getTo());
            }
            sourceBuilder.aggregation(rangeBuilder);
        }



        //分页处理
        sourceBuilder.from((pageNo-1)*pagesize);
        sourceBuilder.size(pagesize);

        System.out.println(sourceBuilder.toString());


        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType(GmallConstant.ES_DEFAULT_TYPE).build();


        Integer total=0;
        List<HashMap> detail =new ArrayList<>();
        HashMap aggsMap=new HashMap();//每个分组的个数
        try {
            SearchResult searchResult = jestClient.execute(search);
            total= searchResult.getTotal();
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                detail.add(hit.source);
            }
            //聚合结果
            if(aggRangeList==null||aggRangeList.size()==0) { //单值聚合
                List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggFieldName).getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    aggsMap.put(bucket.getKey(), bucket.getCount());
                }
            }else{ //分段聚合
                List<Range> buckets = searchResult.getAggregations().getRangeAggregation("groupby_" + aggFieldName).getBuckets();
                Map paramMap=new HashMap();
                for (AggRangeOpt aggRangeOpt : aggRangeList) {
                    paramMap.put(aggRangeOpt.getKey(),Map.class);
                }

                for (Range bucket : buckets) {
                    bucket.getAggregations(paramMap);
                    aggsMap.put( bucket.getName(),bucket.getCount());
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        SaleInfo saleInfo = new SaleInfo(total,null,detail,aggsMap);

        return saleInfo;
    }
}
