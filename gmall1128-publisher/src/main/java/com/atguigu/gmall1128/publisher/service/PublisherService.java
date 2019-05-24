package com.atguigu.gmall1128.publisher.service;

import com.atguigu.gmall1128.publisher.bean.AggRangeOpt;
import com.atguigu.gmall1128.publisher.bean.SaleInfo;

import java.util.List;
import java.util.Map;

public interface PublisherService {

    //1 查询日活总数
    public  Integer getDauTotal(String date);

    //2 查询日活分时汇总数据
    public Map getDauHoursMap(String date);

    // 查询单日收入总数
    public Double getOrderAmount(String date );

    // 查询单日分时收入
    public  Map getOrderAmountHoursMap(String date );

    //用户灵活查询
    public SaleInfo  getSaleInfo(String date,String keyword , int pageNo,int pagesize,String aggFieldName,int aggSize,List<AggRangeOpt> aggRangeOptList);

}