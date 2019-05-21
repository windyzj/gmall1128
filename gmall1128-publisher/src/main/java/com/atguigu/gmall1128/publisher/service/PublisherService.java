package com.atguigu.gmall1128.publisher.service;

import java.util.Map;

public interface PublisherService {

    //1 查询日活总数
    public  Integer getDauTotal(String date);

    //2 查询日活分时汇总数据
    public Map getDauHoursMap(String date);
}
