package com.atguigu.gmall1128.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1128.publisher.bean.AggRangeOpt;
import com.atguigu.gmall1128.publisher.bean.Option;
import com.atguigu.gmall1128.publisher.bean.OptionGroup;
import com.atguigu.gmall1128.publisher.bean.SaleInfo;
import com.atguigu.gmall1128.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date){

        List<Map> totalList=new ArrayList();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",12000);
        totalList.add(newMidMap);

        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增收入");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);


        return   JSON.toJSONString(totalList);
    }

    /**
     * 求分时统计
     * @param id
     * @param today
     * @return
     */
    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id")String id ,@RequestParam("date") String today){
        String result="";
        if("dau".equals(id)){  //日活分时
            Map dauHoursTDMap = publisherService.getDauHoursMap(today);
            String yesterday = getYesterday(today);
            Map dauHoursYDMap = publisherService.getDauHoursMap(yesterday);

            Map dauHourMap=new HashMap();
            dauHourMap.put("today",dauHoursTDMap);
            dauHourMap.put("yesterday",dauHoursYDMap);
            result=JSON.toJSONString(dauHourMap);
        }else if("order_amount".equals(id)){  //收入的分时
            Map orderAmountHoursTDMap = publisherService.getOrderAmountHoursMap(today);
            String yesterday = getYesterday(today);
            Map orderAmountHoursYDMap = publisherService.getOrderAmountHoursMap(yesterday);

            Map orderAmountHourMap=new HashMap();
            orderAmountHourMap.put("today",orderAmountHoursTDMap);
            orderAmountHourMap.put("yesterday",orderAmountHoursYDMap);
            result=JSON.toJSONString(orderAmountHourMap);

        }
        return result;
    }



    public String  getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String  yesterday=null;
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            yesterday=simpleDateFormat.format(yesterdayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterday;
    }


    /**
     * 灵活查询
     */
    @GetMapping("sale_detail")
   public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("startpage") int startpage,@RequestParam("size") int size ){
        //1 先从后台查询性别的统计及明细
        SaleInfo saleInfoWithGender = publisherService.getSaleInfo(date, keyword, startpage, size, "user_gender", 2, null);
        Integer total = saleInfoWithGender.getTotal();
        HashMap genderMap = saleInfoWithGender.getAggsMap();

        List<OptionGroup> optionGroupList=new ArrayList<>();

        List<Option> genderOptionList=new ArrayList<>();
        for (Object o : genderMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;

            String key = (String)entry.getKey(); //M 或者 F
            Long count = (Long)entry.getValue(); //个数
            if("F".equals(key)){
                Double ratio = Math.round( count*1000D/total)/10D;
                genderOptionList.add( new Option("女",   ratio  )) ;
            }else{
                Double ratio = Math.round( count*1000D/total)/10D;
                genderOptionList.add( new Option("男",   ratio  ));
            }

        }

        OptionGroup genderOptionGroup = new OptionGroup(genderOptionList,"性别占比");

        optionGroupList.add(genderOptionGroup);

        //计算年龄段占比
        List aggRangeOptList=new ArrayList();
        aggRangeOptList.add(new AggRangeOpt(0D,20D,"20岁以下"));
        aggRangeOptList.add(new AggRangeOpt(20D,31D,"20岁到30岁"));
        aggRangeOptList.add(new AggRangeOpt(31D,100D,"30岁以上"));

        SaleInfo saleInfoWithAge= publisherService.getSaleInfo(date, keyword, startpage, size, "user_age", 100, null);

        //年龄段各个选项
        List<Option> ageOptionList=new ArrayList<>();
        HashMap ageAggsMap = saleInfoWithAge.getAggsMap();
        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;


        for (Object o : ageAggsMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr = (String) entry.getKey();
            Long count = (Long) entry.getValue();
            int age = Integer.parseInt(ageStr);
            if (age < 20) {
                age_20count += count;
            } else if (age >= 20 && age <= 30) {
                age20_30count += count;
            } else {
                age30_count += count;
            }
        }


        Double age_20ratio = Math.round( age_20count*1000D/total)/10D;
        Double age20_30ratio = Math.round( age20_30count*1000D/total)/10D;
        Double age30_ratio = Math.round( age30_count*1000D/total)/10D;
        ageOptionList.add(new Option("20岁以下", age_20ratio));
        ageOptionList.add(new Option("20岁到30岁", age20_30ratio));
        ageOptionList.add(new Option("30岁以上", age30_ratio));


        OptionGroup ageOptionGroup = new OptionGroup(ageOptionList, "年龄占比");
        optionGroupList.add(ageOptionGroup);

        SaleInfo saleInfo = new SaleInfo(total, optionGroupList, saleInfoWithGender.getDetail(), null);
        return  JSON.toJSONString(saleInfo);
    }
}
