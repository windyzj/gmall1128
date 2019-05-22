package com.atguigu.gmall1128.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall1128.canal.util.MyKafkaSender;
import com.atguigu.gmall1128.common.constant.GmallConstant;
import com.google.common.base.CaseFormat;

import java.util.List;

public class CanalHandler {

    public static void  handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowList){
        //判断业务类型
        if("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)&&rowList!=null&&rowList.size()>0){  //下单业务
            for (CanalEntry.RowData rowData : rowList) {  //遍历行集
                JSONObject jsonObject=new JSONObject();
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();  //得到列集
                for (CanalEntry.Column column : afterColumnsList) {  //遍历列集
                    System.out.println(column.getName()+":::::"+column.getValue());
                    String property = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(property,column.getValue());
                }

                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());


            }

        }


    }
}
