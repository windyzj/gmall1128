package com.atguigu.gmall1128.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1128.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController  //=== controller +responsebody
public class LoggerController {


    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("/log")
    public  String doLog(@RequestParam("log") String log){
        //1 补时间戳
        JSONObject logJSON = JSON.parseObject(log);
        logJSON.put("ts",System.currentTimeMillis());

        //2  落盘log   log4j   log4j.properties
        String jsonString = logJSON.toJSONString();
        logger.info(jsonString);

        //3  启动日志和事件日志进分流     分到不同的kafka topic中
        if( "startup".equals(logJSON.getString("type")) ){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonString);
        }

        return "success";



    }

}
