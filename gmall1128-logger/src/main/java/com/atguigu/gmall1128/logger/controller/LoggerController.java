package com.atguigu.gmall1128.logger.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController  //=== controller +responsebody
public class LoggerController {




    @PostMapping("/log")
    public  String doLog(@RequestParam("log") String log){
        //补时间戳
        // 落盘log   log4j   log4j.properties
        // 启动日志和事件日志进分流     分到不同的kafka topic中

        System.out.println(log);
        return "success";



    }

}
