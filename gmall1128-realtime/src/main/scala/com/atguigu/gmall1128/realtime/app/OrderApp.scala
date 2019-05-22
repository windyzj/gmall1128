package com.atguigu.gmall1128.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1128.common.constant.GmallConstant
import com.atguigu.gmall1128.common.util.MyEsUtil
import com.atguigu.gmall1128.realtime.bean.OrderInfo
import com.atguigu.gmall1128.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {


  def main(args: Array[String]): Unit = {
     val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")

     val ssc = new StreamingContext(sparkConf,Seconds(5))

     val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //  补充日期
      val orderTimeArr: Array[String] = orderInfo.createTime.split(" ")
      orderInfo.createDate = orderTimeArr(0)
      val timeArr: Array[String] = orderTimeArr(1).split(":")
      orderInfo.createHour = timeArr(0)
      orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)

      //  13810100101=> 1381***0101
      val tel1: (String, String) = orderInfo.consigneeTel.splitAt(4)
      val tel2: (String, String) = orderInfo.consigneeTel.splitAt(7)
      orderInfo.consigneeTel = tel1._1 + "***" + tel2._2

      orderInfo
    }
    //orderinfo增加一个字段 来区别是否是用户首次下单
    // 自己实现




    //保存到ES
    orderInfoDstream.foreachRDD{rdd=>

      rdd.foreachPartition{ orderInfoItr=>

        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_ORDER,orderInfoItr.toList)
      }

    }

    ssc.start()
    ssc.awaitTermination()







  }

}
