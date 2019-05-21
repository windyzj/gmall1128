package com.atguigu.gmall1128.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1128.common.constant.GmallConstant
import com.atguigu.gmall1128.common.util.MyEsUtil
import com.atguigu.gmall1128.realtime.bean.StartUpLog
import com.atguigu.gmall1128.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {


  def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

      val ssc = new StreamingContext(sparkConf,Seconds(5))

       val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

//    inputDstream.foreachRDD { rdd =>
//      println(rdd.map(_.value()).collect().mkString("\n"))
//    }
    // 0  装换格式  把json变为case class   补充时间格式
    val startupLogDstream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val datetimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startUpLog.ts))
      val datetimeArr: Array[String] = datetimeStr.split(" ")

      startUpLog.logDate = datetimeArr(0)
      startUpLog.logHour = datetimeArr(1).split(":")(0)
      startUpLog.logHourMinute = datetimeArr(1)
      startUpLog
    }



    //2 根据今日访问日志 过滤每条启动日志   ，  凡是来过的用户 一律过滤掉
   //启动程序时只执行一次

//    startupLogDstream.filter{startupLog=>
//      val jedis: Jedis = RedisUtil.getJedisClient
//      val key="dau:"+startupLog.logDate
//      val flag: Boolean = jedis.sismember(key,startupLog.mid)
//      jedis.close()
//      flag
//    }

    val filteredStartuplogDstream: DStream[StartUpLog] = startupLogDstream.transform { rdd =>
      // driver中执行 ，每个时间间隔执行一次
      println("过滤前：" + rdd.count() + "条数")
      val jedis: Jedis = RedisUtil.getJedisClient
      val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + date
      val dauSet: util.Set[String] = jedis.smembers(key)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)

      val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog =>
        //executor中执行
        val dauSet: util.Set[String] = dauBC.value
        !dauSet.contains(startuplog.mid)
      }

      println("过滤后：" + filteredRdd.count() + "条数")
      filteredRdd

    }

    // filteredStartuplogDstream 过滤后再进行一次去重  以mid进行分组 每组取一个
    val  groupbyMidDtream: DStream[(String, Iterable[StartUpLog])] = filteredStartuplogDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val distinctStartUpDstream: DStream[StartUpLog] = groupbyMidDtream.flatMap { case (mid, startupItr) =>
      startupItr.take(1)
    }

    //1 记录今日访问过的用户
    //保存redis key类型 ：set    key:  dau:2019-xx-xx  values : mids

    distinctStartUpDstream.foreachRDD{ rdd=>
      // driver
      //
      rdd.foreachPartition{ startuplogItr=>
        // executor
        val jedis: Jedis = RedisUtil.getJedisClient

        val startUpList: List[StartUpLog] = startuplogItr.toList
        for (startuplog <- startUpList ) {
          val key="dau:"+startuplog.logDate
          jedis.sadd(key,startuplog.mid)

        }
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_DAU,startUpList)

        jedis.close()


      }

    }



    ssc.start()
    ssc.awaitTermination()

  }

}
