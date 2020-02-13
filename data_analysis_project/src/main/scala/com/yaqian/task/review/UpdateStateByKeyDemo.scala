package com.yaqian.task.review

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by lenovo on 2019/5/9.
  */
object UpdateStateByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc,Seconds(3))
    //这个目录用于保存updateStateByKey聚合后的状态
    ssc.checkpoint("/checkpoint/a1804")
    //1，根据输入流（kafka,tcp,flume,hdfs,...），创建初始的dstream
    val lineDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop-001", 8989, StorageLevel.MEMORY_ONLY)

    val tuple2RDD = lineDstream.flatMap(_.split(" ")).map((_, 1))
    tuple2RDD.updateStateByKey(

      /**
        *(String, Seq[Int], Option[Int])
        * String:代表的是key的类型
        * Seq[Int]：当前key在本批次rdd中value的集合
        * Option[Int]：当前这个key在以前聚合后的结果(注意：有可能有Some，也有可能没有None)
        */

      (it:Iterator[(String,Seq[Int],Option[Int])])=>{
        it.map(t3=>(t3._1,t3._2.sum+t3._3.getOrElse(0)))
      },

      //分区器
      new HashPartitioner(sc.defaultMinPartitions),

      //是否记住分区
      true
    )

  }

}
