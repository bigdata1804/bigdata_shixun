package com.yaqian.task.review

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lenovo on 2019/5/8.
  *
  * kafka对外提供的消费者api有两种：
  * 1，低阶消费者API：最底层的api，没有经过高度封装，包括kafka自己都会使用，使用难度较大，特别灵活，自主性较强(手动挡)
  * 2，高阶消费者API：经过高度封装的api，使用起来简单方便，只需要传递少量参数即可，自主性不强（自动挡）
  *
  * spark-streaming对这两种api都有实现的方式：
  * 1，Direct直联kafka模式：
  * a,基于kafka低阶消费者API,不依赖于zk，消费偏移量程序员可以自己管理
  * b,没有receiver组件，消费数据类似于spark 读取hdfs上的文件（将每个topic分区看成是一个文件）
  * c,所形成的rdd分区数与topic的分区数，一一对应的关系
  *
  * 2，Receiver模式：
  * a,基于kafka的高阶消费者API，消费偏移量无法掌控，由kafka高级api来维护到zk上
  * b,有receiver组件，存在receiver独占cpu core的问题
  * c,所形成的rdd分区数与topic的分区数，不是一一对应的关系
  */
object SparkStreamingKafkaDemo {

  /**
    * Receiver模式
    */

  def receiverDemo(): Unit ={
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(5))
    //1，创建初始的dstream
    //    val inputDstream = KafkaUtils.createStream(ssc,"hadoop-001:2181,hadoop-002:2181","g1804",Map("wordcount"->3))

    val kafkaParams=Map[String,String](
      "zookeeper.connect" ->"hadoop-001:2181,hadoop-002:2181,hadoop-003:2181",
    "group.id" -> "g1804",
    /**
      * 当在zk上找不到消费偏移量时，这个配置项就会起作用，代表的是起始消费位置
      * 1,smallest :最早消息的偏移量
      * 2,largest：最新消息的偏移量(默认值)
      */
    "auto.offset.reset" -> "largest"

    //每隔多长时间更新一次消费偏移量(默认是60s)
//    "auto.commit.interval.ms" -> "1000"
    )

    val inputDStream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Map("wordcount"->3),StorageLevel.MEMORY_ONLY)

    //2，对初始的dstream进行不断的转换操作
    //需要注意的是，我们消费kafak里面的数据，取出来的数据是（key,value），value才是我们的消息

    val wordCountDstream = inputDStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //3，进行输出
    wordCountDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * Direct模式（直联）
    */

  def directDemo(): Unit ={
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams=Map[String,String](
      //kafka broker地址
      "metadata.broker.list" -> "hadoop-001:9092,hadoop-002:9092,hadoop-003:9092",
      "group.id" -> "G1804",
      "auto.offset.reset" -> "largest"
    )
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("wordcount")).map(_._2)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * Direct模式（直联）
    */
  def directDemo2(): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val checkpointPath="/checkpoint/G1804"

    /**
      * checkpointPath保存了一下基本信息：
      * 1，streaming 应用的配置信息 sparkConf
      * 2,保存了dstream的操作转换逻辑关系
      * 3,保存了未处理完的batch元数据信息
      * 4,保存了消费偏移量
      */

    val ssc = StreamingContext.getOrCreate(checkpointPath,()=>createStreamingContext(sc,checkpointPath))

    ssc.start()
    ssc.awaitTermination()


  }
  def createStreamingContext(sc:SparkContext,checkpointPath:String): StreamingContext ={
    val ssc = new StreamingContext(sc,Seconds(3))

    ssc.checkpoint(checkpointPath)

    val kafkaParams=Map[String,String](
      //kafka broker地址
      "metadata.broker.list" -> "hadoop-001:9092,hadoop-002:9092,hadoop-003:9092",
      "group.id" -> "G1804",
      "auto.offset.reset" -> "largest"
    )

    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("wordcount")).map(_._2)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()   //.groupByKey().mapValues(x=>x.toList.sum).print()

    ssc
  }


  def main(args: Array[String]): Unit = {

    //    receiverDemo()

//    directDemo()

    directDemo2()

  }


}
