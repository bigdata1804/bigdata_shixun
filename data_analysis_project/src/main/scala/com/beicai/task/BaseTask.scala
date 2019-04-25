package com.beicai.task

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by lenovo on 2019/4/25.
  */
trait BaseTask {
  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
}
