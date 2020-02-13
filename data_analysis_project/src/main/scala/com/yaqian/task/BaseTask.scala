package com.yaqian.task

import com.yaqian.util.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.SparkSession

/**
  * Created by lenovo on 2019/4/25.
  */
trait BaseTask {

   val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")

  //输入任务参数
  var inputDate:String=null

  /**
    * 验证参数是否正确
    * 1,验证参数的个数  >=1
    * 2,验证参数的各是 yyyy-MM-dd
    *
    * @param args
    */
   def validateInputArgs(args: Array[String]): Unit = {
    if(args.length==0){
      throw new SparkException(
        s"""
          |Usage:${this.getClass.getSimpleName}
          |errorMessage:任务至少需要有一个日期参数
        """.stripMargin)
    }
    if(!Utils.validateInputDate(args(0))){
      throw new SparkException(
        """
          |Usage:${this.getClass.getSimpleName}
          |errorMessage:任务第一个参数是一个日期，日期的格式是：yyyy-MM-dd
        """.stripMargin)
    }
    inputDate=args(0)
  }


}
