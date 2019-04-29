package com.beicai.task.etl

import com.beicai.bean.IPRule
import com.beicai.common.AnalysisLog
import com.beicai.config.ConfigurationManager
import com.beicai.constants.{GlobalConstants, LogConstants}
import com.beicai.task.BaseTask
import com.beicai.util.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by lenovo on 2019/4/25.
  *
  * 解析日志，将解析好的日志保存到hbase上
  *
  */
object AnalysisLogTask extends BaseTask{
    var inputPath:String=null;

  //输入记录数的累加器
    val inputRecordAccumulator=sc.longAccumulator("inputRecordAccumulator")
  //过滤记录数累加器
    val filterRecordAccumulator = sc.longAccumulator("filterRecordAccumulator")

  /**
    * 2,验证当天是否存在用户行为日志
    * /logs/2019/04/24/xx.log
    * 2019-04-24===Long类型时间戳===>2019/04/24==>/logs/logs/2019/04/24==>验证这个路径在hdfs上是否存在
    */
  private def validateExistsLog():Unit={
    inputPath = ConfigurationManager.getValue(GlobalConstants.CONFIG_LOG_PATH_PREFIX) + Utils.formatDate(Utils.parseDate(inputDate, "yyyy-MM-dd"), "yyyy/MM/dd")
    var fileSystem:FileSystem=null;

    try{
      fileSystem=FileSystem.newInstance(configuration)
      if(!fileSystem.exists(new Path(inputPath))){
        throw new SparkException(
          s"""
             |Usage:com.beicai.task.etl.AnalysisLogTask
             |errorMessage:指定的日期${inputDate},不存在需要解析的用户行为日志
         """.stripMargin)
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(fileSystem!=null){
        fileSystem.close()
      }
    }

  }

  /**
    * 3 使用spark加载ip规则
    */

    private def loadIPRule()={
      val ipRuleArray: Array[IPRule] = sc.textFile(ConfigurationManager.getValue(GlobalConstants.CONFIG_IP_RULE_DATA_PATH), 2).map(line => {

        //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
        val fields = line.split("[|]")
        IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
      }).collect()
      ipRuleArray
    }

  /**
    * 4,使用spark加载用户行为日志，进行解析
    * @param ipRuleArray
    * @return
    */
  private def loadLogFromHdfs(ipRuleArray: Array[IPRule]) = {
    /**
      * 广播变量是共享变量，每个task都能共享，这样不至于每个task都需要去driver端拷贝这个副本
      * 可以降低网络流量消耗，降低executor内存消耗，加快spark作用运行效率，降低失败概率
      */
    val iPRulesBroadcast = sc.broadcast(ipRuleArray)

    val logRDD = sc.textFile(inputPath, 4).map(logText => {
      inputRecordAccumulator.add(1)
      AnalysisLog.analysisLog(logText, iPRulesBroadcast.value)
    }).filter(x => {
      if (x != null) {
        true
      } else {
        filterRecordAccumulator.add(1)
        false
      }
    })
    logRDD

  }


  /**
    * 5 将解析好的日志，保存到hbase上
    *
    * @param logRDD
    */

  private def saveLogToHbase(logRDD: RDD[mutable.Map[String, String]]): Unit = {
    val jobConf = new JobConf(configuration)
    //指定使用那个类将数据写入到hbase中
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定目标表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,LogConstants.HBASE_LOG_TABLE_NAME)

    logRDD.map(map =>{
      //构建rowKey 唯一，散列，不能过长，满足业务查询需要 accessTime+"_"+(uid+eventName).hascode
      val accessTime =  map(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME)
      val uid =map(LogConstants.LOG_COLUMNS_NAME_UID)
      val eventName=map(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
      val rowkey=accessTime+ "_" + Math.abs((uid+eventName).hashCode)
      val put = new Put(rowkey.getBytes())
      map.foreach(t2 =>{
        val key=t2._1
        val value=t2._2
        put.addColumn(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(),key.getBytes(),value.getBytes())

      })
      (new ImmutableBytesWritable(),put)
    }).saveAsHadoopDataset(jobConf)
  }

  def main(args: Array[String]): Unit = {
      //1,验证参数是否正确
      validateInputArgs(args)

      //2,验证当天是否存在用户行为日志
      validateExistsLog()

      // 3,使用spark加载ip规则
      val ipRuleArray = loadIPRule()

      //4,使用spark加载用户行为日志，进行解析
      val logRDD = loadLogFromHdfs(ipRuleArray)

      //5,将解析好的日志，保存到hbase上
      saveLogToHbase(logRDD)


      println(s"本次输入日志记录数：${inputRecordAccumulator.value}条,过滤日志记录数：${filterRecordAccumulator.value}条")
      sc.stop()
    }
}
