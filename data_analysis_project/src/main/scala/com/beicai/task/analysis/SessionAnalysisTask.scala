package com.beicai.task.analysis

import com.beicai.constants.{GlobalConstants, LogConstants}
import com.beicai.enum.EventEnum
import com.beicai.task.BaseTask
import com.beicai.util.Utils
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Iterable

/**
  * Created by lenovo on 2019/4/28.
  */
object SessionAnalysisTask extends BaseTask{


  /**
    * 从hbase中加载指定日期当前的所有日志
    */
  def loadDataFromHbase() = {
    val startDateTime = Utils.parseDate(inputDate,"yyyy-MM-dd").toString
    val endDateTime = Utils.getNextDate(startDateTime.toLong).toString
    val scan: Scan = new Scan()
    //设置开始扫描位置
    scan.setStartRow(startDateTime.getBytes())
    //设置结束的扫描位置
    scan.setStopRow(endDateTime.getBytes())

    //scan==>string的scan （base64）
    val protoScan = ProtobufUtil.toScan(scan)
    //使用base64算法对protoscan进行编码，编码成字符串
    val base64StringScan = Base64.encodeBytes(protoScan.toByteArray)

    val jobConf = new JobConf(configuration)
    //设置需要加载的表
    jobConf.set(TableInputFormat.INPUT_TABLE,LogConstants.HBASE_LOG_TABLE_NAME)
    //设置扫描器
    jobConf.set(TableInputFormat.SCAN,base64StringScan)

    val resultRDD: RDD[Result] = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
    val eventLogRDD = resultRDD.map(result => {
      val uid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_UID.getBytes()))
      val sid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_SID.getBytes()))
      val accessTime = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes()))
      val eventName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_EVENT_NAME.getBytes()))
      val country = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_COUNTRY.getBytes()))
      val province = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PROVINCE.getBytes()))
      val city = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_CITY.getBytes()))
      val platform = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PLATFORM.getBytes()))
      val browserName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_BROWSER_NAME.getBytes()))
      val productId = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PRODUCT_ID.getBytes()))
      val osName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes()))
      (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    })
    eventLogRDD
  }
  /**
    * 按时间和平台维度
    * 对我们的session进行统计分析，将结果保持到mysql表中
    *
    * @param eventLogRDD
    * (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    */

  def sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    //取出需要的字段
    //tuple4RDD==>( sid, accessTime, eventName,platform)
    val tuple4RDD: RDD[(String, String, String, String)] = eventLogRDD.map(t11=>(t11._2,t11._3,t11._4,t11._8))
    //以时间accessTime和platform平台作为key，以sid, accessTime, eventName,platform作为value
    //((accessTime,platform),( sid, accessTime, eventName,platform))
    /**
      * ((2019-04-25,pc),(7E900297-0686-4BA1-8048-90243279B696,1556199190643,e_pv,pc))
      * ((2019-04-25,ios),(E2ABF97F-4B3E-49AA-9ABF-6C42EA232C63,1556199191517,e_pv,ios))
      * ((2019-04-25,ios),(129BA911-98C1-4EC5-AED2-459371E610D2,1556199191675,e_pv,ios))
      * ((2019-04-25,pc),(BED1F858-B563-4F79-B331-24782849C6DB,1556199191874,e_bp,pc))
      * ((2019-04-25,ios),(EC1FFF28-7BA6-45CB-94C5-30505899DB37,1556199192689,e_l,ios))
      */
      val tuple2RDD = tuple4RDD.map(t4=>((Utils.formatDate(t4._2.toLong,"yyyy-MM-dd"),t4._4),t4))
    //根据平台维度，一条日志需要变成2条日志
    val flatMapRDD: RDD[((String, String), (String, String, String, String))] = tuple2RDD.flatMap(t2 => {
      Array(
        //所有的平台
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL), t2._2),
        //具体的平台
        t2
      )
    })

    //将同一天同一个平台的数据聚合在一起
    /**
      * ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
      */
    val groupRDD = flatMapRDD.groupByKey()
    /**
      * 计算出同一天同一个平台中，每个session的访问时长和访问步长
      * ((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      */
    val sessionTimeAndStepLengthRDD: RDD[((String, String), Iterable[(String, Long, Long)])] = groupRDD.map(t2 => {
      val it: Iterable[(String, Long, Long)] = t2._2.groupBy(_._1).map(g => {
        //g==>(sid,List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform)))
        var visitStepLength, visitTimeLength: Long = 0L
        var startTime, endTime: Long = 0L
        g._2.foreach(t4 => {
          val eventName = t4._3
          if (eventName.equals(EventEnum.PAGE_VIEW_EVENT.toString) || eventName.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) {
            visitStepLength += 1
          }
          val accessTime = t4._2.toLong
          if (startTime == 0 || accessTime < startTime) startTime = accessTime
          if (endTime == 0 || accessTime > endTime) endTime = accessTime
        })
        visitTimeLength = (endTime - startTime) / 1000
        (g._1, visitTimeLength, visitStepLength)
      })
      //t2==> ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
      //按会话id进行分组(sid,List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform)))
      //((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      (t2._1, it)
    })

    /**
      * 判断同一天同一个平台，每个session访问时长和步长所属区间，对应区间+1
      *
      * "session_count=0|1s_3s=1|4s_6s=0|....."
      */
    val sessionTimeAndStepLengthRangeRDD: RDD[((String, String), String)] = sessionTimeAndStepLengthRDD.map(t2 => {
      //t2==>((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      //session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
      val stringAccumulator = new StringAccumulator()
      t2._2.foreach(t3 => {
        stringAccumulator.add(GlobalConstants.SESSION_COUNT)
        val visitTimeLength = t3._2
        val visitStepLength = t3._3
        if (visitTimeLength >= 0 && visitTimeLength <= 3) {
          stringAccumulator.add(GlobalConstants.TIME_1s_3s)
        } else if (visitTimeLength > 4 && visitTimeLength <= 6) {
          stringAccumulator.add(GlobalConstants.TIME_4s_6s)
        } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
          stringAccumulator.add(GlobalConstants.TIME_7s_9s)
        } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
          stringAccumulator.add(GlobalConstants.TIME_10s_30s)
        } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
          stringAccumulator.add(GlobalConstants.TIME_30s_60s)
        } else if (visitTimeLength > 1 * 60 && visitTimeLength <= 3 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_1m_3m)
        } else if (visitTimeLength > 3 * 60 && visitTimeLength <= 10 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_3m_10m)
        } else if (visitTimeLength > 10 * 60 && visitTimeLength <= 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_10m_30m)
        } else if (visitTimeLength > 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_30m)
        }

        if (visitStepLength >= 1 && visitStepLength <= 3) {
          stringAccumulator.add(GlobalConstants.STEP_1_3)
        } else if (visitStepLength >= 4 && visitStepLength <= 6) {
          stringAccumulator.add(GlobalConstants.STEP_4_6)
        } else if (visitStepLength >= 7 && visitStepLength <= 9) {
          stringAccumulator.add(GlobalConstants.STEP_7_9)
        } else if (visitStepLength >= 10 && visitStepLength <= 30) {
          stringAccumulator.add(GlobalConstants.STEP_10_30)
        } else if (visitStepLength > 30 && visitStepLength <= 60) {
          stringAccumulator.add(GlobalConstants.STEP_30_60)
        } else if (visitStepLength > 60) {
          stringAccumulator.add(GlobalConstants.STEP_60)
        }
      })
      (t2._1, stringAccumulator.value)
    })
    /**
      * ((2019-04-25,ios),session_count=646|1s_3s=555|4s_6s=1|7s_9s=0|10s_30s=8|30s_60s=11|1m_3m=25|3m_10m=46|10m_30m=0|30m=0|1_3=646|4_6=0|7_9=0|10_30=0|30_60=0|60=0)
      * ((2019-04-25,pc),session_count=1025|1s_3s=577|4s_6s=1|7s_9s=3|10s_30s=7|30s_60s=23|1m_3m=83|3m_10m=331|10m_30m=0|30m=0|1_3=890|4_6=72|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,all),session_count=1713|1s_3s=1134|4s_6s=2|7s_9s=5|10s_30s=15|30s_60s=37|1m_3m=116|3m_10m=404|10m_30m=0|30m=0|1_3=1576|4_6=74|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,android),session_count=42|1s_3s=2|4s_6s=0|7s_9s=2|10s_30s=0|30s_60s=3|1m_3m=8|3m_10m=27|10m_30m=0|30m=0|1_3=40|4_6=2|7_9=0|10_30=0|30_60=0|60=0)
      *
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      *
      * */

  }
  



  def main(args: Array[String]): Unit = {
    //1,验证参数是否正确
    validateInputArgs(args)

    //2,从hbase中加载指定日期的日志
    val eventLogRDD = loadDataFromHbase()

    //3,按时间和平台维度对我们的session进行统计分析，将结果保持到mysql表中
    sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD)

  }
}
