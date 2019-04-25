package com.beicai.task.etl

import com.beicai.task.BaseTask

/**
  * Created by lenovo on 2019/4/25.
  *
  * 解析日志，将解析好的日志保存到hbase上
  *
  */
object AnalysisLogTask extends BaseTask{
  def main(args: Array[String]): Unit = {
    //todo 1,验证参数是否正确

    //todo 2,验证当天是否存在用户行为日志

    //todo 3,使用spark加载ip规则

    //todo 4,使用spark加载用户行为日志，进行解析

    //todo 5,将解析好的日志，保存到hbase上
  }
}
