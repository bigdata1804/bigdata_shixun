package com.yaqian.bean.domain

/**
  * Created by 黑桃K on 2019/4/28.
  * 刀客程序员淘宝旗舰店：https://daoke360.taobao.com/
  * 刀客程序员官网：http://www.daoke360.com
  * QQ:272488352
  */
class SessionAggrStat {
  var date_dimension_id: Int = 0
  var platform_dimension_id: Int = 0
  var session_count: Int = 0
  var time_1s_3s                    : Double = 0.0
  var time_4s_6s                    : Double = 0.0
  var time_7s_9s                    : Double = 0.0
  var time_10s_30s                    : Double = 0.0
  var time_30s_60s                    : Double = 0.0
  var time_1m_3m              : Double = 0.0
  var time_3m_10m           : Double = 0.0
  var time_10m_30m                                : Double = 0.0
  var time_30m                        : Double = 0.0
  var step_1_3                        : Double = 0.0
  var step_4_6                        : Double = 0.0
  var step_7_9                        : Double = 0.0
  var step_10_30                    : Double = 0.0
  var step_30_60                    : Double = 0.0
  var step_60                         : Double = 0.0
}
