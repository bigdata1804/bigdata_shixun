package com.beicai.bean.domain

/**
  * Created by lenovo on 2019/5/6.
  */
class StatsEvent(
                  val date_dimension_id: Int,
                  val platform_dimension_id: Int,
                  val event_dimension_id: Int,
                  val times: Int,
                  val created: String
                ) {

}
