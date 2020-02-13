package com.yaqian.bean

/**
  * Created by lenovo on 2019/5/5.
  */
class StatsDeviceLocation(
                      val date_dimension_id:Int,
                      val platform_dimension_id: Int,
                      val location_dimension_id:Int,
                      val active_users: Int,
                      val sessions:Int,
                      val bounce_sessions:Int,
                      val created:String
                    ) {

}
