package com.yaqian.dao

import com.yaqian.bean.StatsDeviceLocation
import com.yaqian.jdbc.JdbcHelper

/**
  * Created by lenovo on 2019/5/5.
  */
object StatsDeviceLocationDao {
  def deleteByDateDimensionId(dateDimensionId:Int)={
    val sql="delete from stats_device_location where date_dimension_id=?"
    val sqlParams=Array[Any](dateDimensionId)
    JdbcHelper.executeUpdate(sql,sqlParams)
  }
  def insertBatch(statisticsUserArray:Array[StatsDeviceLocation])={
    val sql="insert into stats_device_location values(?,?,?,?,?,?,?)"
    val sqlParamsArray=new Array[Array[Any]](statisticsUserArray.length)
    for(i <- 0 until(statisticsUserArray.length)){
     val statisticsUser=statisticsUserArray(i)
      sqlParamsArray(i)=Array[Any](
        statisticsUser.date_dimension_id,
        statisticsUser.platform_dimension_id,
        statisticsUser.location_dimension_id,
        statisticsUser.active_users,
        statisticsUser.sessions,
        statisticsUser.bounce_sessions,
        statisticsUser.created
      )
    }
    JdbcHelper.executeBatch(sql,sqlParamsArray)
  }
}
