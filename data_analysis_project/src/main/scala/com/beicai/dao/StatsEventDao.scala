package com.beicai.dao

import com.beicai.bean.domain.StatsEvent
import com.beicai.jdbc.JdbcHelper

/**
  * Created by lenovo on 2019/5/6.
  */
object StatsEventDao {
  def deleteByDateDimensionId(dateDimensionId: Int) = {
    val sql = "delete from stats_event where date_dimension_id=?"
    val sqlParams = Array[Any](dateDimensionId)
    JdbcHelper.executeUpdate(sql, sqlParams)
  }

  def insertBatch(statsEventArray: Array[StatsEvent]) = {
    val sql = "insert into stats_event values(?,?,?,?,?)"
    val sqlParamsArray = new Array[Array[Any]](statsEventArray.length)
    for (i <- 0 until (statsEventArray.length)) {
      val statsEvent = statsEventArray(i)
      sqlParamsArray(i) = Array[Any](
        statsEvent.date_dimension_id,
        statsEvent.platform_dimension_id,
        statsEvent.event_dimension_id,
        statsEvent.times,
        statsEvent.created
      )
    }
    JdbcHelper.executeBatch(sql, sqlParamsArray)
  }
}
