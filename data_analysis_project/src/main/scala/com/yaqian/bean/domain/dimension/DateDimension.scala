package com.yaqian.bean.domain.dimension

import com.yaqian.enum.DateTypeEnum
import com.yaqian.util.Utils


/**
  * Created by 黑桃K on 2019/4/29.
  * 刀客程序员淘宝旗舰店：https://daoke360.taobao.com/
  * 刀客程序员官网：http://www.daoke360.com
  * QQ:272488352
  */
class DateDimension(var id: Int, var year: Int, var season: Int, var month: Int, var week: Int, var day: Int,
                    var calendar: String, var dateType: String) {

}

object DateDimension {

  /**
    * 构建日期维度
    *
    * @param inputDate yyyy-MM-dd
    *                  2019-04-25
    */
  def buildDateDimension(inputDate: String) = {
    val year: Int = Utils.getDateInfo(inputDate, DateTypeEnum.YEAR)
    val season: Int = Utils.getDateInfo(inputDate, DateTypeEnum.SEASON)
    val month: Int = Utils.getDateInfo(inputDate, DateTypeEnum.MONTH)
    val week: Int = Utils.getDateInfo(inputDate, DateTypeEnum.WEEK)
    val day: Int = Utils.getDateInfo(inputDate, DateTypeEnum.DAY)
    new DateDimension(0, year, season, month, week, day, inputDate, DateTypeEnum.DAY.toString)
  }
}
