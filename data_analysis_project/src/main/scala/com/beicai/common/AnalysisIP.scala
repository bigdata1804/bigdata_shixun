package com.beicai.common

import com.beicai.bean.{IPRule, RegionInfo}
import com.beicai.util.Utils

import scala.util.control.Breaks._
/**
  * Created by lenovo on 2019/4/25.
  */
object AnalysisIP {

  /**
    * 通过ip，获取ip对应的地域信息，将地域信息封装到RegionInfo对象中
    *
    * @param ip
    * @param ipRuleArray
    */
  def getRegionInfoByIP(ip:String,ipRuleArray:Array[IPRule])={
    val regionInfo = RegionInfo()
    //1,将ip转换成数字
    val numIp = Utils.ipToLong(ip)
    //2，通过二分查找法，查找ip对应的地域信息
    val index: Int = binarySearch(numIp,ipRuleArray)
    if(index != -1){
      val iPRule = ipRuleArray(index)
      regionInfo.country=iPRule.country
      regionInfo.province=iPRule.province
      regionInfo.city=iPRule.city
    }
    regionInfo
  }


  /**
    * 二分查找法，找到了返回对应的角标，找不到返回-1
    *
    * @param numIp
    * @param ipRuleArray
    */

  def binarySearch(numIp:Long,ipRuleArray: Array[IPRule])={
    var index = -1
    var min=0
    var max=ipRuleArray.length-1
    breakable(while (min<=max){
      var middle=(min+max)/2
      val iPRule = ipRuleArray(middle)
      if(numIp>=iPRule.startIP&&numIp<=iPRule.endIP){
        index=middle
        break()
      }else if(numIp<iPRule.startIP){
        max=middle-1
      }else if(numIp>iPRule.endIP){
        min=middle+1
      }
    })
    index
  }

}
