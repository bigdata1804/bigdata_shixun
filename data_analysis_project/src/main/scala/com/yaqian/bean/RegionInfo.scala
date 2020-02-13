package com.yaqian.bean

import com.yaqian.constants.GlobalConstants

/**
  * Created by lenovo on 2019/4/25.
  */
case class RegionInfo(var country:String=GlobalConstants.DEFAULT_VALUE
                      ,var province:String=GlobalConstants.DEFAULT_VALUE
                      ,var city:String=GlobalConstants.DEFAULT_VALUE)
