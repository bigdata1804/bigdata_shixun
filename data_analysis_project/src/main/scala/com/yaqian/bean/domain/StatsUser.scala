package com.yaqian.bean.domain

/**
  * Created by 黑桃K on 2019/5/5.
  * 刀客程序员淘宝旗舰店：https://daoke360.taobao.com/
  * 刀客程序员官网：http://www.daoke360.com
  * QQ:272488352
  */
class StatsUser(
                 val date_dimension_id: Int,
                 val platform_dimension_id: Int,
                 val active_users: Int,
                 val new_install_users: Int,
                 val session_count: Int,
                 val session_length: Int,
                 val created: String
               ) {

}
