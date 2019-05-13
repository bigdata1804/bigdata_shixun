package com.beicai.bean.domain.dimension

import com.beicai.constants.GlobalConstants
import com.beicai.enum.EventEnum

/**
  * Created by lenovo on 2019/5/6.
  */
class EventDimension(
                      var id: Int,
                      var event_name: String,
                      var event_description: String, //比如：全站新用户访问，web浏览页面,...
                      var category: String //比如 ： 全站购买行为，全站产品体验行为,...
                    ) {

}

object EventDimension {

  def buildEventDimension(event_name: String) = {
    var event_description: String = null //比如：全站新用户访问，web浏览页面,...
    var category: String = null //比如 ： 全站购买行为，全站产品体验行为,...
    if (event_name.equals(EventEnum.LAUNCH_EVENT.toString) || event_name.equals(EventEnum.PAGE_VIEW_EVENT.toString)) {
      category = "全站产品体验行为"
      if (event_name.equals(EventEnum.LAUNCH_EVENT.toString)) {
        event_description = "全站新用户访问"
      }
      if (event_name.equals(EventEnum.PAGE_VIEW_EVENT.toString)){
        event_description = "web浏览页面"
      }
    }
    if (event_name.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString) || event_name.equals(EventEnum.ADD_CART_EVENT.toString)) {
      category = "全站购买行为"
      if (event_name.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)){
        event_description = "浏览商品详情页"
      }
      if (event_name.equals(EventEnum.ADD_CART_EVENT.toString)) {
        event_description = "加入购物车"
      }
    }
    if (event_name.equals(GlobalConstants.VALUE_OF_ALL)) {
      event_description = "所有事件"
      category = "所有行为"
    }
    new EventDimension(0, event_name, event_description, category)
  }
}
