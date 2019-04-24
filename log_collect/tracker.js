(function () {
    /**
     * 负责读取浏览器cookie数据
     */
    var cookieUtils={
        /**
         *向浏览器写入cookie数据
         */
        set:function (cookieName,cookieValue) {
          var cookieText = encodeURIComponent(cookieName)+"="+encodeURIComponent(cookieValue)
            //计算十年后的时间
            var date = new Date();
            //获取当前时间戳
            var currentTime = date.getTime();
            //获取十年的时间长度
            var tenYearLongTime=10*365*24*60*60*1000;
            //十年后的时间
            var tenYearAfter=currentTime+tenYearLongTime;
            //将date对象调整为10年后的时间
            date.setTime(tenYearAfter);
            cookieText += ";expires="+date.toUTCString();
            document.cookie=cookieText;
        },
        /**
         * 向浏览器获取cookie数据
         * document.cookie
         * uuid=123;sid=456;mid=789
         */
        get : function (cookieName) {
        var cookieValue=null;
        var cookieText = document.cookie;
        if(cookieText.indexOf(encodeURIComponent(cookieName))>1){
        var items = cookieText.split(";");
        for(index in items){
            var kv = items[index].split("=");
            var key = kv[0].trim();
            var value=kv[1].trim();
            if(key==encodeURIComponent(cookieName)){
                cookieValue=decodeURIComponent(value)
                 }
             }
         }
            return cookieValue;
        }
    };

         /**
        * 负责具体采集每个用户行为事件数据
        */
        var tracker={
            clientConfig:{
                logServerUrl: "http://hadoop-001/log.gif",//日志服务器地址
                sessionTimeOut: 2 * 60 * 1000,//会话过期时间
                logVersion: "2.0"
            },
             /**
              * 需要存放到浏览器cookie中的字段
              */
             cookieKeys:{
                 uuid: "uid",//用户唯一标识
                 sid: "sid",//会话id
                 preVisitTime: "pre_visit_time"//用户最近一次访问时间
             },
             /**
              * 定义需要采集的事件名称（这个一定是根据具体业务来）
              */
             events: {
                 launchEvent: "e_l",//用户首次访问事件
                 pageViewEvent: "e_pv",//用户浏览页面事件
                 addCartEvent: "e_ad",//商品加入购物车事件
                 searchEvent: "e_s"//搜索事件
             },
             /**
              * 定义需要采集的字段（这个一定是根据具体业务来）
              */
             columns: {
                 eventName: "en",//事件名称
                 version: "ver",//日志版本
                 platform: "pl",//平台 ios Android
                 sdk: "sdk",//sdk js java
                 uuid: "uid",//用户唯一标识
                 sessionId: "sid",//会话id
                 resolution: "b_rst",//浏览器分辨率
                 userAgent: "b_usa",//浏览器代理信息
                 language: "l",//语言
                 clientTime: "ct",//客户端时间
                 currentUrl: "url",//当前页面的url
                 referrerUrl: "ref",//来源url，上一个页面的url
                 title: "tt",//网页标题
                 keyword: "kw",//搜索关键字
                 goodsId: "gid"//商品id
             },
             /**
              *设置uuid到cookie
              */
             setUuid:function (uuid) {
                 cookieUtils.set(this.cookieKeys.uuid,uuid)
             },
             /**
              * 获取uuid
              */
             getUuid:function () {
                 return cookieUtils.get(this.cookieKeys.uuid)
             },
             /**
              * 设置会话id
              */
             setSid:function (sid) {
                 cookieUtils.set(this.cookieKeys.sid,sid)
             },
             /**
              * 获取会话id
              */
             getSid:function () {
                 return cookieUtils.get(this.cookieKeys.sid)
             },
             /**
              * todo 会话开始了
              */
             sessionStart:function () {
                 /**
                  * 判断会话是否存在，就是尝试从浏览器的cookie中获取用户的会话id，如果获取到了说明会话存在，否则不存在
                  * "" null ==>false
                  * */
                 if(!this.getSid()){//会话不存在
                     //创建新的会话
                     this.createNewSession()
                 }else{//会话存在
                     //判断会话是否过期
                    if(this.isSessionTimeOut()){
                        //会话过期了，创建新的会话
                        this.createNewSession();
                    }else{
                        //更新用户最近一次访问时间
                        this.updatePreVisitTime();
                    }
                 }
                 //触发pageView事件（用户浏览页面事件）
                 this.pageViewEvent();
             },
             /**
              * 创建新的会话
              */
             createNewSession:function () {
                 //生成会话id
                 var sid=this.guid;
                 //将会话id保存到cookie中
                 this.setSid(sid)

                 //判断用户是否是首次访问（查看浏览器的cookie中是否用用户的唯一标识，如果有说明是非首次访问，否则是首次访问）
                 if(!this.getUuid()){//获取不到用户的唯一标识
                     //生成用户唯一标识
                     var uuid=this.guid();
                     //将用户唯一标识保持到cookie中
                     this.setUuid(uuid)
                     //todo 触发首次访问事件，收集用户的数据发送首次访问事件日志到后台服务器上
                 }
             },
             /**
              * 定义首次访问事件
              */
             launchEvent:function () {
                 //定义一个对象data， 这个对象用来封装收集好的数据
                 var data={};
                 //事件名称 data["en"]="e_l"
                 data[this.columns.eventName] = this.events.launchEvent;
                 //设置公共字段
                 this.setCommonColumns(data);
                 //将收集好的数据发送到日志服务器上
                 this.sendDataToLogServer(data);
             },

             /**
              * 用户浏览页面事件
              */
             pageViewEvent:function () {
                 var data={};
                 //事件名称
                 data[this.columns.eventName]=this.events.pageViewEvent;
                 //设置公共字段
                 this.setCommonColumns(data);
                 //当前页面的url
                 data[this.columns.currentUrl]=window.location.href;
                 //当前页面的标题
                 data[this.columns.title]=document.title;
                 //来源页面的url
                 data[this.columns.referrerUrl]=document.referrer;
                 //发送数据到日志服务器上
                 this.sendDataToLogServer(data);
             },


             /**
              * 搜索事件
              */
             searchEvent:function (keyword) {
                 var data={};
                 //事件名称
                 data[this.columns.eventName]=this.events.searchEvent;
                 //设置公共字段
                 this.setCommonColumns(data);
                 //搜索关键词
                 data[this.columns.keyword]=keyword;
                 //发送数据到日志服务器上
                 this.sendDataToLogServer(data);
             },
             /**
              * 商品加入购物车事件
              */
             addCartEvent:function (goodsId) {
                 var data={};
                 //事件名称
                 data[this.columns.eventName]=this.events.addCartEvent;
                 //设置公共字段
                 this.setCommonColumns(data);
               //加入购物车的商品id
                 data[this.columns.goodsId]=goodsId;
                 //发送数据到日志服务器上
                 this.sendDataToLogServer(data);
             },

             /**
              * 判断会话是否过期
              */
             isSessionTimeOut:function () {
                 //当前时间
                 var currentTime=new Date().getTime();
                 //获取用户最近一次访问时间
                 var preVisitTime = cookieUtils.get(this.cookieKeys.preVisitTime);
                 return currentTime-preVisitTime > this.clientConfig.sessionTimeOut;
             },


             /**
              * 将数据发送到日志服务器上
              */
             sendDataToLogServer:function (data) {
                 //data==> key value==> key=value&key=value&....
                 var paramsText="";
                 for(key in data){
                     //有值的情况
                     if(key && data[key])
                         paramsText += encodeURIComponent(key)+"="+encodeURIComponent(data[key])+"&"
                     }
                     if(paramsText)
                         paramsText=paramsText.substring(0,paramsText.length-1);
                         var url=this.clientConfig.logServerUrl+"?"+paramsText;
                         var i=new Image(1,1);
                         i.src=url
                         //更新用户最近一次访问时间
                        this.updatePreVisitTime()
             },


             /**
              * 更新用户最近一次访问时间
              */

             updatePreVisitTime:function () {
                 cookieUtils.set(this.cookieKeys.preVisitTime,new Date().getTime())
             },

             /**
              * 设置公共字段
              */
             setCommonColumns:function (data) {
                 //sdk 版本号
                 data[this.columns.version] = this.clientConfig.logVersion;
                 //用户代理信息
                 var userAgent=window.navigator.userAgent.toLowerCase();
                 data[this.columns.userAgent]=userAgent;
                 /**
                  * indexOf 用来查找子字符串是否在指定字符串之内，如果存在，返回对应字符串起始角标，否则返回 -1
                  */
                 if(userAgent.indexOf("android")>1){
                     data[this.columns.platform]="android"
                 }else if(userAgent.indexOf("iphone")>1){
                     data[this.columns.platform]="ios";
                 }else{
                     data[this.columns.platform]="pc";
                 }
                 data[this.columns.sdk]="js";
                 //用户唯一标识
                 data[this.columns.uuid]=this.getUuid();
                 //会话id
                 data[this.columns.sessionId]=this.getSid();
                 //浏览器分辨率
                 data[this.columns.resolution]=window.screen.width+"*"+window.screen.height;

                 //客户端语言
                data[this.columns.language]=window.navigator.language;
                 //客户端当前时间
                 data[this.columns.clientTime]=new Date().getTime();
             },
             /**
              * 生成唯一标识guid
              */
             guid: function () {
                 return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                     var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                     return v.toString(16);
                 });
             }

         };
        tracker.sessionStart();
})();