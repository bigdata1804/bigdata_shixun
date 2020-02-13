package com.yaqian.dao

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}

import com.yaqian.bean.domain.dimension.{DateDimension, EventDimension, LocationDimension, PlatformDimension}


/**
  * Created by lenovo on 2019/4/29.
  */
object DimensionDao {


  /**
    * 给sql语句赋值
    *
    * @param preparedStatement
    * @param dimension
    */
  def setSqlArgs(preparedStatement: PreparedStatement, dimension: Any) = {
    if(dimension.isInstanceOf[DateDimension]){
      val dateDimension = dimension.asInstanceOf[DateDimension]
      preparedStatement.setObject(1,dateDimension.year)
      preparedStatement.setObject(2, dateDimension.season)
      preparedStatement.setObject(3, dateDimension.month)
      preparedStatement.setObject(4, dateDimension.week)
      preparedStatement.setObject(5, dateDimension.day)
      preparedStatement.setObject(6, dateDimension.calendar)
      preparedStatement.setObject(7, dateDimension.dateType)
    }else if(dimension.isInstanceOf[PlatformDimension]){
      val platformDimension = dimension.asInstanceOf[PlatformDimension]
      preparedStatement.setObject(1,platformDimension.platformName)
    }else if(dimension.isInstanceOf[LocationDimension]){
      val locationDimension = dimension.asInstanceOf[LocationDimension]
      preparedStatement.setObject(1,locationDimension.country)
      preparedStatement.setObject(2, locationDimension.province)
      preparedStatement.setObject(3, locationDimension.city)
    } else if(dimension.isInstanceOf[EventDimension]) {
    val eventDimension = dimension.asInstanceOf[EventDimension]
    preparedStatement.setObject(1, eventDimension.event_name)
    preparedStatement.setObject(2, eventDimension.event_description)
    preparedStatement.setObject(3, eventDimension.category)
  }
}



/**
    * 执行sql语句
    *
    * @param sqlArray
    * @param dimension
    * @param connection
    * @return
    */
  def executeSql(sqlArray: Array[String], dimension: Any, connection: Connection): Int = {
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try{
      val querySql = sqlArray(0)
      preparedStatement = connection.prepareStatement(querySql)
      //给sql语句赋值
      setSqlArgs(preparedStatement,dimension)
      //执行查询的sql语句
      resultSet = preparedStatement.executeQuery()
      if(resultSet.next()){
        //找到维度对应的id
        resultSet.getInt(1)
      }else{
        //找不到维度对应的id
        val insertSql = sqlArray(1)
        preparedStatement = connection.prepareStatement(insertSql,Statement.RETURN_GENERATED_KEYS)
        //给sql赋值
        setSqlArgs(preparedStatement,dimension)
        //执行插入的sql语句
        if(preparedStatement.executeUpdate()>0){
          resultSet = preparedStatement.getGeneratedKeys
          if(resultSet.next()){
            resultSet.getInt(1)
          }else{
            throw new SQLException(
              """
                |Usage:com.beicai.dao.DimensionDao
                |errorMessage:从数据库中获取维度id失败
              """.stripMargin)
          }
        }else{
          throw new SQLException(
            """
              |Usage:com.beicai.dao.DimensionDao
              |errorMessage:维度插入失败
            """.stripMargin)
        }
      }
    }catch {
      case e: Exception => throw e
    }finally {
      if(resultSet!=null)
        resultSet.close()
      if(preparedStatement != null)
        preparedStatement.close()
    }
  }

  /**
    * 获取维度id
    *
    * @param dimension
    * 传入的维度类型
    * @param connection
    *
    */

  def getDimensionId(dimension:Any,connection:Connection)={
    var sqlArray:Array[String]=null
    if(dimension.isInstanceOf[DateDimension]){
      sqlArray=Array(
        "select id from dimension_date where year=? and season=? and month=? and week=? and day=? and calendar=? and type=?",
        "insert into dimension_date(year,season,month,week,day,calendar,type)values(?,?,?,?,?,?,?)"
      )
    }else if(dimension.isInstanceOf[PlatformDimension]){
      sqlArray=Array(
        "select id from dimension_platform where platform_name=?",
        "insert into dimension_platform(platform_name)values(?)"
      )
    }else if(dimension.isInstanceOf[LocationDimension]){
      sqlArray=Array(
        "select id from dimension_location where country=? and province=? and city=?",
        "insert into dimension_location(country,province,city)values(?,?,?)"
      )
    }else if(dimension.isInstanceOf[EventDimension]){
      sqlArray = Array(
        "select id from dimension_event where event_name=? and event_description=? and category=?",
        "insert into dimension_event(event_name,event_description,category)values(?,?,?)"
      )
    }
    synchronized({
      //执行sql语句
      executeSql(sqlArray,dimension,connection)
    })
  }
}
