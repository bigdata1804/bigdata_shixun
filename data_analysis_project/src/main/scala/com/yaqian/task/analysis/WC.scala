package com.yaqian.task.analysis

import scala.io.Source

/**
  * Created by lenovo on 2019/5/20.
  */
object WC {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("D://a.txt")
    val lines = file.getLines().toList
    lines.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.size).foreach(println(_))
  }
}
