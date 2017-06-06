package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/5.
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d://spark//top.txt")
    lines.distinct()
      .map(line => (line.toInt,line))
      .sortByKey(false)
      .map(line => line._2)
      .take(3)
      .foreach(println(_))
  }

}
