package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/5.
  */
object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("d://spark//sort.txt")
    lines.map{line => (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),line)}
      .sortByKey()
      .map(sortedPair => sortedPair._2)
      .foreach(println(_))


  }

}
