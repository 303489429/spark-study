package scala.cn.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/2.
  */
object LocalFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LocalFile")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d://spark//spark.txt")
    val counts = lines.map(line => line.length).reduce(_ + _)
    println("file's count is "+ counts)
  }

}
