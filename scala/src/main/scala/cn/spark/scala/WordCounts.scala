package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 特别注意编译spark的scala版本是否一致。
  * Created by wangzhilong on 2017/6/1.
  */
object WordCounts {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCounts")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\spark\\spark.txt")
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(wordCount => println(wordCount._1 +" appeared "+ wordCount._2 + "times."))
  }
}
