package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/2.
  */
object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)

    val rdd = sc.parallelize(numbers)

    val sum = rdd.reduce(_ + _)

    println("1到10的累加和：" + sum)
  }
}
