package scala.cn.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/5.
  */
object BroadcastVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BroadcastVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val factor = sc.broadcast(3);
    sc.parallelize(Array(1,2,3,4,5))
      .map(num => num * factor.value)
      .foreach(println(_))
  }
}
