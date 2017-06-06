package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/5.
  */
object Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("persist")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("d://spark//spark2.txt").cache()

    var s1 = System.currentTimeMillis() ;
    var count = lines.count() ;
    println(count)
    var s2 = System.currentTimeMillis();
    println("cost " + (s2-s1) +" ms.")


    s1 = System.currentTimeMillis() ;
    count = lines.count() ;
    println(count)
    s2 = System.currentTimeMillis();
    println("cost2 " + (s2-s1) +" ms.")



  }

}
