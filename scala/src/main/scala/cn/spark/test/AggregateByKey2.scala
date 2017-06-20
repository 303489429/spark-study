package scala.cn.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/20.
  */
object AggregateByKey2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AggregateByKey2")
      .setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)
    var data = sc.parallelize(List((1,3),(1,2),(1, 4),(2,3)))
    def seq(a:Int, b:Int) : Int ={
       println("seq: " + a + "\t " + b)
       math.max(a,b)
    }

    def comb(a:Int, b:Int) : Int ={
      println("comb: " + a + "\t " + b)
      a + b
    }

    val arr = data.aggregateByKey(1)(seq, comb)

    arr.foreach(println)
  }

}
