package scala.cn.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加变量
  * Created by wangzhilong on 2017/6/5.
  */
object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AccumulatorVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.longAccumulator("longAccum");
    val sum2 = sc.accumulator(0)

    val numberList = Array(1,2,3,4,5);
    val numbers = sc.parallelize(numberList);
    numbers.foreach(num => sum.add(num))
    numbers.foreach(num => sum2 += num)
    println("sum="+sum.value)
    println("sum2="+sum2)
  }

}
