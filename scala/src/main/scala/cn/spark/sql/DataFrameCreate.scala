package scala.cn.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/6.
  */
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc) ; //2.0.0之后采用SparkSession.builder方法
    val spark = SparkSession  //2.0.0之后创建sqlContext
      .builder()
      .appName("DataFrameCreate")
      .getOrCreate()

    new HiveContext(sc) ;

    val df = sqlContext.read.json("d://spark/students.json")
    spark.sqlContext.read.json()
    val df2 = spark.read.json("d://spark/students.json")
    df.show()
    df2.show()
  }

}
