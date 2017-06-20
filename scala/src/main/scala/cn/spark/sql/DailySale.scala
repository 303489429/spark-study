package scala.cn.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by wangzhilong on 2017/6/14.
  */
object DailySale {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailySale").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    // 模拟数据
    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1122",
      "2015-10-01,23.15,1122",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1122")

    val userSaleLogRDD = spark.sparkContext.parallelize(userSaleLog)
      .filter(log => log.split(",").length ==3 )


    val userSaleLogRowRDD = userSaleLogRDD.map(log => Row(log.split(",")(0),log.split(",")(1).toDouble,log.split(",")(2).toInt))

    val structType = StructType(Seq(
      StructField("date",StringType,true),
      StructField("sale_amount",DoubleType,true),
      StructField("uuid",IntegerType,true)))

    val userSaleLogDF = spark.createDataFrame(userSaleLogRowRDD,structType)


    userSaleLogDF.groupBy("date","uuid")
      .agg(countDistinct("sale_amount").as("taskSize"),
        count("sale_amount").as("totalSize"))
      .withColumn("rank",dense_rank().over(
        Window.partitionBy($"taskSize").orderBy($"totalSize".desc)
      )).show





  }

}
