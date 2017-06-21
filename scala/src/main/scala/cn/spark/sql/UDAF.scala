package scala.cn.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by wangzhilong on 2017/6/21.
  */
object UDAF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDAF")
      .master("local")
      .getOrCreate()


    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo")
    val namesRDD = spark.sparkContext.parallelize(names, 5)
    val namesRowRDD = namesRDD.map { name => Row(name) }
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = spark.createDataFrame(namesRowRDD, structType)

    namesDF.createOrReplaceTempView("names")

    spark.udf.register("strCount",new StringCount)

    spark.sql("select name,strCount(name) from names group by name").show()


  }

}
