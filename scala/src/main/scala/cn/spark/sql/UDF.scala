package scala.cn.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by wangzhilong on 2017/6/20.
  */
object UDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UDF")
      .master("local")
      .getOrCreate()

    val names = Array("Leo","Marry","Jack","Tom")

    val rdd = spark.sparkContext.parallelize(names)
      .map( name => Row(name))

    val structType = StructType(Array(StructField("name",StringType,true)))

    val df = spark.createDataFrame(rdd,schema = structType)

    df.createOrReplaceTempView("names")

    //定义和注册自定义函数
    //定义函数：自己写匿名函数
    //注册函数：spark.udf.register
    spark.udf.register("strLen",(str:String) => str.length)

    spark.sql("select name,strLen(name) from names").show()


  }

}
