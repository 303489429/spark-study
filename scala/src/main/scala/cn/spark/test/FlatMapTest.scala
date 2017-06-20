package scala.cn.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by wangzhilong on 2017/6/13.
  */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("FlatMap")
      .getOrCreate()

    import spark.implicits._
    val lineArr = Array(("leo wzl zou please"),"hello world game")
    val lines = spark.sparkContext.parallelize(lineArr)
    lines.flatMap{ line =>
      val line2s = new Array[(String,String,String)](0)
      val splits = line.split(" ")
      if(splits.length > 10){
        splits
      }
      line2s
    }.toDF("name","age","pwd").filter( $"name" =!= "").show()
  }

}
