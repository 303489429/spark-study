package scala.cn.spark.test

import org.apache.spark.sql.SparkSession

/**
  * Created by wangzhilong on 2017/6/20.
  */
object CataLogTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetDemo")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    spark.catalog.listDatabases().foreach(f => println(f))

    spark.catalog.listTables().show()

  }
}
