package scala.cn.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by wangzhilong on 2017/6/20.
  */
object SparkSQLDemo {

  case class People(name:String ,age:Long)

  case class Record(key:Int, value : String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()  // 用到了java里面的构造器设计模式
      .appName("SparkSQLDemo")
      .master("local")
      // spark.sql.warehouse.dir，必须设置
      // 这是Spark SQL 2.0里面一个重要的变化，需要设置spark sql的元数据仓库的目录
//       .config("spark.sql.warehouse.dir", "d:\\git\\spark-study\\spark-warehouse")
      // 启用hive支持
//      .enableHiveSupport()
      .getOrCreate()

     import spark.implicits._

    val df = spark.read.json("d://spark//people.json")
    df.show()

    df.as[People].filter(p => p.age > 22).show()

    spark.sql("create table if not exists src(key int,value string)")
//    spark.sql("load data local inpath 'kv1.txt' into table src") //此语句不被Windows本地导入支持
//    spark.sql("insert into src values(1,'leo'),(2,'jetty'),(3,'kitty')") //此语句需要启用hive 支持
    spark.sql("select * from src").show()

  }
}
