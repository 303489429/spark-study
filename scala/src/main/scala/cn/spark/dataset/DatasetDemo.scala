package scala.cn.spark.dataset

import com.google.gson.JsonParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
  * Created by wangzhilong on 2017/6/16.
  */
object DatasetDemo {
  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetDemo")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val employee = spark.read.json("d://spark//employee.json")
    val employee2 = spark.read.json("d://spark//employee2.json")
    val department = spark.read.json("d://spark//department.json")

    //    employee.show()
    //    department.show()
//    employee
//      .join(department,employee("depId") === department("id"))
//      .withColumn("num",struct(employee("name"),employee("salary"))).show()
//
//   val filterEmp = employee.filter($"age" > 30)
//     .as[Employee]
//     .map(emp => (emp.age,emp.name,emp.gender,emp.depId,emp.salary,emp.salary * 190))
//     .toDF("age","name","gender","depId","salary","s190").select("age","name","gender","depId","salary","s190")
//
//    employee.select("age","name","gender","depId","salary").union(filterEmp.drop("s190")).show()

//    employee.na.fill("hah",Seq("gender")).show()

//    employee.join(employee2,Seq("name","age")).drop("depId","salary").write.json("d://spark//output")

//    println("src:" +employee.rdd.partitions.size)
//
//   println( "repartition(7)= "+employee.repartition(7).rdd.partitions.size)
//
//    println("coalesce(3):"+employee.coalesce(3).rdd.partitions.size)

    // except：获取在当前dataset中有，但是在另外一个dataset中没有的元素
    // filter：根据我们自己的逻辑，如果返回true，那么就保留该元素，否则就过滤掉该元素
    // intersect：获取两个数据集的交集
    // map：将数据集中的每条数据都做一个映射，返回一条新数据
    // flatMap：数据集中的每条数据都可以返回多条数据
    // mapPartitions：一次性对一个partition中的数据进行处理
    // distinct和dropDuplicates
    // 都是用来进行去重的，区别在哪儿呢？
    // distinct，是根据每一条数据，进行完整内容的比对和去重
    // dropDuplicates，可以根据指定的字段进行去重
    // coalesce和repartition操作
    // 都是用来重新定义分区的
    // 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
    // repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作

//    employee.except(employee2).show()

//    employee.intersect(employee2).show()
//    employee.sort($"salary".desc).show()

//    employee.randomSplit(Array(3,10)).foreach(ds => ds.show())


//    val employee3 = employee2.withColumn("rank",lit("0")).select($"name",$"age",$"salary",$"rank")
//    employee3.show()
//    val employee4 = employee3.drop($"rank")
//    employee4.show()
//    employee.select($"name",$"age",$"salary").union(employee4)show()

//    employee.join(department,$"depId" === $"id")
//      .groupBy(department("name"))
//      .agg(avg(employee("salary")),sum(employee("salary")),max(employee("salary")))
//      .show()
//
//    employee
//      .groupBy($"depId")
//      .agg(collect_list("name"),collect_set("name"))
//      .collect()
//      .foreach(println)


    //窗口函数
    val frame = employee
      .join(department, $"depId" === $"id")
      .select(department("name").as("departName"), employee("name").as("name"), $"age", $"gender", $"salary")
      .cache

    val t1 = frame
      .withColumn("rank",
        dense_rank().over(Window.partitionBy("departName").orderBy($"salary".desc))
        )
      .withColumn("sum",
        sum("salary") over(Window.partitionBy("departName").orderBy($"salary".desc))
        )

    val t2 = frame
      .groupBy("departName")
      .agg(sum("salary").as("total"))

    val all = t1.join(t2,t1("departName") === t2("departName")).drop(t1("departName"))

    all.show()


  }

}
