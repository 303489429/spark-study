package scala.cn.spark.test

import com.google.gson.{JsonArray, JsonParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by wangzhilong on 2017/6/14.
  */
object JsonObject {

  implicit def newJsonWrapper(jsonArray: JsonArray):JsonWrapper = new JsonWrapper(jsonArray)

  val json = "[{\"point\":\"ws2fvs\",\"name\":\"广东省韶关市翁源县翁城镇韶关老杨住宿\",\"startTime\":\"2017-06-08 21:40:34.0\",\"endTime\":\"2017-06-08 22:31:57.0\",\"city\":\"韶关市\",\"seconds\":3083,\"distinct\":308116.0}," +
    "{\"point\":\"ws0g1y\",\"name\":\"广东省东莞市望牛墩镇马沥大桥中堂水道附近\",\"startTime\":\"2017-06-09 03:22:50.0\",\"endTime\":\"2017-06-09 09:17:30.0\",\"city\":\"东莞市\",\"seconds\":21280,\"distinct\":447252.0}]" ;

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("JsonObjectTest")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val parse = new JsonParser
    val jsonArray = parse.parse(json).getAsJsonArray
    val points = jsonArray.sort2ViaPoint()
    val viaPoints = points.filter(v => v.seconds > 2 * 3600)
    val size = if(viaPoints != null ) viaPoints.size else 0
    var viaBy300Km = 0
    try {
      if (size > 0 && points.nonEmpty) {
        val filterPoint = points.filter(v => v != null && v.distinct != null && v.distinct > 300000)
        if (filterPoint != null && filterPoint.nonEmpty) {
          viaBy300Km = filterPoint.size
        }
      }
    } catch {
      case ex:Exception => println("viaBy300Km has Exception"+ex.getMessage)
    }
//    val viaBy300Km:Int = if(size > 0) points.filter(v => v != null && v.distinct != null).filter(v => v.distinct > 300 * 1000).size else 0
    if(size > 0 && viaBy300Km > 0){
      viaPoints.foreach(v => println(v))
    }else{
      println("NIL")
    }


    spark.close()
  }

}
