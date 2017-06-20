package scala.cn.spark.test

import com.google.gson.JsonArray

import scala.collection.mutable.ArrayBuffer


/**
  * Created by wangzhilong on 2017/6/12.
  */
class JsonWrapper(jsonArray: JsonArray) {

  def sort2ViaPoint():ArrayBuffer[ViaPoint] = {
    val jsonSortds = new ArrayBuffer[ViaPoint]
    for(i <- 0 until jsonArray.size()){
      val via = jsonArray.get(i).getAsJsonObject
      if(via.has("distinct")){
        val distinct = via.get("distinct").getAsString
      }
      jsonSortds += ViaPoint(
        via.get("point").getAsString,
        via.get("name").getAsString,
        via.get("startTime").getAsString,
        via.get("endTime").getAsString,
        via.get("city").getAsString,
        via.get("seconds").getAsInt,
        via.get("distinct").getAsDouble)
    }
    jsonSortds.sorted
  }
}
