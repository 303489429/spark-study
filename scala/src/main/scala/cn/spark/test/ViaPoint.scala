package scala.cn.spark.test

import org.joda.time.format.DateTimeFormat

/**
  * Created by wangzhilong on 2017/6/12.
  */
case class ViaPoint(val point:String, val name: String, val startTime: String, val endTime:String, val city:String,
                    val seconds: Int, val distinct: Double) extends Ordered[ViaPoint]{
  override def compare(that: ViaPoint): Int = {
    //日期格式化
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
    val start = format.parseDateTime(this.startTime).getMillis
    val end = format.parseDateTime(that.startTime).getMillis
    //日期格式化
    (start - end).toInt
  }
}
