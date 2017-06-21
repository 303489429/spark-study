package scala.cn.spark.test

import scala.collection.mutable

/**
  * Created by wangzhilong on 2017/6/20.
  */

case class SortBy[T](){
  val stats = new mutable.HashMap[T,Int]()

//  def sortByCountMap(asc : Boolean) = {
//    stats.toList.sorted
//  }

  def add(t:T,size:Int) = {
    val currtentValue = stats.getOrElse(t, 0)
    stats(t) = currtentValue + size
  }
}

object SortBy {

  def main(args: Array[String]): Unit = {
     val map = mutable.Map("Leo"-> 19,"Jerry"->20, "Abao"-> 17,"yanglin" -> 33)

    //case子句　可以判断类型
    println(map.toList.sortBy{  case (_, value) => value })

    println(map.toList.sortBy(x => x._2))
  }

}
