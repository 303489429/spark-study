package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Sorting

/**
  * Created by wangzhilong on 2017/6/5.
  */
object GroupTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GroupTop3")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("d://spark/score.txt")
    val results = lines.map(line => (line.split(" ")(0),line.split(" ")(1).toInt))
      .groupByKey()
      .map{line =>
//        val top3 = new Array[Int](3) ;
//        line._2.iterator.foreach(score => {
//          import scala.util.control.Breaks._
//          breakable{
//            for(i <- 0 to 2){
//              if(top3(i) == null){
//                top3(i) = score.toInt
//                break
//              }else if(score.toInt > top3(i)){
//                for(j <- 2 to 10 if j < i ){
//                  top3(j) = top3(j-1)
//                }
//                top3(i) = score.toInt
//                break
//              }
//            }
//          }
//        })
//        (line._1,top3)
        //实现2
        val scores = line._2.iterator.toArray.distinct
        Sorting.quickSort(scores)
        val top3 = new Array[Int](3)
        for(i <- 0 until scores.length) {
          if(i < 3){
            top3(i)= scores(scores.length -1-i)
          }
        }
        (line._1,top3)
      }.foreach( result => {
      println("class:"+result._1+",score:"+result._2.mkString(","))
    })


  }

}
