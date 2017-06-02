package scala.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangzhilong on 2017/6/2.
  */
object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SortWordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("d://spark//spark.txt")
    val words = lines.flatMap(line => line.split(" "))
    val paris = words.map(word => (word,1))
    val wordCounts = paris.reduceByKey(_+_)

    val countWords = wordCounts.map(wordCount => (wordCount._2,wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    val sortedWordCounts = sortedCountWords.map(sortedCountWord => (sortedCountWord._2,sortedCountWord._1))
    sortedWordCounts.foreach(l => println(l._1 +" appear " + l._2 + " times."))

  }
}
