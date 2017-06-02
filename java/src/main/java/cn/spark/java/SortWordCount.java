package cn.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by wangzhilong on 2017/6/2.
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("d://spark//spark.txt");
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(line -> new Tuple2<>(line._2, line._1))
                .sortByKey(false)
                .mapToPair(line -> new Tuple2<>(line._2, line._1))
                .foreach(line -> System.out.println(line._1 +" appers " + line._2 +" times."));

    }
}
