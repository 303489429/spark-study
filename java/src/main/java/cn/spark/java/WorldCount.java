package cn.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by wangzhilong on 2017/6/1.
 */
public class WorldCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WorldCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("D:\\spark\\spark.txt");
        JavaRDD<String> words = lineRDD.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((v1, v2) -> v1 + v2);
        wordCounts.foreach(line -> System.out.println(line._1+" appeared " + line._2 + "times."));
        sc.close();
    }
}
