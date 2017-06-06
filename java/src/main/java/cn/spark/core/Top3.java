package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("d://spark//top.txt");
        lines.distinct()
                .mapToPair(line -> new Tuple2<Integer, String>(Integer.valueOf(line),line))
                .sortByKey(false)
                .take(3)
                .forEach(l -> System.out.println(l._2));
        sc.close();
    }
}
