package cn.hadoop.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SecondarySort")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("d://spark/sort.txt");

        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(s -> {
            String[] lineSplit = s.split(" ");
            SecondarySortKey key = new SecondarySortKey(Integer.valueOf(lineSplit[0]), Integer.valueOf(lineSplit[1]));
            return new Tuple2<>(key, s);
        });

        pairs.sortByKey().foreach(p -> System.out.println(p._2));
    }
}
