package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class ActionOperation {

    private static SparkConf conf =  new SparkConf()
            .setAppName("ActionOperation")
            .setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf) ;

    public static void main(String[] args) {
        countByKey();
    }

    private static void countByKey(){
        // 模拟集合
        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class1", "marry"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "david"));

        JavaPairRDD<String, String> scores = sc.parallelizePairs(scoreList);
        Map<String, Long> studentCounts = scores.countByKey();
        System.out.println(studentCounts);
    }
}
