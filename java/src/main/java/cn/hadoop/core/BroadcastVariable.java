package cn.hadoop.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final int factor = 3 ;
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5) ;
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        numbers.map(n -> n * factorBroadcast.value())
                .foreach(n -> System.out.println(n));
        sc.close();
    }
}
