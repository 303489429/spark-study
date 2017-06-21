package cn.hadoop.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // cache()或者persist()的使用，是有规则的
        // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
        // 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
        // 而且，会报错，大量的文件会丢失
        JavaRDD<String> lines = sc.textFile("d://spark//spark2.txt");
        lines.cache();
        long s1 = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long s2 = System.currentTimeMillis();
        System.out.println("cost1 "+(s2-s1) + " ms");

         s1 = System.currentTimeMillis();
         count = lines.count();
        System.out.println(count);
         s2 = System.currentTimeMillis();
        System.out.println("cost2 "+(s2-s1) + " ms");
    }
}
