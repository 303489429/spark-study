package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wangzhilong on 2017/6/2.
 */
public class LocalFile {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("d://spark//spark.txt");

        Integer counts = lines.map(line -> line.length()).reduce((v1, v2) -> v1 + v2);

        System.out.println("文件总字数："+counts);

    }
}
