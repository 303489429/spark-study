package cn.hadoop.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class GroupTo3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AccumulatorVariable")
                .setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("d://spark//score.txt");
        lines.mapToPair(line -> new Tuple2<>(line.split(" ")[0],
                                    Integer.valueOf(line.split(" ")[1])))
                .groupByKey()
                .mapToPair( classScores -> {
                    Integer[] top3 = new Integer[3] ;
                    Iterator<Integer> scores = classScores._2.iterator();
                    while (scores.hasNext()){
                        Integer score = scores.next();
                        for (int i = 0; i < 3; i++) {
                            if(top3[i] == null){
                                top3[i] = score ;
                                break;
                            }else if(score > top3[i]){
                                for (int j = 2; j > i; j--) {
                                    top3[j] = top3[j-1] ;
                                }
                                top3[i] = score ;
                                break;
                            }
                        }
                    }
                    return new Tuple2<String, Iterable<Integer>>(classScores._1, Arrays.asList(top3));
                }).foreach(t -> {
                    System.out.println("class: " + t._1);
                    Iterator<Integer> scoreIterator = t._2.iterator();
                    while(scoreIterator.hasNext()) {
                        Integer score = scoreIterator.next();
                        System.out.println(score);
                    }
                    System.out.println("=======================================");
                });
    }
}
