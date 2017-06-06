package cn.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class TransformationOperation {

    private static SparkConf conf =  new SparkConf()
                .setAppName("TransformationOperation")
                .setMaster("local");
    private static JavaSparkContext sc = new JavaSparkContext(conf) ;

    public static void main(String[] args) {
        //map();
        //reduceByKey();
        //join();
        cogroup();
    }

    private static void map(){
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> maps = numberRDD.map(v1 -> v1 * 2);

        System.out.println(maps.collect());
    }

    private static void reduceByKey(){
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2("class1",80),
                new Tuple2("class2",75),
                new Tuple2("class1",90),
                new Tuple2("class2",65)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> totalScores = scores.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println(totalScores.collect());
    }

    private static void join(){
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60));

        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<String, Integer>> joins = students.join(scores);

        System.out.println(joins.collect());
    }

    private static void cogroup(){
        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(4, 80),
                new Tuple2<Integer, Integer>(3, 50));

        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = students.cogroup(scores);

        //JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<String>>> cogroup1 = scores.cogroup(students);

        JavaPairRDD<Integer, Tuple2<String, Integer>> joins = students.join(scores);

        cogroup.foreach( t -> {
           System.out.println("student id: " + t._1);
           System.out.println("student name: " + t._2._1);
           System.out.println("student score: " + t._2._2);
           System.out.println("===============================");
       });
    }
}
