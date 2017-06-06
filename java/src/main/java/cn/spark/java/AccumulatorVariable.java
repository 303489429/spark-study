package cn.spark.java;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by wangzhilong on 2017/6/5.
 */
public class AccumulatorVariable implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AccumulatorVariable")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);



        final Accumulator<Integer> sum = sc.accumulator(0); //已弃用的方法实现

        final LongAccumulator accsum = sc.sc().longAccumulator(); //使用AccumulatorV2 方式实现累加

        final AtomicInteger sum2 = new AtomicInteger(0);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5) ;
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        //numbers.persist(StorageLevel.MEMORY_ONLY());

        numbers.foreach(t -> sum.add(t));
        System.out.println("numbers1="+ numbers.collect());

        numbers.foreach(t -> sum2.addAndGet(t));  //此方法无法实现spark累加

        numbers.foreach(t -> accsum.add(t));

        System.out.println("numbers2="+ numbers.collect());

        System.out.println("sum="+sum.value());
        System.out.println("sum2="+sum2);
        System.out.println("sum3="+accsum.value());
        sc.close();
    }
}
