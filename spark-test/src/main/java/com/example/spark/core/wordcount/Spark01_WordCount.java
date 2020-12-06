package com.example.spark.core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 简单而又经典的wordCount例子。
 * <p>
 * 1、函数式编程，在编写lambda表达式时，要注意，思想是对每个元素进行同样的处理。
 * 2、textFile可以读取单个文件，也可以直接读取路径下的所有文件。
 * <p>
 * {@link Tuple2} 类似于
 *
 * @Author Sakura
 * @Date 2020/7/11 11:30
 */
public class Spark01_WordCount {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("myWordCount");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
          业务逻辑
         */
        // 1、读取文件
        JavaRDD<String> fileRdd = sc.textFile("input");
        // 2、扁平化。或者说是切割
        JavaRDD<String> flatMapRdd = fileRdd.flatMap(s -> {
            String[] split = s.split(" ");
            return Arrays.asList(split).iterator();
        });

        // 3、分组。
        JavaPairRDD<String, Iterable<String>> groupByRdd = flatMapRdd.groupBy(v1 -> v1);

        // 4、聚合。
        JavaPairRDD<String, Integer> mapRdd = groupByRdd.mapToPair(strTuple2 -> {
            int counter = 0;
            for (String s : strTuple2._2()) {
                counter++;
            }
            return new Tuple2<>(strTuple2._1(), counter);
        });

        // 5、收集统计值。
        List<Tuple2<String, Integer>> wordCount = mapRdd.collect();

        System.out.println(wordCount.toString());

        /*
          关闭环境
         */
        sc.stop();
    }
}
