package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *    reduceByKey算子。
 *
 *    根据数据的key进行分组，然后对value进行聚合。
 *
 *    有可能在聚合后造成分区数据不均匀的情况，所以第二个参数可以修改分区大小。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/17 19:43
 **/
public class Spark20_RDDReduceByKeyOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("groupByKey-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("key1", 1),
                new Tuple2<>("key2", 1),
                new Tuple2<>("key1", 1),
                new Tuple2<>("key2", 1),
                new Tuple2<>("key1", 1)
        ));

        JavaPairRDD<String, Integer> rdd2 = rdd.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        List<Tuple2<String, Integer>> collect = rdd2.collect();

        System.out.println(collect.toString());

    }
}
