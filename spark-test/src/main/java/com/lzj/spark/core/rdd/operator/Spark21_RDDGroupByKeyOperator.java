package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *    groupByKey算子。
 *
 *    根据指定的key进行分组，相同key对应的是个迭代器集合。
 *    和groupBy的区别在于groupBy可以自定义分组规则，而groupByKey只能依据key来分组。
 *
 *    其实groupByKey拿到可迭代器，是可以再进一步做聚合操作的。这也是reduceByKey比它多的一个小功能。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/17 19:54
 **/
public class Spark21_RDDGroupByKeyOperator {
    public static void main(String[] args) {
/*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("myPartitioner-rdd");

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

        // 返回的元组中第一个元素是key。
        // 返回的元组中第二个元素是根据key分组后，相同key的value集合。
        JavaPairRDD<String, Iterable<Integer>> rdd1 = rdd.groupByKey();

        System.out.println(rdd1.collect());
    }
}
