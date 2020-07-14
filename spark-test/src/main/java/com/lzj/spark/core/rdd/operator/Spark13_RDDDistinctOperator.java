package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     distinct算子：
 *
 *     1、给数据集去重的算子。
 *     2、去重后数据顺序可能会发生改变。
 *     注意：distinct算子会导致数据不均匀，即可能产生数据倾斜的问题。
 *     所以也会带有一个numPartitions参数，可以手动改善数据倾斜。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/14 23:27
 */
public class Spark13_RDDDistinctOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("distinct-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 3, 4, 5, 4));

        // 小练习；去重
        JavaRDD<Integer> distinct = rdd.distinct();

        List<Integer> collect = distinct.collect();

        System.out.println(collect);
    }
}
