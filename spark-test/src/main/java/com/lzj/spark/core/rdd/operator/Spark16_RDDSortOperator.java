package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * <pre>
 *    SortBy算子。
 *
 *
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/16 20:19
 **/
public class Spark16_RDDSortOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("repartition-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(4, 2, 1, 7, 5, 6), 2);

        // 1、第一个参数是比较规则。
        rdd.sortBy((Function<Integer, Integer>) v1 -> v1, false, 2);
    }
}
