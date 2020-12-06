package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *    repartition算子。
 *
 *    repartition方法其实就是coalesce方法的包装。之不过肯定使用了shuffle操作（因为源码中调用了coalesce方法，并且shuffle参数为true）。
 *    repartition可以让数据更均衡一些，可以有效防止数据倾斜问题的发生。
 *
 *    关于repartition和coalesce的选择。
 *    如果是缩减分区，一般就采用coalesce；如果是扩大分区，就采用repartition。
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/16 19:45
 **/
public class Spark15_RDDRepartitionOperator {
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

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        JavaRDD<Integer> rdd1 = rdd.repartition(3);

        rdd1.saveAsTextFile("output");
    }
}
