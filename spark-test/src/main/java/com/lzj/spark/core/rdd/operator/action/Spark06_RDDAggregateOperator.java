package com.lzj.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *     Aggregate 行动算子
 *        Aggregate算子输入参数
 *          1、初始值
 *          2、分区内计算逻辑
 *          3、分区间计算逻辑
 *
 *        Aggregate和AggregateByKey的区别？
 *          1、aggregateByKey：初始值只参与到分区内计算。
 *          2、aggregate：初始值在分区内计算会参与，在分区间计算也会参与。
 *            比如下面小练习中，初始值为10时，结果为40.
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 10:12
 */
public class Spark06_RDDAggregateOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("sumAndAggregate-action");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(2, 1, 3, 4), 2);

        Integer aggregate = rdd.aggregate(10, (v1, v2) -> v1 + v2, (v1, v2) -> v1 + v2);

        // 注意：当初始值为0时，输出10。当初始值为10时，输出40.
        System.out.println(aggregate);
    }
}
