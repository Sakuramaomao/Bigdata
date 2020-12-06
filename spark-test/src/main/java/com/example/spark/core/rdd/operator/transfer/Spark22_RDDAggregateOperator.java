package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     AggregateByKey聚合算子。此算子在聚合的过程shuffle过程。
 *
 *     AggregateByKey聚合算子有三个参数
 *     1、zeroValue：零值。在分区内使用。用来在分区内进行比较的值。比如求分区内最值，只有单个元素时，会那它进行对比。
 *     2、Function2：输入两个值，返回一个值。在分区内使用。用来写分区内数据计算逻辑的。比如分区内数据求最值。
 *     3、Function2：输入两个值，返回一个值。在分区间使用。用来写分区间数据计算逻辑的。比如分区间数据求和。
 *
 *     AggregateByKey和ReduceByKey的区别：
 *        AggregateByKey和reduceByKey的区别在于。aggregateByKey算子可以分别定义分区内以及分区间
 *     的计算逻辑。reduceByKey算子在分区内以及分区间计算逻辑是一致的。
 *        当分区内和分区间计算逻辑一致时，比如依据key分区求和，此时结果就和reduceByKey一致了。可以使用另外一个算子
 *     foldByKey来实现这一相同的操作。比如下面的“小练习2”代码示例。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 13:01
 */
public class Spark22_RDDAggregateOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("aggregate-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                // 上面一个分区，下面一个分区
                new Tuple2<>("b", 4),
                new Tuple2<>("c", 3),
                new Tuple2<>("a", 3)
        ), 2);

        // 小练习1：求分区内最大值，分区间只和。
        JavaPairRDD<String, Integer> rdd1 = rdd.aggregateByKey(0,
                (Function2<Integer, Integer, Integer>) (v1, v2) -> {
                    int max = v1;
                    if (v2 > v1) {
                        max = v2;
                    }
                    return max;
                },
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        // 小练习2：依据key分组求和。
        JavaPairRDD<String, Integer> rdd2 = rdd.foldByKey(0, (v1, v2) -> v1 + v2);

        System.out.println(rdd1.collect());
        System.out.println(rdd2.collect());
    }
}
