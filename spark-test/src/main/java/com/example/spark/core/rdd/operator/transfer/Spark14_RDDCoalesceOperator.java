package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *   coalesce算子。
 *
 *   coalesce翻译为合并。功能是缩减分区。
 *   比如进行filter算子后，有的分区已经没有数据了，就可以用coalesce来缩减分区，平衡分区数据。
 *
 *   虽然coalesce可以修改分区，但是并不能用来扩大分区。因为coalesce不会将数据打乱重新组合。
 *   扩大分区是没意义的。如果想要扩大分区，那么必须使用shuffle，打乱数据，重新组合。所以，coalesce操作是不会产生shuffle的。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/16 19:30
 **/
public class Spark14_RDDCoalesceOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("join-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 2个分区
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 1, 2, 2, 2), 2);

        // 筛选保留偶数数据。会有一个分区是空的。
        //JavaRDD<Integer> filterRdd = rdd.filter(i -> i % 2 == 0);

        // 将分区缩减为1个。
        JavaRDD<Integer> coalesceRdd = rdd.coalesce(1);

        coalesceRdd.saveAsTextFile("output");
    }
}
