package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * filter算子：
 *  根据指定的规则筛选数据，符合规则的数据保留，不符合规则的数据丢弃。
 *
 * 注意：filter算子不会改变分区的个数，但是可能会导致分区间数据的不均衡。
 *      在生成环境中可能会导致数据倾斜。即filter算子也会产生shuffle操作。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/14 22:00
 */
public class Spark11_RDDFilterOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("filter-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // 小练习：过滤偶数
        JavaRDD<Integer> filter = rdd.filter((Function<Integer, Boolean>) v1 -> v1 % 2 == 0);

        // TODO 取出2020/05/07日的日志。
        // ......

        List<Integer> collect = filter.collect();

        System.out.println(collect);
    }
}
