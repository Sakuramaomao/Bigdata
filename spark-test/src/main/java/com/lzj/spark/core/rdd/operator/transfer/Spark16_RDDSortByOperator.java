package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * <pre>
 *    SortBy算子。
 *
 *   1、第一个参数是比较规则。按照哪个字段进行排序之类的。或者按照这个字段计算后的规则进行排序。
 *   2、升序还是降序。默认asc为true。
 *   3、排序后可以指定分区数。
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/7/16 20:19
 **/
public class Spark16_RDDSortByOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("sortby-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(4, 2, 1, 7, 5, 6), 2);

        // 1、第一个参数是比较规则。按照哪个字段进行排序之类的。或者按照这个字段计算后的规则进行排序。
        // 2、升序还是降序。默认asc为true。
        // 3、排序后可以指定分区数。
        JavaRDD<Integer> sortRdd = rdd.sortBy((Function<Integer, Integer>) v1 -> v1, false, 2);

        System.out.println(sortRdd.collect());
        sortRdd.saveAsTextFile("output");
    }
}
