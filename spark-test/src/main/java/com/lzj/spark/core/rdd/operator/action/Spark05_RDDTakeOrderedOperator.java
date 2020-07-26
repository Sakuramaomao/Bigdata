package com.lzj.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     takeOrdered 行动算子
 *         返回RDD元素排序后的前n个元素组成的数组。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 10:12
 */
public class Spark05_RDDTakeOrderedOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("takeOrdered-action");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(2, 1, 3, 4, 5), 2);

        List<Integer> list = rdd.takeOrdered(2);

        // 结果：[1, 2]
        System.out.println(list);
    }
}
