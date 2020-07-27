package com.lzj.spark.core.rdd.acc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * <pre>
 *     累加器。
 *         这是一个小练习。
 *         使用累加器来消除用来求和的reduce算子。
 *
 *      注意：
 *          累加器的声明不能使用JavaSparkContext，只能使用SparkContext。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/27 22:11
 */
public class Spark01_Accumulate {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("partitionBy-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        SparkContext sparkContext = new SparkContext(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        // 有Shuffle操作的算子
        //Integer result = rdd.reduce(Integer::sum);

        // 声明累加器
        LongAccumulator sum = sparkContext.longAccumulator("sum");

        // 使用累加器。
        rdd.foreach(i -> sum.add(i));

        // 获取累加器中的值
        System.out.println(sum.value());

        //System.out.println(result);
    }
}
