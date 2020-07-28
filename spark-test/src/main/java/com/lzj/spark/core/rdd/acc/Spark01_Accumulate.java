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
 *     1、定义
 *       分布式共享只写变量。
 *
 *         这是一个小练习。
 *         使用累加器来消除用来统计记录个数的reduce算子。
 *
 *      2、累加器原理
 *          （1）将累加器注册到Spark中。
 *          （2）执行计算时，spark会将累加器发送到Executor执行计算。
 *          （3）计算完毕后，executor会将累加器的计算结果返回到driver端。
 *          （4）driver端获取多个累加器的结果，然后两两合并，最后得到累加器的执行结果。
 *
 *      3、注意
 *          （1）累加器的声明不能使用JavaSparkContext，只能使用SparkContext。
 *          （2）累加器不仅仅可以累计数值，还可以是累加集合、数据等等。
 *          （3）累加器不是算子，它是和RDD同级别的数据模型。
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
