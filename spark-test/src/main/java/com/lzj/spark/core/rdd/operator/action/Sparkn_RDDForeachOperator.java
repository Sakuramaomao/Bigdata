package com.lzj.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * <pre>
 *     foreach算子。
 *
 *     这里要分清两个概念。方法和算子。
 *        rdd中的方法都称之为算子。因为算子的执行方式不同。算子是在分布式计算节点Executor中完成的。
 *     而方法是在当前节点（Driver）内存中计算完成的。
 *         算子的逻辑代码是在分布式计算节点中执行的，算子之外的代码是在Driver中执行的，比如Main函数
 *      中的其他非算子代码。
 *
 *     foreach方法和foreach算子？
 *        foreach属于集合中的方法，是在当前节点(Driver)内存中完成的。
 *     rdd的foreach方法是算子。算子的逻辑是在分布式计算节点Executor中执行的。
 *     foreach算子可以将循环在不同的计算节点完成。所以打印顺序每次都不一致。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 23:04
 */
public class Sparkn_RDDForeachOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("foreach-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        // foreach属于集合中的方法，是在当前节点(Driver)内存中完成的。
        rdd.collect().forEach(System.out::println);

        System.out.println("***************************");

        // rdd的foreach方法是算子。算子的逻辑是在分布式计算节点Executor中执行的。
        // foreach算子可以将循环在不同的计算节点完成。
        rdd.foreach(i -> System.out.println(i));
    }
}
