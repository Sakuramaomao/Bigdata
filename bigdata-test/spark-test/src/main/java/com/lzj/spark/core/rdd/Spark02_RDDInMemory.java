package com.lzj.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * 从内存中创建RDD。
 * parallelize：并行。可以从List创建RDD。
 * 在Scala中还有一个makeRDD方法，对parallelize方法做了一层封装。而Java中并没有。
 * </pre>
 *
 *
 * @Author Sakura
 * @Date 2020/7/12 15:58
 */
public class Spark02_RDDInMemory {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("file-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 从内存中创建RDD
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rddInMem = sc.parallelize(list);

        // 收集
        List<Integer> collect = rddInMem.collect();
        collect.forEach(System.out::print);

        sc.stop();
    }
}
