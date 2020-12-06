package com.example.spark.core.rdd.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     Spark之RDD处理数据缓存。
 *        由于<b>RDD不保存数据</b>，所以有重复利用RDD的代码时，就需要重复之前的计算。
 *     比如map后的rdd既想要collect，又想要保存成文件。
 *        但是rdd提供了缓存操作，将rdd处理后的数据缓存到Executor节点的内存中，供下
 *     一个rdd计算使用。这样就避免了前面的重复计算。
 *
 *     会不会内存溢出？
 *         如果缓存的数据过大，那么可能会造成内存溢出。当内存不够用时，Executor也有
 *     会去清除缓存的数据。之后的rdd就需要重复前面的计算步骤了。
 *
 *     cache会影响血缘吗？中断血缘吗？
 *         cache操作不会影响血缘。当执行action算子时，会增加一个cacheDependency
 *     依赖。cache操作也不会中断血缘。带来的好处是可以在节点计算失败的时候进行重试，
 *     重新执行计算。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 21:24
 */
public class Spark01_Persist {
    public static void main(String[] args) {
         /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("persist");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));

        JavaPairRDD<Integer, Integer> mapRdd = rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                System.out.println("----map----");
                return new Tuple2<>(i, 1);
            }
        });

        JavaPairRDD<Integer, Integer> cache = mapRdd.cache();

        // 血缘关系是否中断的测试
        System.out.println(cache.toDebugString());

        // 既要对mapRdd收集，也要保存成文件。
        System.out.println(cache.collect());

        // 血缘关系是否中断的测试
        System.out.println(cache.toDebugString());

        cache.saveAsTextFile("output");
    }
}
