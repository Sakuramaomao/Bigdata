package com.example.spark.core.rdd.dep;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     Spark依赖关系（或者称之为RDD血缘关系）
 *
 *    小练习
 *        从小练习的输出结果中可以看出，新RDD都会包含并依赖前面产生的RDD，形成一种依赖关系。
 *   （前面括号中数字代表分区数）。
 *
 *   本小练习的输出debugger日志。
 *   (4) ParallelCollectionRDD[0] at parallelize at Spark01_Dependence.java:39 []
 *   (4) MapPartitionsRDD[1] at flatMap at Spark01_Dependence.java:43 []
 *    |  ParallelCollectionRDD[0] at parallelize at Spark01_Dependence.java:39 []
 *   (4) MapPartitionsRDD[2] at mapToPair at Spark01_Dependence.java:50 []
 *    |  MapPartitionsRDD[1] at flatMap at Spark01_Dependence.java:43 []
 *    |  ParallelCollectionRDD[0] at parallelize at Spark01_Dependence.java:39 []
 *   (4) ShuffledRDD[3] at reduceByKey at Spark01_Dependence.java:55 []
 *    +-(4) MapPartitionsRDD[2] at mapToPair at Spark01_Dependence.java:50 []
 *       |  MapPartitionsRDD[1] at flatMap at Spark01_Dependence.java:43 []
 *       |  ParallelCollectionRDD[0] at parallelize at Spark01_Dependence.java:39 []
 *
 *    为什么有依赖关系？
 *       在分布式计算中，不可避免会出现某个节点任务计算失败的情况。Spark提供了计算失败后重试的机制。
 *    由于RDD并不保存实际的数据，它只保存数据的元数据信息（比如数据从哪来，数据经历的计算逻辑和过程），
 *    所以通过RDD依赖关系图，来记录数据经历的计算过程。当某个Task失败时，其他节点可以通过依赖关系图
 *    重新计算。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 15:21
 */
public class Spark01_Dependence {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("serialize");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 拿wordCount例子来说明RDD依赖关系
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("abc def", "abc dfe"));

        System.out.println(rdd.toDebugString());

        JavaRDD<String> flatMapRdd = rdd.flatMap((FlatMapFunction<String, String>) line -> {
            String[] split = line.split(" ");
            return Arrays.asList(split).iterator();
        });

        System.out.println(flatMapRdd.toDebugString());

        JavaPairRDD<String, Integer> mapRdd = flatMapRdd.mapToPair(
                (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        System.out.println(mapRdd.toDebugString());

        JavaPairRDD<String, Integer> reduceRdd = mapRdd.reduceByKey(
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        System.out.println(reduceRdd.toDebugString());

        List<Tuple2<String, Integer>> collect = reduceRdd.collect();

        System.out.println(collect);
    }
}
