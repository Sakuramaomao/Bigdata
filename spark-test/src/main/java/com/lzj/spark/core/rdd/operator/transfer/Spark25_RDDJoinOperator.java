package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     join算子。
 *       join操作类似数据库表的内连接。
 *       也有笛卡尔积和shuffle现象。
 *       所以日常使用中能不用join就不用join，性能比较低下。
 *
 *       如果key重复？
 *          如果key重复，会多次连接的。出现笛卡尔积。
 *
 *     leftOuterJoin和rightOuterJoin的效果与数据库表外连接的效果都是一致的。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 22:06
 */
public class Spark25_RDDJoinOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("combineByKey-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("c", 4),
                new Tuple2<>("b", 3),
                new Tuple2<>("e", 4)
        ));

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 2),
                new Tuple2<>("a", 3),
                new Tuple2<>("c", 5),
                new Tuple2<>("b", 4)
        ));

        // 小练习：内连接。
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = rdd.join(rdd2);

        // 小练习：外连接。（右连接类似，这里就不练习了）
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftJoin = rdd.leftOuterJoin(rdd2);

        System.out.println("join: " + join.collect());
        System.out.println("left join: " + leftJoin.collect());

        sc.stop();
    }
}
