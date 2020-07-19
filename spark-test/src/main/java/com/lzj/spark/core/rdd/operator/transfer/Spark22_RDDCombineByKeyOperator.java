package com.lzj.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     CombineByKey算子。
 *
 *        如果发现相同key的value结构不符合计算规则的格式的话，那么选择combineByKey。
 *     比如，下面这个计算平均值的例子，普通的单值value不能满足计算要求，需要将单值value
 *     转话为Tuple双值类型才能继续计算。
 *
 *     第一个参数createCombiner：将计算的第一个值做结构转换。
 *     第二个参数mergeValue：编写分区内的计算逻辑。
 *     第三个参数mergeCombiner：编写分区间的计算逻辑。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 15:37
 */
public class Spark22_RDDCombineByKeyOperator {
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
                new Tuple2<>("a", 2),
                // 上面一个分区，下面一个分区
                new Tuple2<>("b", 4),
                new Tuple2<>("c", 3),
                new Tuple2<>("a", 3),
                new Tuple2<>("c", 5)
        ), 2);

        // 小练习：求取平均值。
        // 第一个参数：将计算的第一个值做结构转换。
        // 第二个参数：编写分区内的计算逻辑。
        // 第三个参数：编写分区间的计算逻辑。
        JavaPairRDD<String, Tuple2<Integer, Integer>> rdd2 = rdd.combineByKey(
                // 将v转化为元组，方便计数。
                v -> new Tuple2<>(v, 1),
                // 对分区内的值在v基础上进行累加，并计数。
                (tup, nextValue) -> new Tuple2<>(tup._1 + nextValue, tup._2 + 1),
                // 对分区间的数值进行累加，计数进行累加。
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2));

        // 上面得出的是依据key分组，值累加和计数累加的结果，还差一步计算平均值。
        JavaRDD<Tuple2<String, Integer>> avgRdd = rdd2.map(v -> {
            // 总数 / 数量
            int avg = v._2._1 / v._2._2;
            return new Tuple2<>(v._1, avg);
        });

        System.out.println(avgRdd.collect());
    }
}
