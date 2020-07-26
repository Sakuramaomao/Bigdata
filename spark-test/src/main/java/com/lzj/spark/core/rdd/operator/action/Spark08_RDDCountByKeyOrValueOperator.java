package com.lzj.spark.core.rdd.operator.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * <pre>
 *     CountByKey 行动算子
 *        统计相同key元素的个数。有种做wordCount的感觉。
 *
 *     ---------------------------
 *     CountByValue 行动算子
 *        统计相同value元素的个数。也有种做wordCount的感觉。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/26 10:12
 */
public class Spark08_RDDCountByKeyOrValueOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("countByKeyOrValue-action");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // countByKey输入数据
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 1)
        ));

        // countByValue输入数据
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "a", "a", "b"));

        Map<String, Long> map = rdd.countByKey();
        Map<String, Long> map2 = rdd2.countByValue();

        // 结果：{a=3, b=1}
        System.out.println(map);
        // 结果：{a=3, b=1}
        System.out.println(map2);
    }
}
