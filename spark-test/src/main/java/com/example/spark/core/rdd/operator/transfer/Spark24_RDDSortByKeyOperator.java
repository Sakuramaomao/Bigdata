package com.example.spark.core.rdd.operator.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <pre>
 *     sortByKey算子。
 *        按照key进行排序。默认是按照数字或者字符集升序排列。
 *
 *     sortBy和sortByKey区别：
 *        sortBy可以传入自定义的排序规则，比如不按照key，而是按照value之类的排序，sortByKey则是默认按照key来排序。
 *     其实sortBy底层还是使用的sortByKey算子。
 *
 *     小插曲：sortByKey可以传入一个对象，需要对象实现Comparator接口，实现比较方法即可。这也可以称为key自定义排序规则。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 17:29
 */
public class Spark24_RDDSortByKeyOperator {
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
                new Tuple2<>("b", 3)
        ), 2);

        // 小练习：按照key降序
        JavaPairRDD<String, Integer> sort = rdd.sortByKey(false);

        System.out.println(sort.collect());
    }
}
