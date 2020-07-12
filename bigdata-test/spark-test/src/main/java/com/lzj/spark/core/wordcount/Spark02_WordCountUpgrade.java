package com.lzj.spark.core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * {@link Spark01_WordCount} 中是按照切分扁平化->分组->映射统计来做的。
 * 此类中是对上述操作的简化，切分扁平化->map结构转换->reduceByKey分组聚合。
 * 即将分组聚合合并到一步中来完成，效率上会高一些。
 * 1、reduceByKey的作用分组聚合，根据数据中的key进行分组，对value进行统计聚合。
 * 2、Function<v1, v2> 输入一个v1类型的值，返回一个v2类型的值。
 *    Function2<v1, v2, v3> 输入两个值，v1类型和v2类型，计算后返回一个v3类型的值。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/11 17:06
 */
public class Spark02_WordCountUpgrade {
    public static void main(String[] args) {
/*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("myWordCount");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
          业务逻辑
         */
        // 1、读取文件
        JavaRDD<String> fileRdd = sc.textFile("input");
        // 2、扁平化。或者说是切割
        JavaRDD<String> flatMapRdd = fileRdd.flatMap(s -> {
            String[] split = s.split(" ");
            return Arrays.asList(split).iterator();
        });

        // 3、转换数据结构 word -> (word, 1)。便于后面直接按照word分组并聚合获得单词统计值。
        JavaPairRDD<String, Integer> mapToPairRdd = flatMapRdd.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        // 4、reduceByKey的作用是依据key进行分组，对value进行统计聚合。
        JavaPairRDD<String, Integer> wordToCountRdd = mapToPairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        List<Tuple2<String, Integer>> wordCountResult = wordToCountRdd.collect();

        System.out.println(wordCountResult);
    }
}
