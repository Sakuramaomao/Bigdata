package com.lzj.spark.core.rdd.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 *     Sample算子：
 *       从数据集中进行随机采样的算子。
 *     1、三个参数
 *      （1）、第一个参数  boolean withReplacement 采样后是否放回，可以重复抽取。
 *         a、true：抽取后放回。
 *         b、false：抽取后不放回。
 *      （2）、第二个参数  double fraction  根据第一个参数的设定不同，有不同的含义。
 *         a、抽取后放回：表示每个元素被抽取的几率。
 *         b、抽取后不放回：表示每个元素被抽取的次数。 （FIXME 这里有疑问，Java中的貌似和Scala中的不一致。Java只能取[0, 1]）
 *      （3）、第三个参数  long seed 随机数种子
 *         随机数种子：让随机数不随机。即相同的seed，采样规律一致的，随机数一致，最终的采样结果一致。
 *
 *     2、采样有什么作用吗？
 *        在实际开发中，往往会出现数据倾斜的情况，那么可以从数据倾斜的分区中抽取数据，查看数据的规划，
 *      如果抽取的数据中重复的很多，那么有一定的理由相信是这类数据过多导致的数据倾斜问题。分析后，可以
 *      进行改善处理，让数据更加均匀。
 *
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/14 23:00
 */
public class Spark12_RDDSampleOperator {
    public static void main(String[] args) {
        /*
          创建配置对象。
         */
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("sample-rdd");

        /*
          使用配置对象创建上下文环境。
         */
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // 采样后放回。每个元素被抽取的概率是0.5.
        JavaRDD<Integer> sample = rdd.sample(true, 0.5);
        // 采样后不反悔。每个元素被抽取到的次数是2.
        JavaRDD<Integer> sample2 = rdd.sample(false, 0.8);
        // 采样后返回。每个元素被抽取的概率是0.8。并且采样的seed是5.
        JavaRDD<Integer> sample3 = rdd.sample(false, 0.8, 5);
        // 采样后返回。每个元素被抽取的概率是0.8.并且采用和上面相同的seed。则结果会保持一致。
        JavaRDD<Integer> sample4 = rdd.sample(false, 0.8, 5);

        List<Integer> collect = sample.collect();
        List<Integer> collect2 = sample2.collect();
        List<Integer> collect3 = sample3.collect();
        List<Integer> collect4 = sample4.collect();

        System.out.println(collect);
        System.out.println(collect2);
        System.out.println(collect3);
        System.out.println(collect4);
    }
}
