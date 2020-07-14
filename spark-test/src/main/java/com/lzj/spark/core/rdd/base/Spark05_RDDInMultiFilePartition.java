package com.lzj.spark.core.rdd.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 多个文件的分区。
 *
 * Spark的分区就是Hadoop的规则。
 * Hadoop分区时以文件为单位进行划分的，所以读取数据不能跨文件。
 *
 * 当有多个文件时，每个区中存放的数据大小的计算公式为：
 * 每个区中存放的数据大小 = 文件字节大小总和 / minPartitions
 * 当有余数时，仍然依据10%阈值来判定是否生成一个新分区。
 *
 * 所以这个例子中。两个文件，每个文件6byte，minPartitions = 3，
 *  每个分区存放数据大小 = 6 * 2 / 3 = 4byte。
 *  那么每个文件会有两个分区，一共是4个分区。
 *  数据存储则是按照单文件中行和数据offset来共同决定的。
 *
 * @Author Sakura
 * @Date 2020/7/12 19:18
 */
public class Spark05_RDDInMultiFilePartition {
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

        JavaRDD<String> rdd = sc.textFile("input/xw*.txt", 3);

        rdd.saveAsTextFile("output");
    }
}
