package com.lzj.spark.core.rdd.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * <pre>
 * 1、Spark读取文件采用的是Hadoop的读取规则
 *      文件切片规则：以字节方式来切片。
 *      数据读取规则：以行为单位来读取。
 * 2、文件到底切成几片？（分区的数量）
 *      文件字节数（10），预计切片数量（minPartitions = 2）
 *      则每个分区存放 10 / 2 = 5byte
 *    所谓的最小分区数，取决于总的字节数是否能整除分区数并且剩余的字节达到一个比率。
 *    这个比率是 （余数 / goalSize） * 100% < 10%。如果高于10%，则会创建出一个新分区。
 *    实际产生的分区数量可能大于最小分区数。
 * 3、分区的数据如何存储？
 *      分区的数据是以行为单位读取的，而不是字节。并且存储还会考虑到数据的offset设置。
 *    下面举一个例子来说明。
 *
 *    （1）假如有共10byte数据如下所示（@@表示回车换行符，占用两个byte）
 *        1@@
 *        2@@
 *        3@@
 *        4
 *    （2）计算分为几个区（设置minPartitions = 4）
 *        10  / 4 = 2byte...2byte 即每个分区存放2byte，多出的2byte大于10%比率，会产生一个分区，所以共计5个分区。
 *    （3）数据的offset
 *        每个分区存放2byte，那么数据的偏移量如下
 *        分区号     偏移量
 *        0   -->  (0, 2)
 *        1   -->  (2, 4)
 *        2   -->  (4, 6)
 *        3   -->  (6, 8)
 *        4   -->  (8, 10)
 *     (4) 数据如何存储
 *        数据是以行的方式读取，但是会考虑（3）的数据偏移量的设置。（偏移量是包含尾的）
 *        行      字节角标
 *        1@@ -->  012
 *        2@@ -->  345
 *        3@@ -->  678
 *        4   -->  9
 *        最终分配的结果
 *        分区号     偏移量       分配的数据
 *         0   -->  (0, 2)  -->   1
 *         1   -->  (2, 4)  -->   2
 *         2   -->  (4, 6)  -->   3
 *         3   -->  (6, 8)  -->
 *         4   -->  (8, 10) -->   4
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/12 18:19
 */
public class Spark04_RDDInFIlePartition {
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

        JavaRDD<String> rdd = sc.textFile("input/w.txt", 4);

        rdd.saveAsTextFile("output");
    }
}
