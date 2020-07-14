package com.lzj.spark.core.rdd.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * <pre>
 * 从文件中创建RDD。
 *
 * path可以设定相对路径，如果是IDEA，则相对路径从项目的根开始查找。
 * path路径会根据环境的不同自动发送改变。比如local是本地文件。yarn就是hdfs上文件了。
 *
 * Spark读取文件时,默认采用的跟Hadoop一样的按行读取规则.
 * 支持读取目录下所有文件、单个文件和通配符。
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/12 16:16
 */
public class Spark01_RDDInFile {
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

        // 1、如果path指定的是目录，会读取目录下所有文件。
        //JavaRDD<String> input = sc.textFile("input");
        // 2、如果path指定的是文件，只会读取这个文件。
        //JavaRDD<String> input = sc.textFile("input/Hello1.txt");
        // 3、如果path中写的是通配符，则会读取符合规则的文件。
        JavaRDD<String> input = sc.textFile("input/Hello*.txt");

        List<String> collect = input.collect();
        collect.forEach(System.out::println);

    }
}
