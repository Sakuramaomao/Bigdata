package com.lzj.spark.core.rdd;

/**
 * <pre>
 * mapPartitions
 * 以分区为单位进行计算，和map算子很像。
 * 区别：
 *  1、map算子是一个一个执行，而mapPartitions一个分区一个分区的执行。类似于批处理
 *
 *  2、map方法是全量数据操作，不能丢失数据。而mapPartitions是一次性获取分区的所有数据，
 *      那么可以执行迭代器集合的所有操作，比如过滤、max和min等。
 * </pre>
 */
public class Spark07_RDDMapPartitionOperator {
    public static void main(String[] args) {

    }
}
