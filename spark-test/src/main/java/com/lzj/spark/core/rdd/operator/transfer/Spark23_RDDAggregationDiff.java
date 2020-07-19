package com.lzj.spark.core.rdd.operator.transfer;

/**
 * <pre>
 *     这是个文档，目的是小结一下所学聚合函数的区别。
 *       目前已经学习的聚合算子有四个，分别为：
 *     1、reduceByKey
 *     2、aggregateByKey
 *     3、foldByKey
 *     4、combineByKey
 *       这四个算子的底层逻辑是相同的。最终都会调用同一个底层方法。我们只需要关注三个传参：
 *     1、createCombiner：对第一个value所做的处理。可以改变value的结构。
 *     2、mergeValue：分区内的计算逻辑。
 *     3、mergeCombiner：分区间的计算逻辑。
 *       下面分别说明下4个算子在三个参数中的使用。
 *     1、reduceByKey：算子不会对第一个value做处理，分区内和分区间计算逻辑相同。
 *     2、aggregateByKey：算子会将初始值（zeroValue）和第一个value使用分区内计算规则进行计算。
 *     3、foldByKey：算子会将初始值和第一个value使用分区内计算规则进行计算，并且分区内和分区间计算规则相同。
 *     4、combineByKey：算子的第一个入参就是对value进行结构处理的函数表达式，所以无需初始值。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/19 17:16
 */
public class Spark23_RDDAggregationDiff {
}
