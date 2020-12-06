package com.example.spark.sql.base;

/**
 * <pre>
 *     SparkSQL介绍
 *     1、什么是SparkSQL
 *      SparkSQL是Spark用于处理结构化数据（structured data）的Spark模块。所谓结构化数据，即二维表。
 *     2、Hive和SparkSQL
 *      SparkSQL的前身是Shark，是给熟悉关系型数据库，而不熟悉MR的开发人员准备的。Hive是早期唯一运行在
 *     SQL-On-Hadoop的工具，即将SQL翻译为MR，来操作HDFS上的结构化数据。运算期间产生的大量落盘操作，对
 *     效率影响很大。后来Hive的低效影响了Spark自身发展，才有了现在的SparkSQL。
 *     3、DataFrame和Dataset（详细区别可以看sparkSql的Word文档）
 *      SparkSQL是将SQL翻译为RDD来执行，简化了RDD的开发。
 *      在SQL和RDD之间，有两个介质，一个是DataFrame，另一个是Dataset。
 *        （1）RDD：没有数据类型、结构的概念，只操作数据。
 *        （2）DataFrame：为了能使用SQL，将RDD做了一层封装（有结构化信息）。类似于传统数据库中的二维表格。
 *              数据不仅仅是数据，还有了结构。DataFrame每一行的类型固定为Row，每一列的值没法直接访问，只有
 *              通过解析才能获取各个字段的值。
 *        （3）Dataset：底层封装的也是RDD。强类型。既有结构，又有类型。是DataFrame的一个扩展。
 *     4、DataSet<Row>和Dataset<T>区别？
 *      * DataFrame和Dataset有着完全相同的成员函数，区别只是每一行的数据类型不同。
 *        DataFrame其实就是Dataset的一个特例。源码 type DataFrame = Dataset[Row]
 *      * DataFrame也可以叫做Dataset[Row]，每一行的类型是Row，不解析，每一行究竟有哪些字段，每个字段有什么
 *        类型都无从得知，只能用getAs或者模式匹配拿出特定字段。而Dataset中，当定义了样例类时，可以很自由的获取
 *        每一行的信息。
 *     5、既然是包装，那性能呢？
 *          虽然增加了结构、类型的包装，但是Spark提供了查询优化器，在内部选择最优计算路线。所以性能上会比直接
 *      用RDD实现的逻辑高。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/30 22:30
 */
public class Spark01_Introduction {
}
