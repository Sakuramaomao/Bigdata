package com.example.spark.core.rdd.persist;

/**
 * <pre>
 *     Cache、Persist和CheckPoint区别
 *
 *     1、Cache：其实调用的是Persist的API，只不过将第二个参数选择为了Memory-only。
 *       缓存不可靠、计算失败等原因，需要重新计算，所以Cache并没有切断血缘。
 *     2、Persist：第二个参数可以随意选择。比如可以选择持久化到内存、硬盘或者两者都存。
 *     3、CheckPoint：缓存是不靠谱的，即使不断电，也有可能会丢失数据。所以将数据保存
 *       到可靠的分布式文件存储系统中。因为信赖分布式存储系统，所以检查点也就切断了血缘。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2020/7/27 21:30
 */
public class Spark03_CachePersistAndCheckPoint {
    public static void main(String[] args) {
    }
}
