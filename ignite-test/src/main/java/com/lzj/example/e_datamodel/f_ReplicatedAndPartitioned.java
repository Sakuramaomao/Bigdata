package com.lzj.example.e_datamodel;

/**
 * <pre>
 *   分区/复制模式
 *
 *      在创建缓存或者SQL表时，可以指定cacheMode。有Replicated和Partitioned模式。
 *   两种模式设计用于不同的场景。
 *
 *   * 分区模式（Partitioned）
 *      这种模式下，所有分区会在所有服务端节点之间均匀分配。此模式是可扩展性最高的分布式缓存模式。
 *    可以在所有节点上的RAM和磁盘上存储尽可能多的数据。
 *      特点：更新成本很低，读取成本高一些。
 *      建议：当数据集很大并且更新频繁时，Partitioned缓存是理想的选择。
 *
 *   * 复制模式（Replicated）
 *      这种模式下，每个分区的数据都会复制到集群中的每个节点。由于每个节点上都有完整的数据，此缓存模式
 *    提供了最大的数据可用性。
 *      特点：读取成本低，更新成本高。
 *      建议：当数据集较小且更新不频繁时，Replicated缓存非常理想。
 *      如果业务系统确实在80％的时间内都在进行缓存查找，那么应该考虑使用REPLICATED缓存模式。
 *
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/3/24 19:49
 **/
public class f_ReplicatedAndPartitioned {
}
