package com.lzj.example.e_datamodel;

/**
 * <pre>
 *     分区备份
 *
 *    * 特性
 *     （1）Ignite默认会保存每个分区的单个副本，不具备高可用性。
 *     （2）分区备份功能是被默认关闭的！
 *     （3）备份分区只能在cacheMode为Partitioned模式下启用。
 *
 *    * 主分区和备份分区
 *      和Kafka的分区备份机制一样。有主分区和备份分区。当主分区宕机时，会触发Leader选取机制（PME：分区映射交换）重新选举主分区。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/24 20:45
 */
public class f_PartitionBackup {
}
