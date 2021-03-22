package com.lzj.example.c_cluster;

/**
 * <pre>
 *  * Zookeeper发现
 *    Ignite与Zookeeper的配置一致性。
 *    zk的tickTime * syncLimit < Ignite的sessionTimeout。
 *
 *  * 客户端节点重连
 *  （1）Ignite节点配置中可以禁用客户端重连。因为重连的客户端id会发生改变，如果业务以来id，会有影响。
 *      关闭重连后，尝试重连的客户端会收到一个IgniteClientDisconnectedException异常。
 *  （2）客户端重连/断开集群连接时，也会在客户端触发两个事件。
 *      a、EVT_CLIENT_NODE_DISCONNECTED
 *      b、EVT_CLIENT_NODE_RECONNECTED
 *      这些事件中可以自定义处理逻辑。
 *
 *  * 基线拓扑
 *    为了控制集群中数据再平衡。
 *  （1）纯内存集群：默认打开基线拓扑。
 *  （2）持久化集群：默认禁用，必须手动更改基线拓扑。
 *      开启持久化的数据区域首次加入集群会处于非激活状态。激活后会自动加入基线拓扑中，重启也会自动激活的。
 *  （3）基线自动调整
 *      ignite.cluster().baselineAutoAdjustEnabled(true);
 *      ignite.cluster().baselineAutoAdjustTimeout(30000);
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/21 15:15
 */
public class ClusterTest {
}
