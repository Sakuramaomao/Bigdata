package com.lzj.example.d_thinclient;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;

import java.util.Collection;

/**
 * <pre>
 *     集群API。使用集群API可以创建集群组，然后在组中执行各种操作。${@link ClientCluster} 是该操作的入口。
 *
 *     * 获取或修改集群的状态。
 *     * 获取集群所有节点的状态列表。（集群每个节点的监控指标，但不能获取集群整理的指标。）
 *     * 创建集群节点的逻辑组，然后使用Ignite的API在组中进行操作。
 *
 * </pre>
 *
 * @Author Sakura
 * @Date 2021/03/23 22:24
 */
public class ThinClientClusterAPI {
    public static void main(String[] args) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("127.0.0.1:10800");

        IgniteClient client = Ignition.startClient(cfg);

        // 获取或修改集群的状态。
        client.cluster().state(ClusterState.ACTIVE);

        // 获取集群中每个node的状态。
        Collection<ClusterNode> nodes = client.cluster().nodes();

        // 节点逻辑分组。
        // 可以使用集群API的ClientClusterGroup接口来创建集群节点的各种组合。
        ClientClusterGroup clientClusterGroup = client.cluster().forServers().forAttribute("dc", "dc1");
    }
}
