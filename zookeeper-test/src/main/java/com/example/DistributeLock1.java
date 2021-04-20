package com.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * <pre>
 *  use zk creating a distributed reentrant fair lock.
 *
 *  curator
 * </pre>
 *
 * @Author zj.li
 * @Date 2021/4/19 11:05
 **/
public class DistributeLock1 {
    private static final String zkServerAddress = "slave1:2181";

    public static void main(String[] args) throws Exception {
        CuratorFramework curatorClient = getZkClient();
        final ZooKeeper zkCli = curatorClient.getZookeeperClient().getZooKeeper();
        InterProcessMutex lock = new InterProcessMutex(curatorClient, "/locks/my_lock");

        lock.acquire();
        try {
            System.out.println("---i am in!---");
            Stat exists = zkCli.exists("/leader", false);
            if (exists == null) {
                zkCli.create("/leader", "lzj_test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            final byte[] data = zkCli.getData("/leader", false, null);
            System.out.println("read value:" + new String(data));

            zkCli.setData("/leader", "localhost".getBytes(), 0);

            final byte[] data1 = zkCli.getData("/leader", false, null);
            System.out.println("read value:" + new String(data1));
        } finally {
            lock.release();
        }
        System.out.println("---i am out!---");
    }

    private static CuratorFramework getZkClient() {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        return zkClient;
    }
}
