package com.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 动态监听服务器节点上下线。客户端
 *
 * @Author Sakura
 * @Date 2020/4/28 10:57
 */
public class DistributeClient {
    private static ZooKeeper zkClient;
    private static String connectStr = "master:2181,slave1:2181,slave2:2181";
    private static int sessionTimeOut = 2000;

    public static void main(String[] args) throws Exception {
        // 1、获取zk连接
        getConnection();

        // 2、注册上线
        regist(args[0]);

        // 3、执行业务逻辑
        doBusiness();
    }

    private static void doBusiness() throws IOException {
        System.in.read();
    }

    /**
     * 使用临时并带有编号的节点。
     *
     * @param hostName
     * @return
     * @throws Exception
     */
    private static String regist(String hostName) throws Exception {
        String registerUrl = zkClient.create("/servers/server", hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostName + " is online");
        return registerUrl;
    }

    private static void getConnection() throws Exception {
        zkClient = new ZooKeeper(connectStr, sessionTimeOut, event -> {

        });
    }
}
