package com.example;

import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

/**
 * 动态监听服务器节点上下线。服务端。
 *
 * @Author Sakura
 * @Date 2020/4/28 10:57
 */
public class DistributeServer {
    private static ZooKeeper zkClient;
    private static String connectStr = "master:2181,slave1:2181,slave2:2181";
    private static int sessionTimeOut = 2000;

    public static void main(String[] args) throws Exception {
        // 1、获取连接
        getConnection();

        // 2、监听DistributeClient的上下线状态
        listening();

        // 3、执行业务逻辑。
        doBusiness();
    }

    private static void doBusiness() throws Exception {
        System.in.read();
    }

    private static void listening() throws Exception {
        List<String> children = zkClient.getChildren("/servers", true);
        List<String> clients = new ArrayList<>();
        for (String child : children) {
            byte[] clientPath = zkClient.getData("/servers/" + child, false, null);
            clients.add(new String(clientPath));
        }
        System.out.println(clients);
    }

    private static void getConnection() throws Exception {
        zkClient = new ZooKeeper(connectStr, sessionTimeOut, event -> {
            try {
                listening();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
