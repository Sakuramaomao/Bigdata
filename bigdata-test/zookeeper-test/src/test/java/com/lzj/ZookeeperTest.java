package com.lzj;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class ZookeeperTest {
    private String connectStr = "master:2181,slave1:2181,slave2:2181";
    private int sessionTimeOut = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
       zkClient = new ZooKeeper(connectStr, sessionTimeOut, event -> {
           System.out.println("--------start-----------");
           try {
               getDataAndWatch();
           } catch (KeeperException e) {
               e.printStackTrace();
           } catch (InterruptedException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }
           System.out.println("--------end-----------");

       });
    }

    // 创建节点，并写入数据。
    @Test
    public void createTest() throws KeeperException, InterruptedException, IOException {
        String path = zkClient.create("/test", "testDataLzj".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(path);
    }

    // 监控数据变化。
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException, IOException {
        List<String> children = zkClient.getChildren("/", true);
        for(String child : children) {
            System.out.println(child);
        }
        System.in.read();
    }

    @After
    public void destroy() throws InterruptedException {
        //zkClient.close();
    }
}
