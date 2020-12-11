package com.example.concurrentconsume.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 生产者自定义分区器。
 *
 * @Author Sakura
 * @Date 2020/7/5 18:10
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // ... 分区业务逻辑。比如按照手机号分区之类的。
        // 这里直接全部发往bigdata2消息通道的2号分区了。
        switch (String.valueOf(key)) {
            case "pluginlog":
                return 0;
            case "dataFlowDetailLog":
                return 1;
            case "wfRate":
                return 2;
            default:
                System.out.println("未知key，默认发送至0号分区。");
                return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
