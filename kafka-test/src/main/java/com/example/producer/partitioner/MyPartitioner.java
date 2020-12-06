package com.example.producer.partitioner;

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
        return 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
