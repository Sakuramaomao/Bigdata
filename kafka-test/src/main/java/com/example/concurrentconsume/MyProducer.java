package com.example.concurrentconsume;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * 生产者发送消息
 *
 * @Author Sakura
 * @Date 2020/1/3 22:42
 */
public class MyProducer {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        // 指定broker list
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.132:9092");
        // ack应答级别
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        // 一次发送的batch大小（单位：byte）。不满大小不发送。默认16KB，即‭16,777,216‬
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "10240");
        // 关键参数。batch不满，此batch在Buffer中从创建到最多等待linger.ms时间，也会被发送。
        prop.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        // 指定RecordAccumulator的大小。用来缓冲batch的buffer pool。
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // 重试次数，默认3。
        prop.put(ProducerConfig.RETRIES_CONFIG, 3);
        // K-V结构消息，分别指定K-V的序列化器。
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 自定义分区器。
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.concurrentconsume.partitioner.MyPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 10; i++) {
            // 注意这里我写的key是变化的，会根据key的hash发送到不同的分区。消息消费顺序不一定，如果想发往一个分区且有序消费，那么key要固定。
            producer.send(new ProducerRecord<>("lzj_test", "pluginlog", "pluginlog--" + i));
            System.out.println("[etlPluginlog] 成功发送第" + i + "条消息");
        }

        for (int i = 0; i < 10; i++) {
            // 注意这里我写的key是变化的，会根据key的hash发送到不同的分区。消息消费顺序不一定，如果想发往一个分区且有序消费，那么key要固定。
            producer.send(new ProducerRecord<>("lzj_test", "dataFlowDetailLog", "dataFlowDetailLog--" + i));
            System.out.println("[dataFlowDetailLog] 成功发送第" + i + "条消息");
        }

        for (int i = 0; i < 10; i++) {
            // 注意这里我写的key是变化的，会根据key的hash发送到不同的分区。消息消费顺序不一定，如果想发往一个分区且有序消费，那么key要固定。
            producer.send(new ProducerRecord<>("lzj_test", "wfRate", "wfRate--" + i));
            System.out.println("[wfRate] 成功发送第" + i + "条消息");
        }

        producer.close();

        // 因为producer是异步发送，main线程不能关闭。
        System.in.read();
    }
}
