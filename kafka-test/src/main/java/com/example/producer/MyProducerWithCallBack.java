package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author Sakura
 * @Date 2020/7/5 17:18
 */
public class MyProducerWithCallBack {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        // 指定broker list
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
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

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 9; i++) {
            producer.send(new ProducerRecord<>("bigdata2", "test", "test---" + i),
                    (metadata, exception) -> {
                        if (exception == null) {
                            System.out.println(metadata.partition() + "--" + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });
        }

        producer.close();
        System.in.read();
    }
}
