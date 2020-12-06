package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者练习
 *
 * @Author Sakura
 * @Date 2020/7/5 21:32
 */
public class MyConsumer {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // 设置自动提交offset。
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 设置自动提交间隔。
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置消费者组。
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Arrays.asList("bigdata2", "first"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecords.count());
                System.out.println(consumerRecord.partition() + "-" + consumerRecord.offset() + ":"
                        + consumerRecord.key() + "--" + consumerRecord.value());
            }
        }
    }
}
