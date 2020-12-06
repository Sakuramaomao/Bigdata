package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author Sakura
 * @Date 2020/7/5 23:01
 */
public class MyConsumerResetOffset {
    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // 设置自动提交offset。
        //prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 关闭自动提交
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 设置自动提交间隔。
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 更改消费者组。
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group1");
        // 设置自动重置offset
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Arrays.asList("bigdata2", "first"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.partition() + "-" + consumerRecord.offset() + ":"
                        + consumerRecord.key() + "--" + consumerRecord.value());
            }
            // 同步提交offset
            consumer.commitSync();
            // 异步提交offset
            consumer.commitAsync();
        }
    }
}
