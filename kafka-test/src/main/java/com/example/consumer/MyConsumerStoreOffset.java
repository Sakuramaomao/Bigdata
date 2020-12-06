package com.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Author Sakura
 * @Date 2020/7/6 22:40
 */
public class MyConsumerStoreOffset {
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

        consumer.subscribe(Arrays.asList("bigdata2", "first"), new ConsumerRebalanceListener() {
            // 在Rebalance之前调用，正常提交。
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // fixme 下面的方法有点问题，待修改
                //commitOffset(currentOffset);
            }

            // 在Rebalance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));
                    // 定位到最近提交的offset位置继续消费。
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            Map<TopicPartition, Long> currentOffset = new HashMap<>();
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.partition() + "-" + consumerRecord.offset() + ":"
                        + consumerRecord.key() + "--" + consumerRecord.value());
                currentOffset.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                        consumerRecord.offset());
            }
            commitOffset(currentOffset);
        }
    }

    // 提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
        // 比如，提交到mysql。
    }

    // 获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        // 比如，从mysql中读取offset。
        return 0;
    }
}
