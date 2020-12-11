package com.example.concurrentconsume;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;

/**
 * <pre>
 *   三分区，单消费者，单线程。   消费者订阅所有的分区数据，有序消费。
 *
 *   三分区，单消费者，多线程。   完全无序消费。
 *
 *   https://club.perfma.com/article/2028934?type=parent&last=2052409
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/12/11 16:24
 **/
public class MyConsumer {
    public static void main(String[] args) {
        Properties prop = new Properties();

        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.132:9092");
        // 关闭自动提交offset。
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置消费者组。
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "lzj_test_group");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Arrays.asList("lzj_test"));


        ExecutorService executor0 = new ThreadPoolExecutor(3, 3, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        ExecutorService executor1 = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        ExecutorService executor2 = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        while (true) {
            ConsumerRecords<String, String> recordsForThisPoll = consumer.poll(1000);
            CountDownLatch countDownLatch = new CountDownLatch(recordsForThisPoll.count());

            Set<TopicPartition> partitions = recordsForThisPoll.partitions();
            partitions.forEach(partition -> {
                // part0需要并行，part1和2是串行。
                int partitionID = partition.partition();
                List<ConsumerRecord<String, String>> recordsInPartition = recordsForThisPoll.records(partition);
                recordsInPartition.forEach(record -> {
                    try {
                        switch (partitionID) {
                            case 0:
                                executor0.execute(() -> logic(countDownLatch, record));
                                break;
                            case 1:
                                executor1.execute(() -> logic(countDownLatch, record));
                                break;
                            case 2:
                                executor2.execute(() -> logic(countDownLatch, record));
                                break;
                            default:
                                System.out.println("未知的partitionNum");
                                break;
                        }
                    } finally {
                    }
                });

            });

            consumer.commitSync();
        }
    }

    private static void logic(CountDownLatch countDownLatch, ConsumerRecord<String, String> record) {
        //    处理一条消息。
        System.out.println("[分区" + record.partition() + "]" +
                " - " + "[offset: " + record.offset() + "]" +
                " - " + "[key: " + record.key() + "]" +
                " - " + "[value: " + record.value() + "]");
        countDownLatch.countDown();
    }
}
