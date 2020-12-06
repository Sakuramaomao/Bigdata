package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author Sakura
 * @Date 2020/7/7 22:23
 */
public class MyProducerWithInterceptor {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        // 指定broker list
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // ack应答级别
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        // K-V结构消息，分别指定K-V的序列化器。
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加自定义拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.lzj.producer.interceptor.TimeInterceptor");
        interceptors.add("com.lzj.producer.interceptor.CountInterceptor");
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i < 10; i++) {
            // 注意这里我写的key是变化的，会根据key的hash发送到不同的分区。消息消费顺序不一定，如果想发往一个分区且有序消费，那么key要固定。
            producer.send(new ProducerRecord<>("bigdata2", "test", "test-" + i));
            System.out.println("成功发送第" + i + "条消息");
        }

        //producer.close();

        // 因为producer是异步发送，main线程不能关闭。
        System.in.read();
    }
}
