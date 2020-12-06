package com.example.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * <pre>
 *
 * </pre>
 *
 * @Author zj.li
 * @Date 2020/8/3 14:45
 **/
public class DescribeTopic {
    public static final String url = "192.168.5.132:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, url);
        AdminClient adminClient = AdminClient.create(props);

        DescribeTopicsResult topicDes = adminClient.describeTopics(Arrays.asList("streamingtest", "streamingtest"));
        KafkaFuture<Map<String, TopicDescription>> kafkaFuture = topicDes.all();
        Map<String, TopicDescription> des = kafkaFuture.get();
        System.out.println(des);

        adminClient.close();
    }
}
