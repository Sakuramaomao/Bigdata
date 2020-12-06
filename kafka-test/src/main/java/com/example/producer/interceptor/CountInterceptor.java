package com.example.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author Sakura
 * @Date 2020/7/7 22:19
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {
    private int successCounter;
    private int failCounter;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 作为第二个拦截器，如果不做处理，一定要把record原封返回。
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            // 发送成功
            successCounter++;
        } else {
            failCounter++;
        }
    }

    @Override
    public void close() {
        System.out.println("发送成功" + successCounter + "条");
        System.out.println("发送失败" + failCounter + "条");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
