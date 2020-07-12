package com.lzj.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 给V增加时间戳。
 *
 * @Author Sakura
 * @Date 2020/7/7 22:15
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();
        String timeVal = System.currentTimeMillis() + "--" + value;
        // 记住是返回一个新的Record。
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), timeVal);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
