package com.lzj.producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义拦截器
 * <p>
 * 注意：生产者是在不断的调用此类中的onSend和onAcknowledgement方法。
 * <p>
 * 只有在生产者关闭时，才会去调用close方法中的逻辑。
 *
 * @Author Sakura
 * @Date 2020/7/7 21:57
 */
public class MyInterceptor implements ProducerInterceptor<String, String> {
    /**
     * 一些配置。
     *
     * @param configs 配置。
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * 在发送前处理。
     * 比如给消息的value添加时间戳。
     *
     * @param record 当前记录。
     * @return 一个处理过的新记录。需要new一个ProducerRecord。
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return null;
    }

    /**
     * 发送后处理。
     * 比如统计发送成功与失败的消息个数。
     *
     * @param metadata 如果metadata不为null，则发送成功。
     * @param exception 如果exception不为null，则发送失败。
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    /**
     * 当此生产者关闭时，会回调此方法。
     */
    @Override
    public void close() {

    }

}
