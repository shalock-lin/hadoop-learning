package com.linchonghui.interceptor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @Author:linchonghui
 * @Date:25/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
//TODO 0.10引入的拦截器功能
    // 可同时创建多个拦截器构成拦截器链，这个链路是有序的，如果其中一个拦截器报错，那么它的下一个拦截器会忽略它的操作
public class Main {

    public static Properties initProperties(){
        Properties properties = new Properties();
        //1. 配置kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //2. 配置序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //3. 设置生产者编号
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.lin");

        //4. 设置重试次数(可选) 如果是重试可解决的异常，会通过重试的方式来进行解决
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return properties;
    }

    public static void main(String []args){

    }
}

class ProducerInterceptorPrefix implements ProducerInterceptor<String,String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix1-"+producerRecord.value();

        return new ProducerRecord<>(producerRecord.topic(),
                    producerRecord.partition(), producerRecord.timestamp(),
                    producerRecord.key(), modifiedValue, producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (null == e) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double)sendSuccess/(sendSuccess + sendFailure);
        System.out.println(String.format("[INFO] 发送成功率为 %s %", successRatio));
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
