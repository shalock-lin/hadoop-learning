package com.linchonghui.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author:linchonghui
 * @Date:25/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
public class produce {

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

        KafkaProducer<String, String> producer = new KafkaProducer<>(initProperties());

        //TODO 三种发送消息的方式 fire-and-forget、sync、async

        try {
            //2.1 单纯同步发送消息
            producer.send(new ProducerRecord<>("Test","TestString")).get();
            //2.2 同步发送消息并将解析结果
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("Test","TestString"));
            RecordMetadata metadata = future.get();
            System.out.println(metadata.topic()+":"+metadata.offset()+":"+metadata.partition());

            //3.异步发送消息，通过回调方式会更加简洁明了
            //同一个分区的回调函数也是保证有序执行的
            producer.send(new ProducerRecord<>("Test", "TestAsyncMessage"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null != e) {
                        e.printStackTrace();
                    } else {
                        System.out.println(metadata.topic()+":"+metadata.offset()+":"+metadata.partition());
                    }
                }
            });

        } catch (ExecutionException | InterruptedException e){
            System.out.println(e.getMessage());
        }

    }
}
