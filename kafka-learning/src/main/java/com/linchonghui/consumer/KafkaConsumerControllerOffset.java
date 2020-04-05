package com.linchonghui.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerControllerOffset {

    public static void main(String []args){
        //1. 配置属性
        Properties properties = new Properties();
            //1.1 配置kafka集群地址
            properties.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
            //1.2 配置消费者组id
            properties.put("group.id", "consumer-test8");
            //1.3 关闭自动提交，改为手动提交
            properties.put("enable.auto.commit", "false");
            //1.5 配置偏移量提交策略
            //earliest: 无提交的offset时，从头开始消费
            //latest: 无提交的offset时，消费新产生的该分区下的数据
            //none : 只要有一个分区不存在已提交的offset，则抛出异常
            properties.put("auto.offset.reset", "earliest");
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2. 创建消费者对象开始消费
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //3. 指定消费的topic
        consumer.subscribe(Arrays.asList("test"));

        // 定义一个数字，表示消息达到多少条后提交偏移量(在web项目里面可以等到成功处理完一条消息后再提交该消息)
        final int minBatchSize = 20;

        //4. 定义一个数组，缓冲一批数据
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(1);for (ConsumerRecord<String, String> record: records){
                buffer.add(record);
            }

            if (buffer.size() >= minBatchSize){
                //insertIntoDb(buffer);  拿到数据之后，进行消费
                System.out.println("缓冲区的数据条数："+buffer.size());
                System.out.println("我已经处理完和一批数据了....");
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
