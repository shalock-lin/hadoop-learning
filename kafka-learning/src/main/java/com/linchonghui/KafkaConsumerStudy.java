package com.linchonghui;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

//TODO 自动提交偏移量
public class KafkaConsumerStudy {

    public static void main(String []args){
        //1. 配置属性
        Properties properties = new Properties();
            //1.1 配置kafka集群地址
            properties.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
            //1.2 配置消费者组id
            properties.put("group.id", "consumer-test-2");
            //1.3 自动提交偏移量
            properties.put("enable.auto.commit", "true");
            //1.4 自动提交偏移量的时间间隔
            properties.put("auto.commit.interval.ms","1000");
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
        //4. 开始消费数据
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record: records){
                //4.1 该消息所在的分区
                int partition = record.partition();
                //4.2 该消息所对应的key
                String key = record.key();
                //4.3 该消息对应的偏移量
                long offset = record.offset();
                //4.4 该消息内容本身
                String value = record.value();

                System.out.println("partition:"+partition+"\t key:"+key+"\t offset"+offset+"\t value"+value);
            }
        }
    }
}
