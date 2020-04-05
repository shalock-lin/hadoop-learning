package com.linchonghui.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerStudy {

    public static void main(String []args){
        //1. 配置属性
        Properties properties = new Properties();
            //1.1 配置kafka集群地址
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
            //1.2 配置消息确认机制  分别为0，1，-1
            properties.put(ProducerConfig.ACKS_CONFIG,"1");
            //1.3 配置重试次数
            properties.put(ProducerConfig.RETRIES_CONFIG,0);
            //1.4 配置缓冲区大小
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            //1.5 配置批处理数据大小,表示每次写多少数据到topic
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            //1.6 配置可以延长多久发送数据，设置为0表示不等待，一有数据就发送
            properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            //1.7 指定key和value的序列化器
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //2. 创建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i =0 ; i < 100; i++){

            producer.send(new ProducerRecord<>("test-2", Integer.toString(i), "test-2-"+i));

        }
        producer.close();
    }
}
