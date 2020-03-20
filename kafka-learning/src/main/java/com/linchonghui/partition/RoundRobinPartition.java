package com.linchonghui.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * @Author:linchonghui
 * @Date:20/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
// TODO 测试轮询的策略  1.0.1还是随机，高版本已经默认为轮询了
    // 思考一下，如果是按照key来进行分区的，相同key(业务)的消息是有序的，但是如果调大该topic的分区，那么相同业务的数据是否会乱序？
    //如果给定了分区号，则直接发送到分区号里面；如果是有key的，则按key的hash值对分区取模
public class RoundRobinPartition {

    public static void main(String []args){
        //1. 配置属性
        Properties properties = new Properties();
        //1.1 配置kafka集群地址
        properties.put("bootstrap.servers","node01:9092,node02:9092,node03:9092");
        //1.2 配置消息确认机制  分别为0，1，-1
        properties.put("acks","1");
        //1.3 配置重试次数
        properties.put("retries",0);
        //1.4 配置缓冲区大小
        properties.put("buffer.memory", 33554432);
        //1.5 配置批处理数据大小,表示每次写多少数据到topic
        properties.put("batch.size", 16384);
        //1.6 配置可以延长多久发送数据，设置为0表示不等待，一有数据就发送
        properties.put("linger.ms", 0);
        //1.7 配置分区策略
        properties.put("partitioner.class", "com.gec.kafkaclient.MyCustomerPartitons");
        //1.8 指定key和value的序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2. 创建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        Scanner scan = new Scanner(System.in);
        // 判断是否还有输入

        while (scan.hasNextLine()) {
            String str = scan.nextLine();
            producer.send(new ProducerRecord<>("RoundRobinTest",  str));
        }

        producer.close();
    }

}
