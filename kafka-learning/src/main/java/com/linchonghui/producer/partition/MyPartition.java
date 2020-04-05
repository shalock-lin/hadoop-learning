package com.linchonghui.producer.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Author:linchonghui
 * @Date:21/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
//TODO 自定义分区策略，相同后缀的(后四位),放在同一个分区
    // 疑问：如何确保hash失效？ 如何解决hash冲突？hash都有哪些高效的实现方式
public class MyPartition implements Partitioner {

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
        properties.put("partitioner.class", MyPartition.class.getName());
        //1.8 指定key和value的序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2. 创建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        Scanner scan = new Scanner(System.in);
        // 判断是否还有输入

        while (scan.hasNextLine()) {
            String str = scan.nextLine();
            producer.send(new ProducerRecord<String,String>("MyPartitionTest", str, str));
        }

        producer.close();
    }


    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        String key = (String)o;
        return hash(key.substring(key.length()-4))%numPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    static final int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

}
