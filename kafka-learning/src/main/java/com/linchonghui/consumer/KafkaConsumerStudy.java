package com.linchonghui.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

//TODO 消费者模板
public class KafkaConsumerStudy {

    public static Properties initProperties(){
        Properties properties = new Properties();
        //1 配置kafka集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //2. 配置序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //TODO 以上配置基本是通用的

        //3 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-test-100");
        //4 自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //5 自动提交偏移量的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //6 配置偏移量消费策略(仅在该消费者组在该分区没有提交过消费偏移量的情况才生效)
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //earliest: 无提交的offset时，从头开始消费
            //latest: 无提交的offset时，消费新产生的该分区下的数据
            //none : 只要有一个分区不存在已提交的offset，则抛出异常

        //TODO 以下配置不是必要的
        //7 发现组成员失效的时间，调小可尽快识别到组成员失效避免消费滞后
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,"10000");
        //8 设置两次拉取消息的时间间隔，如果处理消息的时间为两分钟，则该值应大于两分钟
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"10000");
        //9 如果单条消息比较大，需要适当的调大该值
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"1000000");
        //10 设置单次poll最大拉取的消息数，默认的500一般偏小（可能成为消费速度的瓶颈）
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10000");
        //11 设置coordinator提醒其他消费者新成员变动的时间，一般设置的小且必须小于session.timeout.ms
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"1000");
        //12 Socket持久的时间(可能会导致平均处理时间飙升)，默认9分钟会断开，建议设置为-1
        properties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG,"-1");
        //13 设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"sticky");


        return properties;
    }

    public static void main(String []args){
        //1. 配置属性
        Properties properties = initProperties();
        //2. 创建消费者对象开始消费
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //3. 指定消费的topic
        consumer.subscribe(Arrays.asList("test-hello-kafka"));
        //订阅多个主题
        //consumer.subscribe(Arrays.asList("test-1","test-2"));
        // 正则订阅
        //consumer.subscribe(Pattern.compile("test.*"), new NoOpConsumerRebalanceListener());

        try {
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
        } finally {
            consumer.close();
        }

    }
}
