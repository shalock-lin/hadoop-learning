package com.linchonghui.serializer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

/**
 * @Author:linchonghui
 * @Date:25/3/2020
 * @Blog: https://github.com/Boomxiakalakaka/flink-learning
 */
public class MySerializer {

    public static Properties initProperties(){
        Properties properties = new Properties();
        //1. 配置kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        //2. 配置序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LogSerializer.class.getName());
        //3. 设置生产者编号
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.lin");

        //4. 设置重试次数(可选) 如果是重试可解决的异常，会通过重试的方式来进行解决
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        return properties;
    }
}


class LogSerializer implements Serializer<Log> {
    private String encoding = "UTF8";

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Log log) {
        if (null == log)
            return null;

        byte [] logId,message;
        try {
            if (null != log.getLogId()){
                logId = log.getLogId().getBytes(encoding);
            } else {
                logId = new byte[0];
            }

            if (null != log.getMessage()){
                message = log.getMessage().getBytes(encoding);
            } else {
                message = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer
                    .allocate(4+4+logId.length+message.length);
            byteBuffer.putInt(logId.length);
            byteBuffer.put(logId);
            byteBuffer.putInt(message.length);
            byteBuffer.put(message);
            return byteBuffer.array();
        } catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}

@Data
class Log {
    private String logId;
    private String message;
}