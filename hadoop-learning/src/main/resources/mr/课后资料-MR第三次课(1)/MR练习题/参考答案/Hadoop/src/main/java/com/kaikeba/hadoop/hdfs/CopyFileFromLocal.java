package com.kaikeba.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * 将本地文件系统的文件通过java-API写入到HDFS文件
 */
public class CopyFileFromLocal {

    /**
     * @param args
     * args0 windows本地磁盘文件C:/test.txt 或虚拟机本地磁盘文件/kkb/install/hadoop-2.6.0-cdh5.14.2/README.txt
     * args1 hdfs上文件hdfs://node01:8020/test.txt
     */
    public static void main(String[] args){
        //本地磁盘路径
        String source = args[0];

        //先确保/data目录存在
        String destination = args[1];//HDFS的路徑

        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(source));
            //HDFS读写的配置文件
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(destination),conf);

            //调用Filesystem的create方法返回的是FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            OutputStream out = fs.create(new Path(destination));

            IOUtils.copyBytes(in, out, 4096, true);
        } catch (FileNotFoundException e) {
            System.out.println("exception");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("exception1");
            e.printStackTrace();
        }

    }
}
