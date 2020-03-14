package com.kaikeba.hadoop.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.*;
import java.net.URI;

/**
 *
 * 将本地文件系统的文件通过java-API写入到HDFS文件，并且写入时使用压缩
 */
public class CopyFileFromLocal {

    /**
     *
     * @param args 两个参数 C:\test\01_018分钟.mp4 hdfs://node01:8020/copyFromLocal/01_018分钟.bz2
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws ClassNotFoundException {

        //压缩相关
        //压缩类
        //HDFS读写的配置文件
        Configuration conf = new Configuration();
        BZip2Codec codec = new BZip2Codec();
        codec.setConf(conf);

        String source = args[0]; //linux或windows中的文件路徑,demo存在一定数据

        String destination="hdfs://node01:8020/copyFromLocal/01_018分钟.bz2";//HDFS的路徑

        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(source));

            FileSystem fs = FileSystem.get(URI.create(destination),conf);

            //调用Filesystem的create方法返回的是FSDataOutputStream对象
            //该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
            OutputStream out = fs.create(new Path(destination));
            //对输出流的数据压缩
            CompressionOutputStream compressedOut = codec.createOutputStream(out);

            //流拷贝
            IOUtils.copyBytes(in, compressedOut, 4096, true);
        } catch (FileNotFoundException e) {
            System.out.println("exception");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("exception1");
            e.printStackTrace();
        }
    }
}
