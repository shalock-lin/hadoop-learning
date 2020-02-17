package com.kaikeba.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * 从HDFS读取文件
 * 打包运行jar包 [bruce@node01 Desktop]$ hadoop jar com.kaikeba.hadoop-1.0-SNAPSHOT.jar  com.kaikeba.hadoop.hdfs.ReadFileFromHDFS
 */
public class ReadFileFromHDFS {

    /**
     * @param args
     * args0 hdfs上文件hdfs://node01:8020/test.txt
     * args1 windows本地磁盘文件C:\test\test01.txt或虚拟机本地磁盘文件
     */
    public static void main(String[] args) {
        readFile(args);
    }

    public static void basicReadFile(String[] args) {
        try {
            //源文件
            String srcFile = args[0];

            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(srcFile),conf);

            FSDataInputStream hdfsInStream = fs.open(new Path(srcFile));

            //本地文件
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(args[1]));

            IOUtils.copyBytes(hdfsInStream, outputStream, 4096, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readFile(String[] args) {
        try {
            //源文件
            String srcFile = args[0];

            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(srcFile),conf);

            //获得文件长度
            FileStatus fileStatus = fs.getFileStatus(new Path(srcFile));
            long len = fileStatus.getLen();

            //获得文件的block locations
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(new Path(srcFile), 0, len);

            for(BlockLocation bl: fileBlockLocations) {
                String[] hosts = bl.getHosts();
                for(String host: hosts) {
                    System.out.println("所在节点：" + host);
                }
                String[] names = bl.getNames();
                for(String name: names) {
                    System.out.println("所在节点ip：" + name);
                }
                String[] storageIds = bl.getStorageIds();
                for(String id: storageIds) {
                    System.out.println("存储ID：" + id);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
