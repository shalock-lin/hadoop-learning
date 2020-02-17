package com.kkb.demo03;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//泛型指的是输出的k，v类型
public class MyOutPutFormat extends FileOutputFormat<Text,NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path goodComment = new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\10、自定义outputFormat\\数据\\good\\1.txt");
        Path badComment = new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\10、自定义outputFormat\\数据\\bad\\1.txt");
        FSDataOutputStream goodOutputStream = fs.create(goodComment);
        FSDataOutputStream badOutputStream = fs.create(badComment);
        return new MyRecordWriter(goodOutputStream,badOutputStream);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable>{

        FSDataOutputStream goodStream = null;
        FSDataOutputStream badStream = null;

        public MyRecordWriter(FSDataOutputStream goodStream, FSDataOutputStream badStream) {
            this.goodStream = goodStream;
            this.badStream = badStream;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if (key.toString().split("\t")[9].equals("0")){
                goodStream.write(key.toString().getBytes());
                goodStream.write("\r\n".getBytes());
            }else{
                badStream.write(key.toString().getBytes());
                badStream.write("\r\n".getBytes());
            }
        }

        //释放资源
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(badStream !=null){
                badStream.close();
            }
            if(goodStream !=null){
                goodStream.close();
            }
        }
    }
}

