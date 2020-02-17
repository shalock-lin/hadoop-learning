package com.kkb.demo04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

//要读取分片的数据
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    //要读取的分片
    private FileSplit fileSplit;

    //标记一下分片有没有被读取；默认是false
    private boolean flag = false;

    private Configuration configuration;

    //当前的value值
    private BytesWritable bytesWritable;

    //初始化方法
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit)split;
        configuration = context.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    //RecordReader读取分片时，先判断是否有下一个kv对，如果有，则将key、value读出来
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!flag) {
            long length = fileSplit.getLength();
            byte[] splitContent = new byte[(int) length];
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);

            //split内容写入splitContent
            IOUtils.readFully(inputStream, splitContent,0,(int)length);
            bytesWritable.set(splitContent, 0, (int)length);
            flag = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag ? 1.0f : 0.0f;
    }

    //释放资源
    @Override
    public void close() throws IOException {
    }
}
