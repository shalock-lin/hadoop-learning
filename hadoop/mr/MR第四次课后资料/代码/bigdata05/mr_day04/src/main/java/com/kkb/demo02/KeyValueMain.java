package com.kkb.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class KeyValueMain  {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //翻阅 KeyValueLineRecordReader 的源码，发现切割参数的配置
        configuration.set("key.value.separator.in.input.line","@zolen@");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(KeyValueMain.class);

        //第一步：读取文件，解析成为key，value对
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job,new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\2、keyValueTextInputFormat\\数据"));

        //第二步：设置mapper类
        job.setMapperClass(KeyValueMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //第三步到第六步 分区，排序，规约，分组

        //第七步：设置reducer类
        job.setReducerClass(KeyValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //第八步：输出数据
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\2、keyValueTextInputFormat\\数据\\out"));

        boolean b = job.waitForCompletion(true);
        System.exit(0);
    }

}
