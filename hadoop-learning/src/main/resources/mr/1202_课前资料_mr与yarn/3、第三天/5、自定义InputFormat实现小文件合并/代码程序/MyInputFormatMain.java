package com.kkb.mr.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyInputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "mergeSmallFile");
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\4、自定义InputFormat实现小文件合并\\文件数据"));


        job.setMapperClass(MyInputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //没有reduce。但是要设置reduce的输出的k3   value3 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

//将我们的文件输出成为sequenceFile这种格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job,new Path("file:///D:\\开课吧课程资料\\Hadoop&ZooKeeper课件\\最新版本课件\\hadoop与zookeeper课件资料\\3、第三天\\4、自定义InputFormat实现小文件合并\\文件数据\\out_sequence"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MyInputFormatMain(), args);
        System.exit(run);
    }

}
