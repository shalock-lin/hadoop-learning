package com.kkb.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class MapJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //hdfs路径
        URI uri = new URI("hdfs://node01:8020/cache/pdts.txt");
        //本地路径
//        URI uri = new URI("file:///C:/cache/pdts.txt");
        Configuration conf = super.getConf();
        //添加缓存文件
        DistributedCache.addCacheFile(uri,conf);


        //获取job对象
        Job job = Job.getInstance(conf, "mapJoin");
        job.setJarByClass(MapJoinMain.class);

        //读取文件，解析成为key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\11、join操作\\数据\\map端join\\map_join_input"));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        //没有reducer逻辑，不用设置了
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\admin\\Desktop\\高级05\\MR第一次\\11、join操作\\数据\\map端join\\map_join_input\\out"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);
        System.exit(run);
    }

}
