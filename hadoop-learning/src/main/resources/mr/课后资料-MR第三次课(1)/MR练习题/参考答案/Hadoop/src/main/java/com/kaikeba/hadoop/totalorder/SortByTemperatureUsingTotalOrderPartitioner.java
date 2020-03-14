package com.kaikeba.hadoop.totalorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.net.URI;

/**
 * 使用TotalOrderPartitioner全局排序一个SequenceFile文件的内容；
 * 此文件是SortDataPreprocessor的输出文件；
 * key是IntWritble，气象记录中的温度
 */
public class SortByTemperatureUsingTotalOrderPartitioner{

  /**
   * 两个参数：/ncdc/sfoutput /ncdc/totalorder
   * 第一个参数是SortDataPreprocessor的输出文件
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("<input> <output>");
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, SortByTemperatureUsingTotalOrderPartitioner.class.getSimpleName());
    job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    //输入文件是SequenceFile
    job.setInputFormatClass(SequenceFileInputFormat.class);

    //Hadoop提供的方法来实现全局排序，要求Mapper的输入、输出的key必须保持类型一致
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    //分区器：全局排序分区器
    job.setPartitionerClass(TotalOrderPartitioner.class);

    //分了3个区；且分区i-1中的key小于i分区中所有的键
    job.setNumReduceTasks(3);

    /**
     * 随机采样器从所有的分片中采样
     * 每一个参数：采样率；
     * 第二个参数：总的采样数
     * 第三个参数：采样的最大分片数；
     * 只要numSamples和maxSplitSampled（第二、第三参数）任一条件满足，则停止采样
     *
     * 采样器在客户端运行，所以得限制分片的下载数量，以加速采样器的运行
     */
    InputSampler.Sampler<IntWritable, Text> sampler =
            new InputSampler.RandomSampler<IntWritable, Text>(0.1, 5000, 10);
//    TotalOrderPartitioner.setPartitionFile();
    /**
     * 存储定义分区的键；即整个数据集中温度的大致分布情况；
     * 由TotalOrderPartitioner读取，作为全排序的分区依据，让每个分区中的数据量近似
     */
    InputSampler.writePartitionFile(job, sampler);

    //根据上边的SequenceFile文件（包含键的近似分布情况），创建分区
    String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
    URI partitionUri = new URI(partitionFile);

//    JobConf jobConf = new JobConf();

    //与所有map任务共享此文件，添加到分布式缓存中
    DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
//    job.addCacheFile(partitionUri);

    //方案一：输出的文件RECORD级别，使用BZip2Codec进行压缩
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
    //指定job输出使用的压缩算法
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");

    //方案二
    //SequenceFileOutputFormat.setCompressOutput(job, true);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    //SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
