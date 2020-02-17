package student.gaokun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 让主类继承Configured类，实现Tool接口
 * 实现run()方法
 * 将以前main()方法中的逻辑，放到run()中
 * 在main()中，调用ToolRunner.run()方法，第一个参数是当前对象；第二个参数是输入、输出
 */
public class SmallFiles2SequenceFile extends Configured implements Tool {

   public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new SmallFiles2SequenceFile(), args);
        System.exit(run);
    }


        @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"combine small files to sequencefile");
        job.setJarByClass(SmallFiles2SequenceFile.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        WholeFileInputFormat.addInputPath(job,new Path(args[0]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceFileMapper.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true)?0:1;
    }
}
