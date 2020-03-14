package student.liuxin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyOwnOutputFormatMain extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, MyOwnOutputFormatMain.class.getSimpleName());
        job.setJarByClass(MyOwnOutputFormatMain.class);
        System.out.print("设置的输出目录:"+args[1]);
        //HadoopUtils.deletePathFiles(conf,args[1]);
        //默认项，可以省略或者写出也可以
        //job.setInputFormatClass(TextInputFormat.class);
        //设置输入文件
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(NullWritable.class);

        //FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置自定义的输出类
        job.setOutputFormatClass(MyOutPutFormat.class);
        //设置一个输出目录，这个目录会输出一个success的成功标志的文件
        MyOutPutFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //默认项，即默认有一个reduce任务，所以可以省略
//        job.setNumReduceTasks(1);
//        //Reducer将输入的键值对原样输出
//        job.setReducerClass(Reducer.class);

        boolean b = job.waitForCompletion(true);
        return b ? 0: 1;
    }

    /**
     *
     * Mapper输出的key、value类型
     * 文件每行的内容作为输出的key，对应Text类型
     * 输出的value为null，对应NullWritable
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.print("map操作:"+value.toString());
            //把当前行内容作为key输出；value为null
            context.write(value, NullWritable.get());
        }
    }

    /**
     *
     * @param args /ordercomment.csv /ofo 或windows C:\test\salary.txt C:\test\output
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(configuration, new MyOwnOutputFormatMain(), args);
    }
}
