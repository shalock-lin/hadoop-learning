package com.kaikeba.hadoop.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class SecondarySort {

	/**
	 *
	 * @param args /salary.txt /secondarysort
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		//configuration.set("mapreduce.job.jar","/home/bruce/project/kkbhdp01/target/com.kaikeba.hadoop-1.0-SNAPSHOT.jar");
		Job job = Job.getInstance(configuration, SecondarySort.class.getSimpleName());
		/**
		 * 若缺少此行代码，集群运行时报错
		 * mapreduce.JobResourceUploader: No job jar file set.  User classes may not be found.
		 * See Job or Job#setJar(String).
		 */
		job.setJarByClass(SecondarySort.class);

		FileSystem fileSystem = FileSystem.get(URI.create(args[1]), configuration);
		//生产中慎用
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setMapperClass(MyMap.class);
		//由于mapper与reducer输出的kv类型分别相同，所以，下两行可以省略
//		job.setMapOutputKeyClass(Person.class);
//		job.setMapOutputValueClass(NullWritable.class);
		
		//设置reduce的个数;默认为1
		//job.setNumReduceTasks(1);

		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(Person.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

	//MyMap的输出key用自定义的Person类型；输出的value为null
	public static class MyMap extends Mapper<LongWritable, Text, Person, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//nancy	22	8000
			String[] fields = value.toString().split("\t");
			String name = fields[0];
			int age = Integer.parseInt(fields[1]);
			int salary = Integer.parseInt(fields[2]);
			//在自定义类中进行比较
			Person person = new Person(name, age, salary);
			//person对象作为输出的key
			context.write(person, NullWritable.get());
		}
	}

	public static class MyReduce extends Reducer<Person, NullWritable, Person, NullWritable> {
		@Override
		protected void reduce(Person key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			//输入的kv对，原样输出
			context.write(key, NullWritable.get());
		}
	}
}
