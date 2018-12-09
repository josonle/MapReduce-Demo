package InputOutputFormatTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ssdut.training.mapreduce.output.MultOutput;
import ssdut.training.mapreduce.output.MultOutput.MultOutputMapper;
import ssdut.training.mapreduce.output.MultOutput.MultOutputReducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class MultiInOutput {
	public static class TxtFileMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			Text date = new Text(strs[0]);
			context.write(date, one);
		}
	}

	public static class CsvFileMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(";");//定义csv文件时用了；做分隔符
			context.write(new Text(strs[0]), one);
		}
	}

	public static class MultOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// 通过 MultipleOutputs 类控制输出的文件名和输出路径
		// 定义MultipleOutput对象
		private MultipleOutputs<Text, IntWritable> mos;

		// 覆写MultipleOutput对象的setup()初始化和cleanup()关闭mos对象方法
		protected void setup(Context context) {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// 使用MultiOutputs对象替代Context对象输出
			// 1. 输出到不同文件（格式、文件名）
			if (key.toString().startsWith("2015"))
				mos.write("f2015", key, new IntWritable(sum));
			else if (key.toString().startsWith("2016"))
				mos.write("f2016", key, new IntWritable(sum));
			else
				mos.write("f2017", key, new IntWritable(sum));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		// 2.设置MapReduce作业配置信息
		String jobName = "MultInputOutput"; // 作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MultiInOutput.class); // 指定运行时作业类
		job.setJar("export\\MultiInOutput.jar"); // 指定本地jar包
		job.setMapOutputKeyClass(Text.class); // 设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class); // 设置Mapper输出Value类型
		job.setReducerClass(MultOutputReducer.class); // 指定Reducer类
		// job.setOutputKeyClass(Text.class); //设置Reduce输出Key类型
		// job.setOutputValueClass(IntWritable.class); //设置Reduce输出Value类型
		
		// 3.指定作业多输入路径，及Map所使用的类
		MultipleInputs.addInputPath(job, new Path(hdfs+"/expr/multiinoutput/data/txt"), TextInputFormat.class, TxtFileMapper.class);
		MultipleInputs.addInputPath(job, new Path(hdfs+"/expr/multiinoutput/data/csv"), TextInputFormat.class, CsvFileMapper.class);
		
		// 定义多文件输出的文件名、输出格式、Reduce输出键类型，值类型
		MultipleOutputs.addNamedOutput(job, "f2015", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "f2016", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "f2017", MapFileOutputFormat.class, Text.class, IntWritable.class);

		// 设置作业输出路径
		String outputDir = "/expr/multiinoutput/output"; // 实验输出目录
		Path outPath = new Path(hdfs + outputDir);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		// 4.运行作业
		System.out.println("Job: " + jobName + " is running...");
		if (job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}

}
