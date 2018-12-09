package mapreduceProgram;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DateSortAsc {

	public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
		private IntWritable num = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			num.set(Integer.parseInt(strs[1]));
			// 将次数作为key进行升序排序
			context.write(num, new Text(strs[0]));
			System.out.println(num.get()+","+strs[0]);
		}
	}

	public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				// 排序后再次颠倒k-v，将日期作为key
				System.out.println(value.toString()+":"+key.get());
				context.write(value, key);
			}
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
		String jobName = "DateSortAsc"; // 定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DateSortAsc.class); // 指定作业类
		job.setJar("export\\DateSortAsc.jar"); // 指定本地jar包
		
		job.setMapperClass(SortMapper.class); // 指定Mapper类
		job.setMapOutputKeyClass(IntWritable.class); // 设置Mapper输出Key类型
		job.setMapOutputValueClass(Text.class); // 设置Mapper输出Value类型
		
		job.setReducerClass(SortReducer.class); // 指定Reducer类
		job.setOutputKeyClass(Text.class); // 设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); // 设置Reduce输出Value类型

		// 3.设置作业输入和输出路径
		String dataDir = "/workspace/dateSort/data"; // 实验数据目录
		String outputDir = "/workspace/dateSort/output"; // 实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
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
