package ssdut.training.mapreduce.datecount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DateFilter {

	public static class DateFilterMapper extends Mapper<Object, Text, Text, NullWritable> {		
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	String[] strs = value.toString().split(" ");
	    	Text date = new Text(strs[0]);
			context.write(date, NullWritable.get());
	    }
	}
/*
	public static class DateFilterReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
	    }
	}
*/
	public static void main(String[] args) throws Exception {		
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		//2.设置MapReduce作业配置信息
		String jobName = "DateFilter";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);		
		job.setJarByClass(DateFilter.class);			//指定运行时作业类
		job.setJar("export\\DateFilter.jar");			//指定本地jar包
		job.setMapperClass(DateFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(NullWritable.class);	//设置Mapper输出Value类型
		//job.setReducerClass(DateCountReducer.class);	//不需要设置Reducer类
		//job.setOutputKeyClass(Text.class);			//设置Reduce输出键类型
		//job.setOutputValueClass(NullWritable.class);	//设置Reduce输出值类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/data";			//实验数据目录	
		String outputDir = "/expr/datecount/output_filter";	//实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		//4.运行作业
		System.out.println("Job: " + jobName + " is running...");
		if(job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}

}