package ssdut.training.mapreduce.medianstddev;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianStdDevJob {
	public static void main(String[] args) throws Exception {
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		//2.设置MapReduce作业配置信息
		String jobName = "MedianStdDevJob";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MedianStdDevJob.class);			//指定运行时作业类
		job.setJar("export\\MedianStdDevJob.jar");			//指定本地jar包
		job.setMapperClass(MedianStdDevMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(IntWritable.class);		//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);		//设置Mapper输出Value类型
		job.setReducerClass(MedianStdDevReducer.class);		//指定Reducer类
		job.setOutputKeyClass(IntWritable.class);			//设置Reduce输出Key类型
		job.setOutputValueClass(MedianStdDevTuple.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/medianstddev/data";			//实验数据目录	
		String outputDir = "/expr/medianstddev/output";		//实验输出目录
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