package ssdut.training.mapreduce.minmaxcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxCountJob {
	public static void main(String[] args) throws Exception {
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		//2.设置MapReduce作业配置信息
		String jobName = "MinMaxCountJob";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MinMaxCountJob.class);			//指定运行时作业类
		job.setJar("export\\MinMaxCountJob.jar");			//指定本地jar包
		job.setMapperClass(MinMaxCountMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);				//设置Mapper输出Key类型
		job.setMapOutputValueClass(MinMaxCountTuple.class);	//设置Mapper输出Value类型
		job.setCombinerClass(MinMaxCountReducer.class);		//指定Combiner类
		job.setReducerClass(MinMaxCountReducer.class);		//指定Reducer类
		job.setOutputKeyClass(Text.class);					//设置Reduce输出Key类型
		job.setOutputValueClass(MinMaxCountTuple.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/minmaxcount/data";			//实验数据目录	
		String outputDir = "/expr/minmaxcount/output";		//实验输出目录
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
