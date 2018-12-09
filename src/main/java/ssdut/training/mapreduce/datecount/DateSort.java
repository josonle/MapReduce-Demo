package ssdut.training.mapreduce.datecount;

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

public class DateSort {	
	public static class DateSortMapper extends Mapper<Object, Text, IntWritable, Text> {	//key-value类型不同于以往的	
		IntWritable num = new IntWritable();
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	String[] strs = value.toString().split("\t");	//从DateCount运行结果读取数据，默认是用Tab分割输入
	    	String date = strs[0];					//获取日期
	    	num.set(Integer.parseInt(strs[1]));		//获取次数
	    	context.write(num, new Text(date));		//以次数作为key，日期作为value输出;利用shuffle自动对key升序排序的特性
	    }
	}
  
	public static class DateSortReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);//Map阶段将日期和次数反过来以实现排序，Reduce这里再次翻转key-value
			}
	    }
	}

	public static void main(String[] args) throws Exception {		
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		//2.设置MapReduce作业配置信息
		String jobName = "DateSort";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DateSort.class);				//指定作业类
		job.setJar("export\\DateSort.jar");				//指定本地jar包
		job.setMapperClass(DateSortMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(IntWritable.class);	//设置Mapper输出Key类型
		job.setMapOutputValueClass(Text.class);			//设置Mapper输出Value类型
		job.setReducerClass(DateSortReducer.class);		//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class);		//设置Reduce输出Value类型
				
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/output/part-r-00000";	//实验数据目录	
		String outputDir = "/expr/datecount/output_sort";				//实验输出目录
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