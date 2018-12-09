package ssdut.training.mapreduce.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineInput {

	public static class NLineInputMapper extends Mapper<LongWritable, Text, Text, IntWritable> {		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context ) 
				throws IOException, InterruptedException {
			System.out.println("value: "+value.toString());
			String[] strs = value.toString().split(" ");
			System.out.println("NLines strs is:"+strs);
			System.out.println("strs[0]"+strs[0]);
	    	Text date = new Text(strs[0]);
			context.write(date, one);
	    }
	}
  
	public static class NLineInputReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
	    }
	}

	public static void main(String[] args) throws Exception {		
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.setInt("mapreduce.input.lineinputformat.linespermap", 1000);	//设置每个Map处理的行数
		
		//2.设置MapReduce作业配置信息
		String jobName = "NLineInput";						//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(NLineInput.class);				//指定运行时作业类
		job.setJar("export\\NLineInput.jar");				//指定本地jar包
		job.setMapperClass(NLineInputMapper.class);			//指定Mapper类
		job.setMapOutputKeyClass(Text.class);				//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);		//设置Mapper输出Value类型
		job.setReducerClass(NLineInputReducer.class);		//指定Reducer类
		job.setOutputKeyClass(Text.class);					//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); 		//设置Reduce输出Value类型
		
		job.setInputFormatClass(NLineInputFormat.class);	//设置输入格式化类
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/nlineinput/data";			//实验数据目录	
		String outputDir = "/expr/nlineinput/output";		//实验输出目录
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