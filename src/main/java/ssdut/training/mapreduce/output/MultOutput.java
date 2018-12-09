package ssdut.training.mapreduce.output;

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
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MultOutput {

	public static class MultOutputMapper extends Mapper<Object, Text, Text, IntWritable> {		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
	    	String[] strs = value.toString().split(" ");	//按空格分割输入
	    	Text date = new Text(strs[0]);		//获取日期
			context.write(date, one);			//将日期和常数1作为Map输出	
	    }
	}
  
	public static class MultOutputReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		//定义MultiOutputs对象
		private MultipleOutputs<Text,IntWritable> mos;
		
		//初始化MultiOutputs对象
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		//关闭MultiOutputs对象
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			//context.write(key, new IntWritable(sum));
			
			//使用MultiOutputs对象替代Context对象输出
			//1. 输出到不同文件（格式、文件名）
			if (key.toString().startsWith("2015"))
				mos.write("f2015", key, new IntWritable(sum));
			else if (key.toString().startsWith("2016"))
				mos.write("f2016", key, new IntWritable(sum));
			else
				mos.write("f2017", key, new IntWritable(sum));
			
			//2. 输出到以年分类的子目录，只需指定输出子目录+文件名，不需要在驱动类中定义文件名
			//mos.write(key, new IntWritable(sum), key.toString().substring(0,4)+"/result");
			
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
		String jobName = "MultOutput";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MultOutput.class);			//指定运行时作业类
		job.setJar("export\\MultOutput.jar");			//指定本地jar包
		job.setMapperClass(MultOutputMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(MultOutputReducer.class);	//指定Reducer类
		//job.setOutputKeyClass(Text.class);			//设置Reduce输出Key类型
		//job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型
		
		//定义多文件输出的文件名、输出格式、键类型、值类型
		MultipleOutputs.addNamedOutput(job, "f2015", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "f2016", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "f2017", MapFileOutputFormat.class, Text.class, IntWritable.class);
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/multoutput/data";			//实验数据目录	
		String outputDir = "/expr/multoutput/output";		//实验输出目录
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