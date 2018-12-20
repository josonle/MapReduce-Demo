package ssdut.training.mapreduce.weblog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//1. 计算网站全天产生的流量
public class FlowCount {

	public static class FlowCountMapper extends Mapper<Object, Text, Text, IntWritable> {		
		private SimpleDateFormat SDFIN = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		private SimpleDateFormat SDFOUT = new SimpleDateFormat("yyyy-MM-dd");
        private Text date = new Text();					//Map输出key
        private IntWritable flow = new IntWritable();	//Map输出value
        
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			String strFlow = strs[strs.length-1];		//获取流量字符串
			String strTime = strs[3].substring(1);		//获取时间字符串
			String strDate = null;						//定义日期字符串
			try {
				strDate = SDFOUT.format(SDFIN.parse(strTime));	//时间格式转成日期格式
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			//利用正则表达式判断strFlow是否是数字
			if ( Pattern.compile("[0-9]+").matcher(strFlow).matches() ) {
				flow.set(Integer.parseInt(strFlow));
				date.set(strDate);
				context.write(date, flow);
			} 			
	    }
	}
  
	public static class FlowCountReducer extends Reducer<Text,IntWritable,Text,LongWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			long sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
			/*
			for (IntWritable val : values) {
				context.write(key, val);
			}
			*/
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
		String jobName = "FlowCount";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(FlowCount.class);				//指定运行时作业类
		job.setJar("export\\FlowCount.jar");			//指定本地jar包
		job.setMapperClass(FlowCountMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(FlowCountReducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/weblog/data";			//实验数据目录	
		String outputDir = "/expr/weblog/output1";		//实验输出目录
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