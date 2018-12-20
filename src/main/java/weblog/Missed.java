package ssdut.training.mapreduce.weblog;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//2. 将所有状态为404的记录输出到文件：missed
public class Missed {

	public static class MissedMapper extends Mapper<Object, Text, Text, NullWritable> {
		private Text k = new Text();	//Map输出key
		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			String status = strs[strs.length-2];	//获取状态码
			if (status.equals("404")) {
				//context.write(value, NullWritable.get());
				String reqResource = strs[6];		//获取被请求的资源
				int index = reqResource.indexOf("?");
				if ( index > 0 ) {
					reqResource = reqResource.substring(0, index);	//截取问号前的请求资源名称（去掉请求参数）
				}
				k.set(reqResource);
				context.write(k, NullWritable.get());
			}
	    }
	}
  
	public static class MissedReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		//定义MultiOutputs对象
		private MultipleOutputs<Text,NullWritable> mos;
		
		//初始化MultiOutputs对象
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}
		
		//关闭MultiOutputs对象
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
		public void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			mos.write("missed", key, NullWritable.get());
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
		String jobName = "Missed";						//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(Missed.class);				//指定运行时作业类
		job.setJar("export\\Missed.jar");				//指定本地jar包
		job.setMapperClass(MissedMapper.class);			//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(NullWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(MissedReducer.class);		//指定Reducer类		
		//定义多文件输出的文件名、输出格式、键类型、值类型
		MultipleOutputs.addNamedOutput(job, "missed", TextOutputFormat.class, Text.class, NullWritable.class);
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/weblog/data";			//实验数据目录	
		String outputDir = "/expr/weblog/output2";		//实验输出目录
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