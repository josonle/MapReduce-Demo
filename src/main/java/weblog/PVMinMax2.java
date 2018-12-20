package ssdut.training.mapreduce.weblog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//5.2 计算网站每分钟访问量的峰值（最大、最小值）
public class PVMinMax2 {

	public static class PVMinMax2Mapper extends Mapper<Object, Text, Text, Text> {		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			// 传入数据类似2014-12-12 18:06 	1234，前面通过空格分开，后面是制表符分隔
			String[] strs = value.toString().split(" ");
			// key是2014-12-12这样的时间
			context.write(new Text(strs[0]), new Text(strs[1]));
	    }
	}
  
	public static class PVMinMax2Reducer extends Reducer<Text, Text, Text, Text> {
		// Map<String, Integer> map = new HashMap<String, Integer>();
		int maxVisit = 0;					//默认最大值设为0
		int minVisit = Integer.MAX_VALUE;	//默认最小值设为最大整数
		String maxMinute = null;// 最大访问量的所在时间
		String minMinute = null;
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			for (Text val : values) {
				String[] strs = val.toString().split("\t");
				String minute = strs[0];				//minute:访问时间，如：17:38
				int visit = Integer.parseInt(strs[1]);	//visit:访问次数,如：813
				if (visit > maxVisit) {
					maxVisit = visit;
					maxMinute = minute;
				}					
				if (visit < minVisit) {
					minVisit = visit;
					minMinute = minute;
				}
			}
			
			String strMaxTime = key.toString() + " " + maxMinute;	//将日期和分钟合并
			String strMinTime = key.toString() + " " + minMinute;
			context.write(new Text(strMaxTime), new Text(String.valueOf(maxVisit)));
			context.write(new Text(strMinTime), new Text(String.valueOf(minVisit)));
			
			/*
			*或者这样写
			String value = maxMinute + " " + maxVisit + "\t" + minMinute + " " + minVisit;
			context.write(key, new Text(value));
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
		String jobName = "PVMinMax2";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(PVMinMax2.class);				//指定运行时作业类
		job.setJar("export\\PVMinMax2.jar");			//指定本地jar包
		job.setMapperClass(PVMinMax2Mapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(Text.class);			//设置Mapper输出Value类型
		job.setReducerClass(PVMinMax2Reducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(Text.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/weblog/output5_1";			//实验数据目录	
		String outputDir = "/expr/weblog/output5_2";		//实验输出目录
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