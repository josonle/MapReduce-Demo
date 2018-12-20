package ssdut.training.mapreduce.weblog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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

//5.1 统计网站每分钟的访问量
// 访问量是每一条记录
public class PVMinMax {

	public static class PVMinMaxMapper extends Mapper<Object, Text, Text, IntWritable> {		
		private SimpleDateFormat SDFIN = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		private SimpleDateFormat SDFOUT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        private Text minute = new Text();		//Map输出key
        private final static IntWritable one = new IntWritable(1);
        
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			String strTime = strs[3].substring(1);		//获取时间字符串
			String strMinute = null;
			try {
				strMinute = SDFOUT.format(SDFIN.parse(strTime));	//时间格式转成日期格式
			} catch (ParseException e) {
				e.printStackTrace();
			}
			minute.set(strMinute);
			context.write(minute, one);
	    }
	}
  
	public static class PVMinMaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		Map<String, Integer> map = new HashMap<String, Integer>();
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

		//2.设置MapReduce作业配置信息
		String jobName = "PVMinMax";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(PVMinMax.class);				//指定运行时作业类
		job.setJar("export\\PVMinMax.jar");				//指定本地jar包
		job.setMapperClass(PVMinMaxMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(PVMinMaxReducer.class);		//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/weblog/data";			//实验数据目录	
		String outputDir = "/expr/weblog/output5_1";		//实验输出目录
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