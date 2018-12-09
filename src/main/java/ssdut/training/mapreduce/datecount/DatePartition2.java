package ssdut.training.mapreduce.datecount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatePartition2 {

	public static class DatePartition2Mapper extends Mapper<Object, Text, Text, IntWritable> {		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
	    	String[] strs = value.toString().split(" ");
	    	Text date = new Text(strs[0]);
			context.write(date, one);
	    }
	}
  
	public static class DatePartition2Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
	    }
	}

	public static class YearPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			//根据月份对数据进行分区，返回不同分区号			
			String month = key.toString().substring(5,7);	//substring取从下标5到下标7前一个字符，即下标5-6的字符
			switch (month) {
				case "01": return 1;
				case "02": return 2;
				case "03": return 3;
				case "04": return 4;
				case "05": return 5;
				case "06": return 6;
				case "07": return 7;
				case "08": return 8;
				case "09": return 9;
				case "10": return 10;
				case "11": return 1;
				case "12": return 12;
				default  : return 0;
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
		String jobName = "DatePartition2";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DatePartition2.class);			//指定运行时作业类
		job.setJar("export\\DatePartition2.jar");			//指定本地jar包
		job.setMapperClass(DatePartition2Mapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);				//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);		//设置Mapper输出Value类型
		job.setReducerClass(DatePartition2Reducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);					//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class);			//设置Reduce输出Value类型
		job.setPartitionerClass(YearPartitioner.class);		//自定义分区方法
		job.setNumReduceTasks(3); 	//设置reduce任务的数量,该值传递给Partitioner.getPartition()方法的numPartitions参数
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/data";				//实验数据目录	
		String outputDir = "/expr/datecount/output_partition2";	//实验输出目录
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