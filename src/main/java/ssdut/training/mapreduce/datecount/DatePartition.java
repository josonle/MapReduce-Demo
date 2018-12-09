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

public class DatePartition {

	public static class DatePartitionMapper extends Mapper<Object, Text, Text, IntWritable> {		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
	    	String[] strs = value.toString().split(" ");
	    	Text date = new Text(strs[0]);
			context.write(date, one);
	    }
	}
  
	public static class DatePartitionReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
			//根据年份对数据进行分区，返回不同分区号
			if (key.toString().startsWith("2015"))
				return 0 % numPartitions;
			else if (key.toString().startsWith("2016"))
				return 1 % numPartitions;
			else
				return 2 % numPartitions;		
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
		String jobName = "DatePartition";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DatePartition.class);				//指定运行时作业类
		job.setJar("export\\DatePartition.jar");			//指定本地jar包
//		Map
		job.setMapperClass(DatePartitionMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);				//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);		//设置Mapper输出Value类型
//		Reduce
		job.setReducerClass(DatePartitionReducer.class);	//指定Reducer类
//		全局
		job.setOutputKeyClass(Text.class);					//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class);			//设置Reduce输出Value类型
//		Partition
		job.setPartitionerClass(YearPartitioner.class);		//自定义分区方法
		job.setNumReduceTasks(10); 	//设置reduce任务的数量,该值传递给Partitioner.getPartition()方法的numPartitions参数
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/data";				//实验数据目录	
		String outputDir = "/expr/datecount/output_partition";	//实验输出目录
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