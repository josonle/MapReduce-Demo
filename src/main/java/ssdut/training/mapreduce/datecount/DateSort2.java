package ssdut.training.mapreduce.datecount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DateSort2 {	
	
	public static class DateSort2Mapper extends Mapper<Object, Text, IntWritable, Text> {		
		IntWritable num = new IntWritable();
		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	String[] strs = value.toString().split("\t");	//从DateCount运行结果读取数据，默认是用Tab分割输入
	    	String date = strs[0];							//获取日期
	    	num.set(Integer.parseInt(strs[1]));				//获取次数
	    	context.write(num, new Text(date));				//以次数作为key，日期作为value输出
	    }
	}
  
	public static class DateSort2Reducer extends Reducer<IntWritable,Text,Text,IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
	    }
	}

	// 自定义Key排序算法
	public static class MySort extends WritableComparator {
		public MySort() {
			super(IntWritable.class, true);
		}
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public int compare(WritableComparable a, WritableComparable b) {
			return b.compareTo(a);// 默认升序a比b小返回-1，升序排序；现在a比b小，返回1，降序排序
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
		String jobName = "DateSort2";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DateSort2.class);				//指定作业类
		job.setJar("export\\DateSort2.jar");			//指定本地jar包
//		Map
		job.setMapperClass(DateSort2Mapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(IntWritable.class);	//设置Mapper输出Key类型
		job.setMapOutputValueClass(Text.class);			//设置Mapper输出Value类型
//		Reduce
		job.setReducerClass(DateSort2Reducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class);		//设置Reduce输出Value类型
//		自定义Sort
		job.setSortComparatorClass(MySort.class);		//设置自定义排序类
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/output/part-r-00000";	//实验数据目录	
		String outputDir = "/expr/datecount/output_sort2";				//实验输出目录
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