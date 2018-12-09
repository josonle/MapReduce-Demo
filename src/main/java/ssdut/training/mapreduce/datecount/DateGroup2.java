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

public class DateGroup2 {

	public static class DateGroup2Mapper extends Mapper<Object, Text, Text, IntWritable> {		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
	    	String[] strs = value.toString().split(" ");		//按空格分割输入
	    	String date = strs[0];					//获取日期
	    	int id = Integer.parseInt(strs[1]);		//获取序号
			context.write(new Text(date), new IntWritable(id));		
	    }
	}
  
	public static class DateGroup2Reducer extends Reducer<Text,IntWritable,Text,Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();		
			sb.append("[ ");
			for (IntWritable val : values) {	//将value值串联
				sb.append(val.toString()).append(" ");
			}
			sb.append("]");			
			String year = key.toString().substring(0,4);	//取年份
			context.write(new Text(year), new Text(sb.toString()));
	    }
	}

	public static class MyGroup extends WritableComparator {
		public MyGroup() {				//注册比较方法
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String d1 = a.toString();
			String d2 = b.toString();	
			
			if (d1.startsWith("2015"))
				d1 = "2015";
			else if (d1.startsWith("2016"))
				d1 = "2016";
			else
				d1 = "2017";
			
			if (d2.startsWith("2015"))
				d2 = "2015";
			else if (d2.startsWith("2016"))
				d2 = "2016";
			else
				d2 = "2017";
			
			return d1.compareTo(d2);	//将原本KEY(年月日)的比较变成年份的比较
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
		String jobName = "DateGroup2";					//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DateGroup2.class);			//指定运行时作业类
		job.setJar("export\\DateGroup2.jar");			//指定本地jar包
		job.setMapperClass(DateGroup2Mapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(DateGroup2Reducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(Text.class); 			//设置Reduce输出Value类型
		job.setGroupingComparatorClass(MyGroup.class);	//设置自定义分组类
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/data";			//实验数据目录	
		String outputDir = "/expr/datecount/output_group2";	//实验输出目录
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