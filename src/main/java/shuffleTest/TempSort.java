package shuffleTest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TempSort {
	/*
	 * 按年输出（分区），每个文件包含每月的最高温度
	 */
	public static class TempSortMapper extends Mapper<Object, Text, Text, IntWritable> {
		IntWritable temp = new IntWritable();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strings =value.toString().split(" ");
			String date = strings[0].substring(0, 7);
			temp.set(Integer.parseInt(strings[2].substring(0, strings[2].length()-1)));
			context.write(new Text(date), temp);
		}
	}
	
	public static class TempSortReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
//			气温降序排序，区第一个
//			IntWritable temp = values.iterator().next();
//			System.out.println("气温："+temp);
//			context.write(key, temp);
			
			int  maxTemp = Integer.MIN_VALUE;
			for(IntWritable value:values) {
				System.out.println("年："+key+", 气温："+value);
				if (value.get()>maxTemp) {
					maxTemp = value.get();
				}
			}
			System.out.println("Date:"+key+", MaxTemp:"+maxTemp);
			context.write(key, new IntWritable(maxTemp));
		}
	}
	
	public static class YearPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			//根据年份对数据进行分区，返回不同分区号
			if (key.toString().startsWith("1949"))
				return 0 % numPartitions;
			else if (key.toString().startsWith("1950"))
				return 1 % numPartitions;
			else
				return 2 % numPartitions;		
		}
	}
	
//	public static class MySort extends WritableComparator {
//		 public MySort() {
//			 super(IntWritable.class,true);
//		 }
//		 
//		 @SuppressWarnings({"rawtypes","unchecked"})
//		 public int compare(WritableComparable a,WritableComparable b) {
//			 return b.compareTo(a);
//		 }
//	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String hdfs = "hdfs://192.168.17.10:9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		// 设置作业配置信息
		String jobName = "TempSort";
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(TempSort.class);
		job.setJar("export\\TempSort.jar");
		// Map
		job.setMapperClass(TempSortMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// Reduce
		job.setReducerClass(TempSortReducer.class);
		// 全局
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// Sort
//		job.setSortComparatorClass(MySort.class);
		// Partition
		job.setPartitionerClass(YearPartitioner.class);
		job.setNumReduceTasks(3);
		//3.设置作业输入和输出路径
		String dataDir = "/expr/test/data";	//实验数据目录	
		String outputDir = "/expr/test/output";				//实验输出目录
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
