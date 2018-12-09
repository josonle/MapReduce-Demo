package ssdut.training.mapreduce.datecount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DateSort3 {
	
	public static class MyKey implements WritableComparable<MyKey> {
		private String date;
		private int num;
		
		public String getDate() {
			return date;
		}

		public void setDate(String date) {
			this.date = date;
		}

		public int getNum() {
			return num;
		}

		public void setNum(int num) {
			this.num = num;
		}

		public MyKey() {			
		}
		
		public MyKey(String date, int num) {
			this.date = date;
			this.num = num;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(date);
			out.writeInt(num);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			date = in.readUTF();
			num = in.readInt();
		}

		@Override
		public int compareTo(MyKey o) {
			//按date升序，num降序
			if (!date.equals(o.date)) //相等的话，返回true，取反为false
				return date.compareTo(o.date);
			else
				return o.num-num;
		}
	}
	
	public static class DateSort3Mapper extends Mapper<Object, Text, MyKey, NullWritable> {		
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			MyKey myKey = new MyKey(strs[0], Integer.parseInt(strs[1]));
			context.write(myKey, NullWritable.get());	//将自定义的myKey作为Map KEY输出
	    }
	}
  
	public static class DateSort3Reducer extends Reducer<MyKey,NullWritable,Text,IntWritable> {
		public void reduce(MyKey key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {			
			context.write(new Text(key.date), new IntWritable(key.num));
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
		String jobName = "DateSort3";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(DateSort3.class);				//指定运行时作业类
		job.setJar("export\\DateSort3.jar");			//指定本地jar包
		job.setMapperClass(DateSort3Mapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(MyKey.class);			//设置Mapper输出Key类型
		job.setMapOutputValueClass(NullWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(DateSort3Reducer.class);	//指定Reducer类
		job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/datecount/data";			//实验数据目录	
		String outputDir = "/expr/datecount/output_sort3";	//实验输出目录
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