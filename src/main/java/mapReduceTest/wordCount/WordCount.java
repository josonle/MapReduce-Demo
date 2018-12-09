package mapReduceTest.wordCount;

import java.io.IOException;
import java.util.StringTokenizer;
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

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
	    	StringTokenizer itr = new StringTokenizer(value.toString());
	    	while (itr.hasMoreTokens()) {
	    		word.set(itr.nextToken());
	    		context.write(word, one);
	    	}
	    }
	}
  
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
	    }
	}

	public static void main(String[] args) throws Exception {		
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();	//Hadoop配置类
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");	//集群交叉提交
/*		conf.set("hadoop.job.user", "hadoop");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.jobtracker.address", namenode_ip + ":9001");
		conf.set("yarn.resourcemanager.hostname", namenode_ip);	
		conf.set("yarn.resourcemanager.resource-tracker.address", namenode_ip + ":8031");
		conf.set("yarn.resourcemtanager.address", namenode_ip + ":8032");
		conf.set("yarn.resourcemanager.admin.address", namenode_ip + ":8033");
		conf.set("yarn.resourcemanager.scheduler.address", namenode_ip + ":8034");
		conf.set("mapreduce.jobhistory.address", namenode_ip + ":10020"); */
		
		//2.设置MapReduce作业配置信息
		String jobName = "WordCount";					//定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(WordCount.class);			//指定作业类
		job.setJar("export\\WordCount.jar");			//指定本地jar包
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);		//指定Combiner类
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/wordcount/data";		//实验数据目录	
		String outputDir = "/expr/wordcount/output";	//实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		//如果输出目录已存在则删除
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