package gradesAverage;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapReduceTest.wordCount.WordCount;
import mapReduceTest.wordCount.WordCount.IntSumReducer;
import mapReduceTest.wordCount.WordCount.TokenizerMapper;

public class GradesAverage {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text student = new Text();
		private IntWritable grade = new IntWritable();

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			StringTokenizer iTokenizer = new StringTokenizer(value.toString(),"\n");
			System.out.println("key is："+key+",value is: "+value.toString());
//			while (iTokenizer.hasMoreTokens()) {
//				
//			}
			String[] list_strs = value.toString().split(" ");
			// 因为每行只有一个学号和对应成绩，不需要考虑切分多个词
			student.set(list_strs[0]);
			grade.set(Integer.parseInt(list_strs[1]));
			context.write(student, grade);
		}
	}
	
//	public static class gradesAverageCombiner extends Reducer<Text, IntWritable, Text, Text> {
//		private Text gradesSum = new Text();
//
//		public void reduce(Text key, Iterable<IntWritable> values, Context context)
//				throws IOException, InterruptedException {
//			int sum = 0;
//			int grades = 0;
//			for (IntWritable val : values) {
//				sum += 1;
//				grades += val.get();
//			}
//			System.out.println("Combiner---student is:"+key.toString()+",grades is:"+grades+",sum is:"+sum);
//			gradesSum.set(grades+","+sum);
//			System.out.println(gradesSum);
//			context.write(key, gradesSum);
//		}
//	}
	public static class gradesAverageReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		private FloatWritable gradesSum = new FloatWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int grades = 0;
			for (IntWritable val : values) {
				sum += 1;   
				grades += val.get();
			}
			System.out.println("Reduce----student is:"+key.toString()+",grades is:"+grades+",sum is:"+sum);
			gradesSum.set((float)grades/sum);
			context.write(key, gradesSum);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration(); // Hadoop配置类
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true"); // 集群交叉提交
		/*
		 * conf.set("hadoop.job.user", "hadoop"); conf.set("mapreduce.framework.name",
		 * "yarn"); conf.set("mapreduce.jobtracker.address", namenode_ip + ":9001");
		 * conf.set("yarn.resourcemanager.hostname", namenode_ip);
		 * conf.set("yarn.resourcemanager.resource-tracker.address", namenode_ip +
		 * ":8031"); conf.set("yarn.resourcemtanager.address", namenode_ip + ":8032");
		 * conf.set("yarn.resourcemanager.admin.address", namenode_ip + ":8033");
		 * conf.set("yarn.resourcemanager.scheduler.address", namenode_ip + ":8034");
		 * conf.set("mapreduce.jobhistory.address", namenode_ip + ":10020");
		 */

		// 2.设置MapReduce作业配置信息
		String jobName = "GradesAverage"; // 定义作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(GradesAverage.class); // 指定作业类
		job.setJar("export\\GradesAverage.jar"); // 指定本地jar包
		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(gradesAverageCombiner.class); // 指定Combiner类
		job.setReducerClass(gradesAverageReducer.class);
//		输出key-value的类型
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		// 3.设置作业输入和输出路径
		String dataDir = "/expr/studentgrades/grades"; // 实验数据目录
		String outputDir = "/expr/studentgrades/output"; // 实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		// 如果输出目录已存在则删除
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		// 4.运行作业
		System.out.println("Job: " + jobName + " is running...");
		if (job.waitForCompletion(true)) {
			System.out.println("统计 success!");
			System.exit(0);
		} else {
			System.out.println("统计 failed!");
			System.exit(1);
		}
	}
}
