package mergeMultipleFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import InputOutputFormatTest.MultiInOutput;

public class MergeJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		// 2.设置MapReduce作业配置信息
		String jobName = "MergeMultipleFiles"; // 作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(MultiInOutput.class); // 指定运行时作业类
		job.setJar("export\\MergeMultipleFiles.jar"); // 指定本地jar包
		job.setMapOutputKeyClass(Text.class); // 设置Mapper输出Key类型
		job.setMapOutputValueClass(BytesWritable.class); // 设置Mapper输出Value类型
		job.setMapperClass(MergeMapper.class);
		// 输入数据格式
		job.setInputFormatClass(MyInputFormat.class);
		// 以文件格式输出，使用序列化文件输出类
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 设置作业输出路径
		String inputDir = "/workspace/mergeFiles/data";
		String outputDir = "/workspace/mergeFiles/output"; // 输出目录
		Path outPath = new Path(hdfs + outputDir);
		Path inputPath = new Path(hdfs+inputDir);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		// 运行作业
		System.out.println("Job: " + jobName + " is running...");
		if (job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}

}
