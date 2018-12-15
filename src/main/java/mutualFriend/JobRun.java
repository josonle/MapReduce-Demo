package mutualFriend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();		
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		// job1配置信息
		Job job1 = Job.getInstance(conf,"Decompose");
		job1.setJarByClass(JobRun.class);
		job1.setJar("export\\mutualFriend.jar");
		job1.setMapperClass(DecomposeFriendsMapper.class);
		job1.setReducerClass(DecomposeFriendsReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path input = new Path(hdfs+"/workspace/mutualFriends/data");
		Path output1 = new Path(hdfs+"/workspace/mutualFriends/output_Dec");
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output1);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output1)) {
			fs.delete(output1, true);
		}
		
		// job1如果运行成功则进入job2
		if(job1.waitForCompletion(true)) {//job2完全依赖job1的结果，所以job1成功执行就开启job2
			// job2配置信息
			Job job2 = Job.getInstance(conf, "Merge");
			job2.setJarByClass(JobRun.class);
			job2.setJar("export\\mutualFriend.jar");
			job2.setMapperClass(MergeFriendsMapper.class);
			job2.setReducerClass(MergeFriendsReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			Path output2 = new Path(hdfs+"/workspace/mutualFriends/output_Meg");
			FileInputFormat.addInputPath(job2, output1);// 输入是job1的输出
			FileOutputFormat.setOutputPath(job2, output2);
			if (fs.exists(output2)) {
				fs.delete(output2, true);
			}
			if(job2.waitForCompletion(true)) {
				System.out.println("sucessed");
			}else {
				System.out.println("failed");
			}
		}
	}
}
