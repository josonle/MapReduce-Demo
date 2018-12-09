package mapreduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

public class FlowStatistics {

	public static class FlowMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			Text phone = new Text(strs[0]);
			Text flow = new Text(strs[1]+"\t"+strs[2]);
			context.write(phone, flow);
		}
	}
	
	public static class FlowReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int upFlow = 0;
			int downFlow = 0;
			
			for (Text value : values) {
				String[] strs = value.toString().split("\t");
				upFlow += Integer.parseInt(strs[0].toString());
				downFlow += Integer.parseInt(strs[1].toString());
			}
			int sumFlow = upFlow+downFlow;
			
			context.write(key,new Text(upFlow+"\t"+downFlow+"\t"+sumFlow));
		}
	}
	
	// 第二种写法
	public static class FlowWritableMapper extends Mapper<Object, Text, Text, FlowWritable> {
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			Text phone = new Text(strs[0]);
			FlowWritable flow = new FlowWritable(Integer.parseInt(strs[1]),Integer.parseInt(strs[2]));
			System.out.println("Flow is:"+flow.toString());
			context.write(phone, flow);
		}
	}
	public static class FlowWritableReducer extends Reducer<Text, FlowWritable, Text, FlowWritable>{
		public void reduce(Text key,Iterable<FlowWritable> values,Context context) throws IOException, InterruptedException {
			int upFlow = 0;
			int downFlow = 0;
			
			for (FlowWritable value : values) {
				upFlow += value.getUpFlow();
				downFlow += value.getDownFlow();
			}
			System.out.println(key.toString()+":"+upFlow+","+downFlow);
			context.write(key,new FlowWritable(upFlow,downFlow));
		}
	}
	
	public static class FlowWritable implements Writable{
		private int upFlow;
		private int downFlow;
		private int sumFlow;
		
		public FlowWritable() {}

		public FlowWritable(int upFlow,int downFlow) {
			this.upFlow = upFlow;
			this.downFlow = downFlow;
			this.sumFlow = upFlow+downFlow;
		}
		
		public int getDownFlow() {
			return downFlow;
		}

		public void setDownFlow(int downFlow) {
			this.downFlow = downFlow;
		}

		public int getUpFlow() {
			return upFlow;
		}

		public void setUpFlow(int upFlow) {
			this.upFlow = upFlow;
		}

		public int getSumFlow() {
			return sumFlow;
		}

		public void setSumFlow(int sumFlow) {
			this.sumFlow = sumFlow;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(upFlow);
			out.writeInt(downFlow);
			out.writeInt(sumFlow);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			upFlow = in.readInt();
			downFlow = in.readInt();
			sumFlow = in.readInt();
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return upFlow+"\t"+downFlow+"\t"+sumFlow;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置hdfs配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://"+namenode_ip+":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		// 设置作业Job配置信息
		String jobName = "FlowStatistics";
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(FlowStatistics.class);
		job.setJar("export\\FlowStatistics.jar");
		// Map
		job.setMapperClass(FlowMapper.class);// 第一种
//		job.setMapperClass(FlowWritableMapper.class);
		// 这里因为同Reducer输出类型一致，可不写
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(FlowWritable.class);
		// Reduce
		job.setReducerClass(FlowReducer.class);// 第一种
//		job.setReducerClass(FlowWritableReducer.class);
		// 输出k-v类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);// 第一种
//		job.setOutputValueClass(FlowWritable.class);
		
		// 设置job输入出路径
		String dataDir = "/workspace/flowStatistics/data";
		String outputDir = "/workspace/flowStatistics/output";
		Path inPath = new Path(hdfs+dataDir);
		Path outPath = new Path(hdfs+outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		// 运行作业
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
