package mapreduceProgram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSort {

	public static class MySortKey implements WritableComparable<MySortKey> {
		private int upFlow;
		private int downFlow;
		private int sumFlow;

		public MySortKey() {
			// TODO Auto-generated constructor stub
		}
		
		public MySortKey(int up, int down) {
			upFlow = up;
			downFlow = down;
			sumFlow = up + down;
		}

		public int getUpFlow() {
			return upFlow;
		}

		public void setUpFlow(int upFlow) {
			this.upFlow = upFlow;
		}

		public int getDownFlow() {
			return downFlow;
		}

		public void setDownFlow(int downFlow) {
			this.downFlow = downFlow;
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
		public int compareTo(MySortKey o) {
			if ((this.upFlow - o.upFlow) == 0) {// 上行流量相等，比较下行流量
				return o.downFlow - this.downFlow;// 按downFlow降序排序
			} else {
				return this.upFlow - o.upFlow;// 按upFlow升序排
			}
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return upFlow + "\t" + downFlow + "\t" + sumFlow;
		}
	}

	public static class SortMapper extends Mapper<Object, Text, MySortKey, Text> {
		Text phone = new Text();
		MySortKey mySortKey = new MySortKey();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lists = value.toString().split("\t");
			phone.set(lists[0]);
			mySortKey.setUpFlow(Integer.parseInt(lists[1]));
			mySortKey.setDownFlow(Integer.parseInt(lists[2]));
			context.write(mySortKey, phone);
			System.out.println(phone.toString()+":"+mySortKey.toString()+",up:"+lists[1]+"=="+mySortKey.getUpFlow());
		}
	}

	public static class SortReducer extends Reducer<MySortKey, Text, Text, MySortKey> {
		public void reduce(MySortKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				System.out.println(value.toString()+","+key.toString());
				context.write(value, key);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		// 设置job配置信息
		Job job = Job.getInstance(conf, "FlowSort");
		job.setJarByClass(FlowSort.class);
		job.setJar("export\\FlowSort.jar");
		// Mapper
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(MySortKey.class);
		job.setMapOutputValueClass(Text.class);
		// Reducer
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(MySortKey.class);
		// 作业输入输出路径
		String dataDir = "/workspace/flowStatistics/output/part-r-00000"; // 实验数据目录
		String outputDir = "/workspace/flowStatistics/output_sort"; // 实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		// 运行作业
		System.out.println("Job: FlowSort is running...");
		if (job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}
}
