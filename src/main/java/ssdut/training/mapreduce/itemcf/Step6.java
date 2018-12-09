package ssdut.training.mapreduce.itemcf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//评分排序取Top10
public class Step6 {
	private final static Text K = new Text();
	private final static Text V = new Text();

	public static boolean run(Configuration config, Map<String, String> paths) 
			throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step6";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step6.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step6_Mapper.class);
		job.setReducerClass(Step6_Reducer.class);		
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(Text.class);
		//job.setSortComparatorClass(ScoreSort.class);			//自定义排序
		job.setGroupingComparatorClass(UserGroup.class);	//自定义分组
		
		Path inPath = new Path(paths.get("Step6Input"));
		Path outpath = new Path(paths.get("Step6Output"));
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);		
	}

	static class Step6_Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = Pattern.compile("[\t,]").split(value.toString());	//输入格式："u13	i524,3.0"
			String user = strs[0];
			String item = strs[1];
			String score = strs[2];
			
			PairWritable k = new PairWritable();	//将uid和score封装到PairWritable对象中，作为MapKey输出
			k.setUid(user);
			k.setScore(Double.parseDouble(score));
			
			V.set(item + ":" + score);	//将item和score组合，作为MapValue输出
			context.write(k, V);		//输出格式：key:"u13 3.0"  value:"i524:3.0"
		}
	}

	static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text> {
		protected void reduce(PairWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i = 0;
			StringBuffer sb = new StringBuffer();
			for (Text v : values) {
				if (i == 10)
					break;
				sb.append(v.toString() + ",");	//将评分数前10项串联
				i++;
			}
			K.set(key.getUid());	//获取自定义key中的uid
			V.set(sb.toString().substring(0,sb.toString().length()-1));	//去掉最后的逗号
			context.write(K, V);
		}
	}

	static class PairWritable implements WritableComparable<PairWritable> {
		private String uid;
		private double score;
		
		public String getUid() {
			return uid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}

		public double getScore() {
			return score;
		}

		public void setScore(double score) {
			this.score = score;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(uid);
			out.writeDouble(score);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.uid = in.readUTF();
			this.score = in.readDouble();
		}

		@Override
		public int compareTo(PairWritable o) {
			int r = this.uid.compareTo(o.getUid());	//按uid升序排列
			if (r == 0) {
				return -Double.compare(this.score, o.getScore()); //uid相同，则按score降序排列
			}
			return r;
		}		
	}

	//自定义排序：先按uid升序，再按score降序
	/*static class ScoreSort extends WritableComparator {
		public ScoreSort() {
			super(PairWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 = (PairWritable) a;
			PairWritable o2 = (PairWritable) b;
			int r = o1.getUid().compareTo(o2.getUid());	//按uid升序排列
			if (r == 0) {
				return -Double.compare(o1.getScore(), o2.getScore());	//按num降序排列
			}
			return r;
		}
	}*/

	//自定义分组，Map输出key（PairWritable）中uid相同的记录设为同组
	static class UserGroup extends WritableComparator {
		public UserGroup() {
			super(PairWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 = (PairWritable) a;
			PairWritable o2 = (PairWritable) b;
			return o1.getUid().compareTo(o2.getUid());
		}
	}
}
