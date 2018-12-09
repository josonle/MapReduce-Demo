package ssdut.training.mapreduce.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//计算用户评分矩阵
public class Step2 {
	public static boolean run(Configuration config, Map<String, String> paths) 
			throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step2";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step2.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step2_Mapper.class);
		job.setReducerClass(Step2_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Path inPath = new Path(paths.get("Step2Input"));
		Path outpath = new Path(paths.get("Step2Output"));
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);	
	}

	static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(",");
			String item = strs[0];		//商品id
			String user = strs[1];		//用户id
			String action = strs[2];	//用户行为
			Integer rv = StartRun.R.get(action);	//获取行为评分
			Text v = new Text(item + ":" + rv.intValue());	//value格式: "i1:1"
			Text k = new Text(user);
			context.write(k, v);	//map输出格式: "u2723  i1:1"
		}
	}

	static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> m = new HashMap<String, Integer>();	//用于存放每种商品的行为评分之和
			for (Text value : values) {
				String[] strs = value.toString().split(":");
				String item = strs[0];						//商品id
				Integer score = Integer.parseInt(strs[1]);	//行为评分
				score += ((Integer) (m.get(item) == null ? 0 : m.get(item))).intValue();	//计算用户对每件商品的行为评分和（如果Map集合中已有该商品评分，则累加）
				m.put(item, score);		//向HashMap中存入商品及评分之和
			}
			
			StringBuffer sb = new StringBuffer();
			for (Entry<String, Integer> entry : m.entrySet()) {
				sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");	//将商品和评分串联，格式：  i1:1,i2:1,...I:N,
			}
			context.write(key, new Text(sb.toString().substring(0, sb.toString().length() - 1)));	//去掉最后的逗号
		}
	}
}
