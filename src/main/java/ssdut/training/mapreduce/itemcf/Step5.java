package ssdut.training.mapreduce.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
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

//计算总和评分
public class Step5 {
	public static boolean run(Configuration config, Map<String, String> paths) 
			throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step5";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step5.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step5_Mapper.class);
		job.setReducerClass(Step5_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Path inPath = new Path(paths.get("Step5Input"));
		Path outpath = new Path(paths.get("Step5Output"));
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);		
	}

	static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//输入格式："u2732	i405,2.0"
			String[] strs = Pattern.compile("[\t,]").split(value.toString());
			Text k = new Text(strs[0]);						//key: userID
			Text v = new Text(strs[1] + "," + strs[2]);		//value: "itemID,评分"
			context.write(k, v);
		}
	}

	static class Step5_Reducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<String, Double>();	//用于对商品评分累加
			for (Text val : values) {	//val格式: "itemID,评分"
				String[] strs = val.toString().split(",");
				String itemID = strs[0];
				Double score = Double.parseDouble(strs[1]);
				
				if (map.containsKey(itemID)) {	//如果Map中已记录该商品，取出评分累加后重新写入Map
					map.put(itemID, map.get(itemID) + score);
				} else {
					map.put(itemID, score);
				}
			}
			
			//遍历Map，完成输出
			Iterator<String> iter = map.keySet().iterator();	//根据itemID创建迭代器对象
			while (iter.hasNext()) {
				String itemID = iter.next();					//取出itemID
				double score = map.get(itemID);					//根据itemID从map中取出score
				context.write(key, new Text(itemID + "," + score));	//格式："userid	itemID,score"
			}
		}
	}
}
