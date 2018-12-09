package ssdut.training.mapreduce.itemcf;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//计算用户同显矩阵
public class Step3 {
	private final static Text K = new Text();
	private final static IntWritable one = new IntWritable(1);

	public static boolean run(Configuration config, Map<String, String> paths)  throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step3";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step3.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step3_Mapper.class);
		job.setReducerClass(Step3_Reducer.class);
		job.setCombinerClass(Step3_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		Path inPath = new Path(paths.get("Step3Input"));
		Path outpath = new Path(paths.get("Step3Output"));
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);		
	}

	static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// u2727 i468:2,i446:3			
			String[] items = value.toString().split("\t")[1].split(",");			//每件商品和评分列表，格式：i468:2 i446:3
			for (int i = 0; i < items.length; i++) {
				String itemA = items[i].split(":")[0];		// itemA = i468 .. i446
				for (int j = 0; j < items.length; j++) {
					String itemB = items[j].split(":")[0];	// itemB = i468 .. i446
					K.set(itemA + ":" + itemB);				// i468:i468 , i468:i446, i446:i468, i446:i446
					context.write(K, one);
				}
			}
		}
	}

	static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
