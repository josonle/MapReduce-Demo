package ssdut.training.mapreduce.itemcf;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//去重
public class Step1 {
	public static boolean run(Configuration config, Map<String, String> paths)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step1";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step1.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step1_Mapper.class);
		job.setReducerClass(Step1_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		Path inPath = new Path(paths.get("Step1Input"));
		Path outpath = new Path(paths.get("Step1Output"));
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);
	}

	static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0) {	//过滤掉输入文件标题行
				context.write(value, NullWritable.get());
			}
		}
	}

	static class Step1_Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}
