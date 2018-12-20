package ssdut.training.mapreduce.weblog;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//3. 找到访问量最高的10个页面（按访问量降序输出）
public class PVTopTen {	
	public static class PVTopTenMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text k = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			String reqResource = strs[6];		//获取请求资源字符串
			int index = reqResource.indexOf("?");
			if ( index > 0 ) {
				reqResource = reqResource.substring(0, index);	//截取问号前的请求资源名称（去掉请求参数）
			}
			if ( reqResource.endsWith(".html") || reqResource.contains(".php") ) {
				k.set(reqResource);
				context.write(k, one);
			}			
		}
	}
	
	public static class PVTopTenReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
		public TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();	//计算被请求页面的访问量
			}
			String str = String.valueOf(sum) + "\t" +  key.toString() ;
			map.put(sum, new Text(str));	//将页面访问量和被请求页面名称放入TreeMap中，TreeMap按KEY键（访问量）自动排序
			if (map.size() > 10) {	//如果TreeMap中元素超过N个，则将第一个（KEY最小的）元素删除
				map.remove(map.firstKey());
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			//将TreeMap反序处理（降序），遍历输出top10
			NavigableMap<Integer, Text> reverseMap = map.descendingMap();
			for ( Entry<Integer, Text> entry  : reverseMap.entrySet() ) {
				context.write(entry.getValue(), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		String jobName = "PVTopTenJob";
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(PVTopTen.class);
		job.setJar("export\\PVTopTen.jar");
		job.setMapperClass(PVTopTenMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(PVTopTenReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);		//计算最终TopN，只能运行一个Reduce任务

		String dataDir = "/expr/weblog/data";	
		String outputDir = "/expr/weblog/output3";
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		System.out.println( "Job: " + jobName + " is running...");
		if(job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}
	
}