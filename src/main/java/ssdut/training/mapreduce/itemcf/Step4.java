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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//同显矩阵*评分矩阵，计算评分单项
public class Step4 {
	public static boolean run(Configuration config, Map<String, String> paths) 
			throws IOException, ClassNotFoundException, InterruptedException {
		String jobName = "step4";
		Job job = Job.getInstance(config, jobName);
		job.setJarByClass(Step4.class);
		job.setJar("export\\ItemCF.jar");
		job.setMapperClass(Step4_Mapper.class);
		job.setReducerClass(Step4_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		Path[] inPaths = new Path[] { 
				new Path(paths.get("Step4Input1")),
				new Path(paths.get("Step4Input2")) };		
		Path outpath = new Path(paths.get("Step4Output"));
		FileInputFormat.setInputPaths(job, inPaths);
		FileOutputFormat.setOutputPath(job, outpath);		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		
		return job.waitForCompletion(true);
	}

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;	//保存Map输入数据来自于哪个目录（output2或ouput3），用于判断数据是同现矩阵还是评分矩阵

		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();	//根据上下文获取输入分片对象
			flag = split.getPath().getParent().getName();			//获取输入分片所属的目录名称
		}

		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] strs = Pattern.compile("[\t,]").split(value.toString());
			if (flag.equals("output3")) {				//输入的是同现矩阵，strs格式："i100:i105 1"
				String[] items = strs[0].split(":");	
				String itemID1 = items[0];				//第一个商品id  "i100"
				String itemID2 = items[1];				//第二个商品id	 "i105"
				String num = strs[1];					//两件商品的同现次数    "1"
				
				Text k = new Text(itemID1);
				Text v = new Text("A:" + itemID2 + "," + num);	//格式："A:i105,1"
				context.write(k, v);							//格式："i100	A:i105,1"
				
			} else if (flag.equals("output2")) {	//输入的是评分矩阵，strs格式："u14 i100:1 i25:1"
				String userID = strs[0];
				for (int i = 1; i < strs.length; i++) {
					String[] vector = strs[i].split(":");	//i100:1
					String itemID = vector[0];
					String score = vector[1];
					Text k = new Text(itemID);				
					Text v = new Text("B:" + userID + "," + score);	//格式："B:u14,1"
					context.write(k, v);							//格式："i100 B:u14,1" 和 "i25 B:u14,1"
				}
			}
		}
	}

	static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> mapA = new HashMap<String, Integer>();
			Map<String, Integer> mapB = new HashMap<String, Integer>();
			//reduce输入格式："i100  A:i105,1  A:i107,2  B:u14,1  B:u22,3"
			for (Text val : values) {	//将AB格式的输入分别放入HashMap中
				String str = val.toString();
				if (str.startsWith("A:")) {			//str格式："A:i105,1"
					String[] kv = Pattern.compile("[\t,]").split(str.substring(2));
					mapA.put(kv[0], Integer.parseInt(kv[1]));
				} else if (str.startsWith("B:")) {	//str格式："B:u14,1"
					String[] kv = Pattern.compile("[\t,]").split(str.substring(2));
					mapB.put(kv[0], Integer.parseInt(kv[1]));
				}
			}
			double result = 0;
			Iterator<String> itera = mapA.keySet().iterator();		//根据mapA中key键(itemID)生成迭代器对象
			while (itera.hasNext()) {
				String mapka = itera.next();							//获得itemID
				int num = mapA.get(mapka).intValue();				//根据itemID从mapA获取同现次数
				
				Iterator<String> iterb = mapB.keySet().iterator();	//根据mapB中key键生成迭代器对象
				while (iterb.hasNext()) {
					String mapkb = iterb.next();					//userID
					int score = mapB.get(mapkb).intValue();			//根据userID从mapB中获取用户行为评分
					
					result = num * score;							//矩阵相乘，计算评分
					context.write(new Text(mapkb), new Text(mapka + "," + result));	//输出 key："userID" value:"itemID,result"
				}
			}
		}
	}
}
