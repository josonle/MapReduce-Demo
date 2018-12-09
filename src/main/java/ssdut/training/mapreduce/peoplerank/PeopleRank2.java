package ssdut.training.mapreduce.peoplerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PeopleRank2 {
	public static enum Mycounter {
		my
	}

	static class PeopleRank2Mapper extends Mapper<Text, Text, Text, Text> {
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String pid = key.toString();	//key:"a"  value:"d c b"
			int runCount = context.getConfiguration().getInt("runCount", 1);	//从上下文配置中获取runCount的值，如果值为空则默认为1
			People people = null;
			if (runCount == 1) {	//第一次运行时，Map输入记录中没有PR值，将PR值默认设为1.0
				people = People.fromMR("1.0" + People.fieldSeparator + value.toString());//参数String格式："PeopleRank值	u1	u2..."
			} else {				//后续迭代的Map输入value中已经包含PR值，无需再指定
				people = People.fromMR(value.toString());
			}
			context.write(new Text(pid), new Text(people.toString())); 		//Map输出格式："userid pr值  userlist"
			
			if (people.containsAttentionPeoples()) {	//如果Map输入中有被关注人，则计算每个被关注人的概率并通过Map输出
				double outValue = people.getPeopleRank() / people.getAttentionPeoples().length;			//例如：a关注bcd，则bcd得到的概率都是1.0/3
				for (int i = 0; i < people.getAttentionPeoples().length; i++) {
					context.write(new Text(people.getAttentionPeoples()[i]), new Text(outValue + ""));	//Map输出格式："被关注人id 关注人投给被关注人的概率"
				}
			}
		}
	}

	static class PeopleRank2Reducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			People sourcePeople = null;
			for (Text v : values) {
				People people = People.fromMR(v.toString());
				if (people.containsAttentionPeoples()) {		//含有关注人的是源结构
					sourcePeople = people;
				} else {
					sum += people.getPeopleRank();				//将不含临近节点的PR值累加
				}
			}
			double newPR = (1 - 0.85) / 4 + (0.85 * sum);		//计算新的PR值 = (1-d)/N + d* sum（每个被关注者PR值/每个关注人链出数）  (阻尼系数取0.85)
			double d = newPR - sourcePeople.getPeopleRank();	//计算新旧PR差值
			int j = Math.abs( (int)(d*100.0) );					//取收敛差值放大100倍后的绝对值
			context.getCounter(Mycounter.my).increment(j);		//放入计数器
			sourcePeople.setPeopleRank(newPR);					//更新PR值			
			context.write(key, new Text(sourcePeople.toString()));	//输出格式："userid  新PR值  userlist"
		}
	}
	
	//查看Reduce输入
	static class TestReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text v : values) {
				sb.append("["+v.toString()+"] ");
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();		
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", namenode_ip);	
		conf.set("yarn.resourcemanager.scheduler.address", namenode_ip + ":8034");
		//conf.set("hadoop.job.user", "hadoop");		
		//conf.set("yarn.resourcemanager.resource-tracker.address", namenode_ip + ":8031");
		//conf.set("yarn.resourcemtanager.address", namenode_ip + ":8032");
		//conf.set("yarn.resourcemanager.admin.address", namenode_ip + ":8033");
		//conf.set("mapreduce.jobtracker.address", namenode_ip + ":9001");
		//conf.set("mapreduce.jobhistory.address", namenode_ip + ":10020");		
		//conf.set("mapreduce.map.memory.mb", "2048");
		//conf.set("mapreduce.task.io.sort.mb","300");
		
		String jobName = "PeopleRank";
		double e = 0.01;	//收敛差的阈值，e越小迭代次数越多
		int i = 0;
		while (true) {
			i++;
			conf.setInt("runCount", i);		//runCount记录迭代次数
			Job job = Job.getInstance(conf, jobName + i);
			job.setJarByClass(PeopleRank2.class);
			job.setJar("export\\PeopleRank2.jar");
			job.setMapperClass(PeopleRank2Mapper.class);
			job.setReducerClass(PeopleRank2Reducer.class);
			//job.setReducerClass(TestReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);		//设置Map输入为KV文本文件类型
			
			Path inPath = null;
			if (i == 1) {
				inPath = new Path(hdfs + "/expr/peoplerank/output/adjacent");
			} else {
				inPath = new Path(hdfs + "/expr/peoplerank/output/output" + (i - 1));	//每次迭代使用上一次的输出路径作为输入路径
			}
			Path outPath = new Path(hdfs + "/expr/peoplerank/output/output" + i);
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);			
			FileSystem fs = FileSystem.get(conf);				
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
			
			System.out.println("Job Iteration " + i + " is running...");
			boolean f = job.waitForCompletion(true);
			if (f) {
				long sum = job.getCounters().findCounter(Mycounter.my).getValue();
				double avge = sum / 400.0;	//sum值在reduce任务中放大了100倍，此处需缩小100倍
				if (avge < e) {				//当平均差值小于阈值时迭代结束
					break;
				}
			}
		}
		System.out.println("finished!");
	}

}
