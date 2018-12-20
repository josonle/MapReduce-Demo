package ssdut.training.mapreduce.weblog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//4. 计算每天访问该网站的独立IP数
public class IPCount {

	public enum IpCounter {
		ipnum1, ipnum2
	}
	
	public static class IPCountMapper extends Mapper<Object, Text, DayAndIp, IntWritable> {		
		private SimpleDateFormat SDFIN = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		private SimpleDateFormat SDFOUT = new SimpleDateFormat("yyyy-MM-dd");
        private DayAndIp k = new DayAndIp();						//Map输出Key：日期+IP
        private final static IntWritable one = new IntWritable(1);	//Map输出Value
        
		public void map(Object key, Text value, Context context ) 
				throws IOException, InterruptedException {
			String[] strs = value.toString().split(" ");
			String strIP = strs[0];						//获取IP字符串
			String strTime = strs[3].substring(1);		//获取时间字符串
			String strDate = null;						//定义日期字符串
			try {
				strDate = SDFOUT.format(SDFIN.parse(strTime));	//时间格式转成日期格式
			} catch (ParseException e) {
				e.printStackTrace();
			}			
			k.setDate(strDate);
			k.setIp(strIP);
			context.write(k, one);
	    }
	}
  
	public static class IPCountReducer extends Reducer<DayAndIp,IntWritable,DayAndIp,IntWritable> {
		public void reduce(DayAndIp key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
			String[] strs = key.toString().split("\t");
			if ( strs[0].equals("2013-05-30") ) {
				context.getCounter(IpCounter.ipnum1).increment(1);	//使用计数器统计某天访问的IP数
			} else {
				context.getCounter(IpCounter.ipnum2).increment(1);
			}
	    }
	}

	public static void main(String[] args) throws Exception {		
		//1.设置HDFS配置信息
		String namenode_ip = "192.168.17.10";
		String hdfs = "hdfs://" + namenode_ip + ":9000";			
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfs);
		conf.set("mapreduce.app-submission.cross-platform", "true");

		//2.设置MapReduce作业配置信息
		String jobName = "IPCount";						//作业名称
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(IPCount.class);				//指定运行时作业类
		job.setJar("export\\IPCount.jar");				//指定本地jar包
		job.setMapperClass(IPCountMapper.class);		//指定Mapper类
		job.setMapOutputKeyClass(DayAndIp.class);		//设置Mapper输出Key类型
		job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型
		job.setReducerClass(IPCountReducer.class);		//指定Reducer类
		job.setOutputKeyClass(DayAndIp.class);			//设置Reduce输出Key类型
		job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型
		
		//3.设置作业输入和输出路径
		String dataDir = "/expr/weblog/data";			//实验数据目录	
		String outputDir = "/expr/weblog/output4";		//实验输出目录
		Path inPath = new Path(hdfs + dataDir);
		Path outPath = new Path(hdfs + outputDir);
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		//4.运行作业
		System.out.println("Job: " + jobName + " is running...");
		if(job.waitForCompletion(true)) {
			System.out.println("success!");
			System.exit(0);
		} else {
			System.out.println("failed!");
			System.exit(1);
		}
	}

	//自定义KEY类，封装日期和IP
	public static class DayAndIp implements WritableComparable<DayAndIp> {
		private String date;
		private String ip;
		
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
		public String getIp() {
			return ip;
		}
		public void setIp(String ip) {
			this.ip = ip;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(date);
			out.writeUTF(ip);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			date = in.readUTF();
			ip = in.readUTF();	
		}		

		@Override
		public int compareTo(DayAndIp o) {
			int r = date.compareTo(o.getDate());
			if ( r == 0 ) {
				return ip.compareTo(o.getIp());
			}
			return r;
		}
		
		@Override
		public String toString() {
			return date + "\t" + ip;
		}
	}
}