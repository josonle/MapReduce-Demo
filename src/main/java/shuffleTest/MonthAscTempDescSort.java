package shuffleTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MonthAscTempDescSort {
//	按年分区，每个文件中按月升序，按温度降序
	public static class MonthTempMapper extends Mapper<Object, Text, Text, IntWritable> {
		IntWritable temp = new IntWritable();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strings =value.toString().split(" ");
			String date = strings[0].substring(0, 7);
			temp.set(Integer.parseInt(strings[2].substring(0, strings[2].length()-1)));
			context.write(new Text(date), temp);
		}
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
