package ssdut.training.mapreduce.inverted;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedCombiner extends Reducer<Text, Text, Text, Text> {
	private Text info = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (Text value : values) {
			sum += Integer.parseInt(value.toString());	//单词数求和
		}
		int splitIndex = key.toString().indexOf(":");	//获取key中冒号的下标
		//注意此处应先计算info再计算key，否则key下标会越界
		info.set(key.toString().substring(splitIndex + 1) + ":" + sum);	//将key中冒号后的内容（文件名）与单词数总和组合成Combiner输出的value
		key.set(key.toString().substring(0, splitIndex));				//将key中冒号前的内容（单词）设置为Combiner输出的key
		context.write(key, info);		//输出格式："word	filename:sum"
	}
}
