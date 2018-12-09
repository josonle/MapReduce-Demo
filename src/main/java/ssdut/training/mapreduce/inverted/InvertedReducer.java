package ssdut.training.mapreduce.inverted;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String fileList = new String();
		for (Text value : values) {
			fileList += value.toString() + "; ";
		}
		result.set(fileList);
		context.write(key, result);		//输出格式："word	file1:num1; file2:num2;"
	}
}
