package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedMapper extends Mapper<Object, Text, Text, Text> {
	private Text keyInfo = new Text();
	private Text valueInfo = new Text();
	private FileSplit split;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		split = (FileSplit) context.getInputSplit();	//通过context获取输入分片对象，目的是获得输入文件名称
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			keyInfo.set(itr.nextToken() + ":" + split.getPath().getName()); //将单词及其所属文件拼接成"word:filename"格式作为key
			valueInfo.set("1");
			context.write(keyInfo, valueInfo);  //输出格式： "word:filename	1"
		}
	}
}
