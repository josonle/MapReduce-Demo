package ssdut.training.mapreduce.topten;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> visittimesMap = new TreeMap<Integer, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			String[] strs = val.toString().split(" ");
			visittimesMap.put(Integer.parseInt(strs[1]), new Text(val));	//将访问次数（KEY）和行记录（VALUE）放入TreeMap中自动排序
			if (visittimesMap.size() > 10) {		//如果TreeMap中元素超过N个，将第一个（KEY最小的）元素删除
				visittimesMap.remove(visittimesMap.firstKey());
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		//将TreeMap反序处理，降序输出top10
		NavigableMap<Integer, Text> reverMap = visittimesMap.descendingMap();	//获得TreeMap反序
		for (Text t : reverMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}


 
