package ssdut.training.mapreduce.topten;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	private TreeMap<Integer, Text> visittimesMap = new TreeMap<Integer, Text>();	//TreeMap是有序KV集合

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (value == null) {
			return;
		}
		String[] strs = value.toString().split(" ");
		String tId = strs[0];
		String tVisittimes = strs[1];
		if (tId == null || tVisittimes == null) {
			return;
		}
		visittimesMap.put(Integer.parseInt(tVisittimes), new Text(value));	//将访问次数（KEY）和行记录（VALUE）放入TreeMap中自动排序
		if (visittimesMap.size() > 10) {	//如果TreeMap中元素超过N个，将第一个（KEY最小的）元素删除
			visittimesMap.remove(visittimesMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Text t : visittimesMap.values()) {
			context.write(NullWritable.get(), t);	//在clean()中完成Map输出
		}
	}
}
