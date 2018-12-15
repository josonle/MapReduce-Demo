package mutualFriend;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeFriendsReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		String friends = "";
		for (Text value : values) {
			friends += value.toString()+",";
		}
		System.out.println(key.toString()+" "+friends);
		context.write(key, new Text(friends));
	}
}
