package mutualFriend;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class DecomposeFriendsMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		String strs = value.toString();
		Text uString = new Text(strs.substring(0, 1));
		String[] friends = strs.substring(2).split(",");
		
		//A:B,C,D,F,E,O
		for (int i = 0; i < friends.length; i++) {
			// 以<B，A>,<C,A>形式输出
			context.write(new Text(friends[i]),uString);
		}
	}
}
