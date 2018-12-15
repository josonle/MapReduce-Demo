package mutualFriend;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class DecomposeFriendsReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String friendList = "";
		for (Text value : values) {
			friendList += value.toString()+",";
		}
		// 输出个人所有好友，A	I,K,C,B,G,F,H,O,D
		context.write(key, new Text(friendList.substring(0,friendList.length()-1)));
	}

}
