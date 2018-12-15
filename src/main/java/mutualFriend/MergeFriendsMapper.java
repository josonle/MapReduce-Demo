package mutualFriend;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeFriendsMapper extends Mapper<Object, Text, Text, Text>{// 别写成输入key也是Text类型，这里输入的是偏移量
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		Text uText = new Text(value.toString().substring(0, 1));
		String[] lists = value.toString().substring(2).split(",");
		Arrays.sort(lists);// 要排好序，不然如A-B，B-A不能归并到一起
		//对如A B,C,E遍历输出如<B-C A>
		for (int i = 0; i < lists.length; i++) {
			for(int j=i+1;j<lists.length;j++) {
				String friends = lists[i]+"-"+lists[j];
//				System.out.println(friends);
				context.write(new Text(friends), uText);
			}
		}
	}
}
