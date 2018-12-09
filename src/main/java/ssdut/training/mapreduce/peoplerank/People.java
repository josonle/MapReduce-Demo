package ssdut.training.mapreduce.peoplerank;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;

public class People {
	private double peopleRank = 1.0;		//存储 PR值，初值默认1.0
	private String[] attentionPeoples;		//关注的人
	public static final char fieldSeparator = '\t';	//多处使用分隔符\t，定义为常量

	public double getPeopleRank() {
		return peopleRank;
	}

	public People setPeopleRank(double pageRank) {
		this.peopleRank = pageRank;
		return this;
	}

	public String[] getAttentionPeoples() {
		return attentionPeoples;
	}

	public People setAttentionPeoples(String[] attentionPeoples) {
		this.attentionPeoples = attentionPeoples;
		return this;
	}

	//判断是否包含关注用户
	public boolean containsAttentionPeoples() {
		return attentionPeoples != null && attentionPeoples.length > 0;
	}

	@Override
	//People对象转成字符串
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(peopleRank);
		if (attentionPeoples != null) {
			sb.append(fieldSeparator).append(StringUtils.join(attentionPeoples, fieldSeparator));
		}
		return sb.toString();	//返回String格式："PeopleRand值	u1	u2..."
	}
	
	//字符串转成People对象
	public static People fromMR(String str) throws IOException {	//参数String格式："PeopleRand值	u1	u2..."
		People people = new People();
		String[] strs = StringUtils.splitPreserveAllTokens(str, fieldSeparator);	//将字符串按分隔符分割成字符串数组
		people.setPeopleRank(Double.valueOf(strs[0]));	//处理第一个元素
		if (strs.length > 1) {// 设置关注的人，从strs下标为1的位置开始（因为传进来类似"1.0 b c d"的字符串）
			people.setAttentionPeoples(Arrays.copyOfRange(strs, 1, strs.length));	//处理其它元素
		}
		return people;	//返回People对象
	}
}
