package ssdut.training.mapreduce.minmaxcount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
	private Text outUserId = new Text();	//用户ID
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();	//日期最小值、日期最大值、评论数的组合
	private final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());	//分解每条评论，保存每对KV到Map对象
		String userId = map.get("UserId");			//从Map对象中获取用户ID
		String strDate = map.get("CreationDate");	//从Map对象中获取评论时间
		
		if (strDate == null || userId == null) {	//过滤掉不含统计数据的记录
			return;
		}
		try {
			Date creationDate = frmt.parse(strDate);
			// 因为还没有MinMax，只有把当前数据中日期作为MinMax
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);
			outTuple.setCount(1);
			outUserId.set(userId);
			context.write(outUserId, outTuple);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			return;
		}
	}
}
