package ssdut.training.mapreduce.medianstddev;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	private IntWritable outHour= new IntWritable();
	private IntWritable outCommentLength= new IntWritable();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@SuppressWarnings("deprecation")
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
		String strDate = map.get("CreationDate");		//获取评论日期
		String text = map.get("Text");					//获取评论内容
		if (strDate == null || text == null) {
			return;
		}
		try {
			Date creationDate = frmt.parse(strDate);	//转换日期格式
			outHour.set(creationDate.getHours());		//从日期中获取小时值
			outCommentLength.set(text.length());		//设置评论内容的长度
			context.write(outHour, outCommentLength);	//将小时和评论长度作为Map输出
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			return;
		}
	}
}