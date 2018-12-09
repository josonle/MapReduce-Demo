package ssdut.training.mapreduce.medianstddev;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple> {
	private MedianStdDevTuple result = new MedianStdDevTuple();			//记录评论长度中位数和标准差
	private ArrayList<Float> commentLengths = new ArrayList<Float>();	//用列表记录每条评论的长度

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;		//评论长度总和
		float count = 0;	//评论数
		commentLengths.clear();	//清空评论数列表
		result.setStddev(0);	//标准差默认值设为0		
		for (IntWritable val : values) {
			commentLengths.add((float) val.get());	//将评论长度保存到列表
			sum += val.get();	//计算评论长度总和
			count++;			//评论总数
		}
		
		//计算中位数：集合数量如为偶数，取中间两位的均值；如为奇数，则直接取中值
		Collections.sort(commentLengths);	//对集合中评论字数排序
		if (count % 2 == 0) {//偶
			result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths.get((int) count / 2)) / 2.0f);
		} else {//奇
			result.setMedian(commentLengths.get((int) count / 2));
		}
		
		//计算标准差
		float mean = sum / count;	//计算评论的平均字数
		float sumOfSquares = 0.0f;	//平方和
		for (Float f : commentLengths) {
			sumOfSquares += (f - mean) * (f - mean);
		}
		result.setStddev((float) Math.sqrt(sumOfSquares / (count - 1)));	//计算标准差
		context.write(key, result);
	}
}