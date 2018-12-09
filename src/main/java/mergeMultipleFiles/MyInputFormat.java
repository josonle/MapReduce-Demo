package mergeMultipleFiles;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable>{

	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO 因为是合并小文件，设置文件不可分割，k-v的v就是文件对象
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		MyRecordReader myRecordReader = new MyRecordReader();
		myRecordReader.initialize(split, context);
		return myRecordReader;
	}

}
