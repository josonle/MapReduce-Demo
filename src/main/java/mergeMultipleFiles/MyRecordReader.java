package mergeMultipleFiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable>{
	private FileSplit fileSplit;
	private Configuration conf ;
	private BytesWritable value = new BytesWritable();
	private boolean processed =false;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		fileSplit = (FileSplit)split;
		conf = context.getConfiguration();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];// 获取分片长度字节数组
			Path file = fileSplit.getPath();// 获取切片所在位置
			FileSystem fSystem = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fSystem.open(file);// 打开文件
				IOUtils.readFully(in, contents, 0, contents.length);// 读取整个文件字节数据，写入contents
				value.set(contents,0,contents.length);// 将整个文件数据赋值给value
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// 获取当前key，因为合并文件，我们应该将文件对象付给value，key赋空即可
		return NullWritable.get();
	}
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;// value是整个文件对象的字节数据
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ? 1.0f:0.0f;
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
