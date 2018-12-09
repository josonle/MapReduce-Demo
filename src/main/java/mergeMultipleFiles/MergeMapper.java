package mergeMultipleFiles;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergeMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{
	private Text fileNameKey;

	@Override
	protected void map(NullWritable key, BytesWritable value,
			Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(fileNameKey, value);
	}

	@Override
	protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		InputSplit split = context.getInputSplit();
		Path path = ((FileSplit)split).getPath();//???
		fileNameKey = new Text(path.toString());
	}
	
}
