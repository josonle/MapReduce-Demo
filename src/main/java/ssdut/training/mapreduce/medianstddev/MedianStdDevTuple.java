package ssdut.training.mapreduce.medianstddev;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class MedianStdDevTuple implements Writable {
	private float median = 0f;
	private float stddev = 0f;

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getStddev() {
		return stddev;
	}

	public void setStddev(float stddev) {
		this.stddev = stddev;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		median = in.readFloat();
		stddev = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(median);
		out.writeFloat(stddev);
	}

	@Override
	public String toString() {
		return median + "\t" + stddev;
	}
	
}