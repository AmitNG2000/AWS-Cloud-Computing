package dsp.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class InvertAndCountCallerRecipient {
	public static class MapperClass  extends Mapper<LongWritable,LongWritable,LongWritable,LongWritable> {
		@Override
		public void map(LongWritable caller, LongWritable recipient, Context context) throws IOException,  InterruptedException {
			context.write(recipient, caller);
		}
	}


	public static class ReducerClass  extends Reducer<LongWritable,LongWritable,LongWritable,IntWritable> {
		@Override
		public void reduce(LongWritable recipient, Iterable<LongWritable> callers, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (LongWritable caller : callers)
				count++;
			context.write(recipient, new IntWritable(count));
		}
	}
}
