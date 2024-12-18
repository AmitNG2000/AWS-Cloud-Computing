package dsp.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public abstract class UserActionRecordReader extends RecordReader<User,UserAction> { 

	protected LineRecordReader reader;
	protected User key;
	protected UserAction value;
	
	protected abstract  User parseUser(String str);
	protected abstract UserAction parseUserAction(String str) throws IOException;

	UserActionRecordReader() {
		reader = new LineRecordReader();
		key = null;
		value = null;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(split, context);
	}


	@Override
	public void close() throws IOException {
		reader.close();		
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (reader.nextKeyValue()) {
			key = parseUser(reader.getCurrentValue().toString());
			value = parseUserAction(reader.getCurrentValue().toString());
			return true;
		} else {
			key = null;
			value = null;
			return false;
		}
	}
	
	@Override
	public User getCurrentKey() throws IOException, InterruptedException {
		return key;
	}
	
	@Override
	public UserAction getCurrentValue() throws IOException, InterruptedException {
		return value;
	}	


	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

}
