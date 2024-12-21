package dsp.hadoop.examples;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class Call implements WritableComparable<Call> {
	long caller;
	long recipient;
	int year;
	
	Call() {
		this.caller = -1;
		this.recipient = -1;
		this.year = -1;
	}
	
	Call(long caller, long recipient, int year) {
		this.caller = caller;
		this.recipient = recipient;
		this.year = year;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.caller = in.readLong();
		this.recipient = in.readLong();
		this.year = in.readInt();
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(caller);
		out.writeLong(recipient);
		out.writeInt(year);
		
	}
	
	public int compareTo(Call other) {
		int ret = (int) (caller - other.caller);
		if (ret == 0)
			ret = (int) (recipient - other.recipient);
		if (ret == 0)
			ret = year - other.year;

		return ret;
	}

	public long getCaller() {
		return caller;
	}

	public long getRecipient() {
		return recipient;
	}

	public int getYear() {
		return year;
	}

}


public class DistributePhoneCallsPerYear { 

public static class MapperClass extends Mapper<User,UserAction,Call,IntWritable> {
    @Override
    public void map(User user, UserAction userAction, Context context) throws IOException,  InterruptedException {
    	
    	if (userAction.getAction().getType() == Action.ActionType.CALL) {
    		Calendar cal = Calendar.getInstance();
    		cal.setTime(userAction.getDate());
    		context.write(
    			new Call(
    					user.getPhoneNumber(),
    					Long.parseLong(userAction.getAction().desc),
    					cal.get(Calendar.YEAR)),
    			new IntWritable(1));
    	}
    }
  }

  public static class ReducerClass extends Reducer<Call,IntWritable,Call,IntWritable> {
	@Override
    public void reduce(Call call, Iterable<IntWritable> counts, Context context) throws IOException,  InterruptedException {
		int sum = 0;
		for (IntWritable count : counts)
			sum += count.get();
			context.write(call, new IntWritable(sum));		
    }
  }

  
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "DistributePhoneCallsPerYear");
    job.setJarByClass(DistributePhoneCallsPerYear.class);
    job.setMapperClass(MapperClass.class);
    job.setReducerClass(ReducerClass.class);
    job.setMapOutputKeyClass(Call.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Call.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CellcomUserActionInputFormat .class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
