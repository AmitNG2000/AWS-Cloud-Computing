package dsp.hadoop.examples;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

public class GenerateCallerRecipientGraph { 

public static class MapperClass extends Mapper<User,UserAction,LongWritable,LongWritable> {
    @Override
    public void map(User user, UserAction userAction, Context context) throws IOException,  InterruptedException {
    	
    	if (userAction.getAction().getType() == Action.ActionType.CALL)
    		context.write(
    			new LongWritable(user.getPhoneNumber()),
    			new LongWritable(Long.parseLong(userAction.getAction().getDesc())));
    }
}


 public static class ReducerClass extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
	@Override
    public void reduce(LongWritable caller, Iterable<LongWritable> recipients, Context context) throws IOException,  InterruptedException {
		for (LongWritable recipient : recipients)
			context.write(caller, recipient);		
    }	
  }
}
