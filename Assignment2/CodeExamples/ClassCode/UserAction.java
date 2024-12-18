package dsp.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.Writable;

import dsp.hadoop.examples.Action.ActionType;

public class UserAction implements Writable {
		
	protected Date date;
	protected Action action;
	
	public UserAction() {
		date = null;
		action = new Action();
	}
	
	public UserAction(Date date, ActionType actionType, String actionDesc) throws ParseException {
		this.date = date; 
		this.action = new Action(actionType,actionDesc);
	}
	
	public void readFields(DataInput in) throws IOException {
		date = new Date(in.readLong());
		action.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(date.getTime());
		action.write(out);
	}
	
	public Action getAction() {
		return action;
	}
	
	public Date getDate() {
		return date;
	}
	
}
