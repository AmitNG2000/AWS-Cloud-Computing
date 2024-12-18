package dsp.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class User implements WritableComparable<User> {

	protected long phonenumber;
	protected String name;
			
	public User() {
		phonenumber = -1;
		name = null;
	}
	
	public User(long phonenumber, String name) {
		this.phonenumber = phonenumber;
		this.name = name;
	}
	
	public long getPhoneNumber() { return phonenumber; }
	public String getName() { return name; }
	
	public void readFields(DataInput in) throws IOException {
		phonenumber = in.readLong();
		name = in.readUTF();
		
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeLong(phonenumber);
		out.writeUTF(name);
	}
	
	
	public int compareTo(User other) {
		return (int) (phonenumber - other.phonenumber);
	}
	
}