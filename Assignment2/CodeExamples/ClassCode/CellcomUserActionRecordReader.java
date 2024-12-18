package dsp.hadoop.examples;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import dsp.hadoop.examples.Action.ActionType;

public class CellcomUserActionRecordReader extends UserActionRecordReader {
	
	protected User parseUser(String str) {
		String[] toks = str.split("###")[0].split("!");
		long phonenumber = Long.parseLong(toks[0]);
		String name = toks[1];
		return new User(phonenumber,name);
	}

	protected UserAction parseUserAction(String str) throws IOException {
		try {
			String[] toks = str.split("###")[1].split("!");
			Date date = new SimpleDateFormat().parse(toks[0]); 
			ActionType type =  ActionType.valueOf(toks[1]);
			String actionDesc = toks[2];
			return new UserAction(date,type,actionDesc);
		} catch (ParseException e) {
			throw new IOException(e);
		}	
	}

}
