package dsp.hadoop.examples;

import java.io.IOException;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MergeFiles {
	
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		FileSystem local = FileSystem.getLocal(conf);
		Path inputDir = new Path(args[0]); 
		Path hdfsFile = new Path(args[1]);
		try {
			FileStatus[] inputFiles = local.listStatus(inputDir); 
			OutputStream out = hdfs.create(hdfsFile); 
			for (int i=0; i<inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				InputStream in = local.open(inputFiles[i].getPath());
				byte buffer[] = new byte[256];	
				int bytesRead = 0;
				while( (bytesRead = in.read(buffer)) > 0) 
					out.write(buffer, 0, bytesRead);
				in.close();
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}