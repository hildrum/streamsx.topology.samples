package hdfshelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streams.operator.internal.runtime.api.PEContext;
import com.ibm.streamsx.topology.function.Supplier;

public class ReadHDFSFile implements Supplier<String>{

	final static byte NEWLINE=0xA;
	final static byte CARRIAGE_RETURN=13;
	final String filename; 
	long startOffset;
	long endOffset;
	transient BufferedReader reader; 
	transient FSDataInputStream inStream;
	final boolean readInParallel ;
	
	public ReadHDFSFile(String file, boolean readInParallel) {
		filename = file;
		startOffset =-1;
		endOffset = -1;
		this.readInParallel = readInParallel;
	}
	
	public ReadHDFSFile(String file, long start, long end) {
		filename = file;
		startOffset = start;
		endOffset = end;
		readInParallel = false;
	}
	
	@Override
	public String get() {
		if (reader != null) {
			try {
			String line = reader.readLine();
			if (line == null || endOffset > 0 && inStream.getPos() >= endOffset) {
				System.out.println("Done with "+startOffset+" to "+endOffset);
				// File is done, let's cleanup.
				reader.close();
				reader = null;
				return "done";
			}
			else {
				return line;
			}
			}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		else {
			return null;
		}
	}
	
	void init() {
		System.out.println("Init");
		Configuration conf = new Configuration();
		conf.addResource(new Path("/opt/ibm/biginsights/hadoop-conf/core-site.xml"));
		
		System.out.println("Configuration created");
		FileSystem fSystem;
		try {
			//Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
			fSystem = FileSystem.get(conf);
			if (fSystem == null) {
				System.out.println("Problem getting FileSystem");
			}
			int channel = PERuntime.getCurrentContext().getChannel();
			int maxChannels = PERuntime.getCurrentContext().getMaxChannels();
			System.out.println("Channel "+channel+" of "+maxChannels);
			if (readInParallel && channel > 0) {
				FileStatus fs = fSystem.getFileStatus(new Path(filename));
				long length = fs.getLen();
				startOffset = channel * (length/maxChannels);

				if (length >  maxChannels) {
					if (channel == maxChannels -1) {
						endOffset = length;
					}
					else {
				        endOffset = startOffset + (length/maxChannels) -1;  
					}
				}
			}
			inStream = fSystem.open(new Path(filename));
			if (startOffset > 0) {
				inStream.seek(startOffset-1);
				byte myByte = inStream.readByte();
				while(myByte != NEWLINE && myByte != CARRIAGE_RETURN) {
					myByte = inStream.readByte();
				}
				if (myByte == CARRIAGE_RETURN) {
					if (inStream.readByte() == NEWLINE) {
						System.err.println("Position "+inStream.getPos()+" expected newline");
					}
				}
			}
			if (inStream == null) {
				System.out.println("Problem opening file "+filename);
			}
			reader = new BufferedReader(new InputStreamReader(inStream));
			System.out.println("Reader successfully initialized");
		} catch (IOException e) {
			System.out.println("Caught IOException ");
			e.printStackTrace();
			reader = null;
		}
	}
	private Object readResolve() {
		init();
		return this;
	}
	
	

}
