package hdfshelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.streamsx.topology.function.Supplier;

public class HDFSFileSplitReadIterator implements Iterator<String>{
	final static byte NEWLINE=0xA;
	final static byte CARRIAGE_RETURN=13;
	final String filename; 
	final long startOffset;
	final long endOffset;
	boolean finished = false;
	byte lineBuffer[];
	transient FSDataInputStream inStream;
	transient BufferedPartialReader reader;
	int numLines = 0;
	
	public HDFSFileSplitReadIterator(String file, long start, long end) {
		Logger.getLogger(HDFSFileSplitReadIterator.class.getCanonicalName()).log(Level.ALL,"HDFSFileIterator constructor");
		filename = file;
		startOffset = start;
		endOffset = end;
	}
	
	public void remove() {
		
	}
	
	public boolean hasNext() {
		return !finished;
	}
	public String next() {
		if (!finished && reader==null) {
			init();
		}
		if (reader != null) {
			try {
			String line = reader.readLine();
			if (line == null) {
				// File is done, let's cleanup.
				reader.close();
				reader = null;
				finished = true;
				System.out.println(numLines+" read. End offset actually "+inStream.getPos()+" aiming for "+endOffset);
				return "done";	
			}
			else {
				numLines++;
				return line;
			}
			}
			catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		else {
			System.out.println("Unable to initialize");
			return null;
		}
	}
	
	void init() {
		System.out.println("******Init ReadHDFSInParallel "+filename+" "+startOffset+" "+endOffset);
		final Logger logger = Logger.getLogger(HDFSFileSplitReadIterator.class.getCanonicalName());
		Configuration conf = new Configuration();
		System.out.println("Configuration created");
		FileSystem fSystem;
		try {
			fSystem = FileSystem.get(conf);
			if (fSystem == null) {
				System.out.println("Problem getting FileSystem");
			}
			inStream = fSystem.open(new Path(filename));
			if (startOffset > 0) {
				inStream.seek(startOffset-1);
				byte myByte = inStream.readByte();
				while(myByte != NEWLINE && myByte != CARRIAGE_RETURN) {
					myByte = inStream.readByte();
				}
				if (myByte == CARRIAGE_RETURN) {
					if (inStream.readByte() != NEWLINE) {
						logger.log(Level.SEVERE,"Expected new line at "+(inStream.getPos()-1)+" found "+(int)myByte);
					}
				}
			}
			System.out.println("start advanced to "+inStream.getPos());
			if (inStream == null) {
				logger.log(Level.SEVERE,"Problem opening file "+filename);
			}
			reader = new BufferedPartialReader(inStream,inStream.getPos(),endOffset);
			logger.log(Level.INFO,"File successfully opened.");
		} catch (IOException e) {
			logger.log(Level.SEVERE,"Problem opening file: "+e);
			finished = true;
			inStream = null;
			reader = null;
		}
	}

}
