package hdfshelper;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

public class BufferedPartialReader implements Closeable{
	 static final int DEFAULT_BUFFER_SIZE = 8192;
		final static byte NEWLINE=0xA;
		final static byte CARRIAGE_RETURN=13;
	 int pos = 0;
	 int limit = 0;
	 byte buffer[] = new byte[DEFAULT_BUFFER_SIZE];
	 boolean eatFirstNewline = false;
	 final long startPos;
	 final long endPos;
	 // describes the file position of the starting point of the array.
	 long currFilePosition;
	 boolean inputDone = false;
	final FSDataInputStream in;
	 BufferedPartialReader(FSDataInputStream byteStream) {
		 this(byteStream, 0,-1);
	 }
	 
	 BufferedPartialReader(FSDataInputStream byteStream, long startPos, long endPos) {
		in = byteStream;
		this.startPos = startPos;
		this.endPos = endPos;
		currFilePosition = startPos;
	 }

	 private void fillBuffer() throws IOException {
		// System.out.println("Need to fill buffer pos "+pos+" limit "+limit);
		 	if (pos == 0) {
		 		// nothing to do, ready ot read in array.
		 	}
		 	else if (pos < buffer.length/2 && limit == buffer.length) {
		 		// In this case, we tried to get a line and couldn't get it, so our lines are super 
		 		// long.
		 	    byte[] oldbuffer = buffer;
		 	    buffer = new byte[oldbuffer.length];
		 	    for (int i = pos; i < limit; i++) {
		 	    	buffer[i] = oldbuffer[i];
		 	    }
		 	}
		 	else {
		 		// just move stuff from teh end of hte buffer to the beginning
		 		for (int i = 0; i < limit - pos; i++) {
		 			buffer[i] = buffer[pos+i];
		 		}
		 	}
		 	int firstFree = limit - pos;
			int numRead =in.read(currFilePosition+limit,buffer,firstFree,buffer.length-firstFree);
		//	System.out.println("Numread = "+numRead);
			currFilePosition += pos;
		//	System.out.println("Current file position moved to "+currFilePosition);
			pos = 0;
			limit = firstFree + numRead;
			if (numRead< 0) {
				inputDone = true;
				return;
			}
			if (firstFree == 0 && eatFirstNewline && buffer[0] == NEWLINE) {
				pos++;
			}
		}
	 
	  public String readLine() throws IOException{
		  return readLine(0);
	  }
	  public String readLine(int level) throws IOException {
		  
		  // Our real current position is currFilePOsition + pos.  
		  if (currFilePosition + pos >= endPos ||
				  (inputDone && pos == limit)) {
			  return null;
		  }
		
		int stringLen = -1;
		for (int i = pos; i < limit; i++) {
			if (buffer[i] == NEWLINE || buffer[i] == CARRIAGE_RETURN) {
				stringLen= i-pos;
				break;
			}
		}
		if (stringLen < 0 && inputDone) {
			// EOF
			String toReturn = new String(buffer,pos,limit);
			pos = limit;
			return toReturn;
		}
		
		if (stringLen >= 0) {
			String toReturn = new String(buffer,pos,stringLen);
			// now adjust pos.
			
			// CASE 1: last character read was a newline.
			if (buffer[stringLen+pos] == NEWLINE) {
				// this is fine whether we hit the end of the buffer or not.
				pos = pos + stringLen +1;
			}
			else if (buffer[stringLen+pos] == CARRIAGE_RETURN) {
				// Lines can end with CR, or CR/NEWLINE. 
				// If there is a newline, we should eat it.
				// first, if we're far from buffer end...
				if (stringLen+pos +1 < limit) {
					if( buffer[stringLen+pos+1] ==NEWLINE) {
						pos = stringLen+pos+2;
					}
				   else 
					   pos = stringLen+pos+1;
				} 
				else {
					pos = stringLen +1;
					// we'll need to eat the first character if it's a newline.
					eatFirstNewline = true;
				}
			}
			else {
				System.out.println("pos "+pos+" limit "+limit+" stringLen "+stringLen+" last char "+buffer[stringLen+pos]);
				throw new RuntimeException(" You have a bug.");
			}
			return toReturn;
		}
		else {
	//		System.out.println("pos "+pos+" limit "+limit+" inputDone "+inputDone);
			if (level > 5) {
				throw new RuntimeException("Broken");
			}
			// Can't make a line, need to fill up buffer.
			fillBuffer();
			return readLine(level+1);
		}
	 }
	  
	 public void close() {
		  try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	 
}
