package apps;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.file.FileStreams;
import com.ibm.streamsx.topology.function7.Consumer;
import com.ibm.streamsx.topology.function7.Function;


public class ReadAnonWrite {

	
	public static void main(String argv[]) throws Exception{
		String contextType = "STANDALONE";
		if (argv.length>0) {
		 contextType = argv[0];
		}
		//String fileName = "infile.txt";
		String fileName = "/homes/hny2/hildrum/Netro_6021_10m.log";
		 Topology flow = new Topology("ReadAnonWrite");
		 TStream<String> filenamestream = flow.strings(fileName);
		 TStream<String> lines = FileStreams.textFileReader(filenamestream);
		  final SecretKeySpec key = new SecretKeySpec(new byte[16], "AES");
		 
		
		  TStream<String> 
			  processed = lines.transform(new Function<String,String>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				class AnonimizedRecord {
					 // The toCSVString function would be a lot simpler if I stored these in an array.
					// Not sure which is bette.r 
					 String PDPContextID =null;  // encrypted
					 String ID_NETRO_START_TIMESTAMP=null; //HT2
				     String ID_NETRO_END_TIMESTAMP=null; //HT3
				     String userId=null; //HT 4
				     String TACFAC=null;  // HT5
				     String locationCGI=null;  // HT6
				     private final String consentStatus="1"; // HT7
				     String hostName=null; // HT8
				     private final String hostAnonStatus="0"; // HT9
				     String hostIP=null; //HT10
				     String clientIP=null; //HT11
				     String hostTCPPort=null; //HT12
				     String clientTCPPort=null;//HT13
				     String uplinkBytes=null; //HT15
				     String downlinkBytes=null; //HT16
				     
				     String toCSVString(String sep) {
				    	 StringBuilder toReturn = new StringBuilder();
				    	 toReturn.append(PDPContextID).append(sep);
				    	 toReturn.append(ID_NETRO_START_TIMESTAMP).append(sep);
				    	 toReturn.append(ID_NETRO_END_TIMESTAMP).append(sep);
				    	 toReturn.append(userId).append(sep);
				    	 toReturn.append(TACFAC).append(sep);
				    	 toReturn.append(locationCGI).append(sep);
				    	 toReturn.append(consentStatus).append(sep);
				    	 toReturn.append(hostName).append(sep);
				    	 toReturn.append(hostAnonStatus).append(sep);
				    	 toReturn.append(hostIP).append(sep);
				    	 toReturn.append(clientIP).append(sep);
				    	 toReturn.append(hostTCPPort).append(sep);
				    	 toReturn.append(clientTCPPort).append(sep);
				    	 toReturn.append(uplinkBytes).append(sep);
				    	 toReturn.append(downlinkBytes);
				    	 
				    	 return toReturn.toString();
				     }
				     
				}

			 
			 static final int ID_GTP_PDP_CONTEXT_CREATION_TIMESTAMP_index = 51;
			 static final int  ID_GTP_IMSI_index = 19;
			 static final int  ID_NETRO_START_TIMESTAMP_index = 4;
			 static final int  ID_NETRO_END_TIMESTAMP_index = 5;
			 static final int ID_GTP_MSISDN_index = 18;
			 static final int ID_GTP_IMEISV_index = 48;
			 static final int ID_GTP_MCC_index = 53;
			 static final int  ID_GTP_MNC_index = 54;
			 static final int  ID_GTP_CELL_LAC_index = 49;
             static final int ID_NETRO_HOSTNAME_index = 45;
             static final int ID_SERVER_IP_ADDRESS_index = 13;
             static final int ID_CLIENT_IP_ADDRESS_index  =12;
             static final int  ID_SERVER_PORT_NUMBER_index = 17;
             static final int ID_CLIENT_PORT_NUMBER_index= 16;
             static final int ID_NETRO_ALL_BYTES_UP_index = 23;
             static final int ID_NETRO_ALL_BYTES_DOWN_index = 24;

			 
			 @Override
			 public String apply(String v) {
				 try {
				 String field[] = v.split("\\|",-1);
				 AnonimizedRecord r = new AnonimizedRecord();
				 r.PDPContextID = AES_CMAC.asHexString(AES_CMAC.compute(key,field[ID_GTP_PDP_CONTEXT_CREATION_TIMESTAMP_index]+field[ID_GTP_IMSI_index]));
		 		 r.ID_NETRO_START_TIMESTAMP = field[ID_NETRO_START_TIMESTAMP_index];
		 		 r.ID_NETRO_END_TIMESTAMP = field[ID_NETRO_END_TIMESTAMP_index];
		 		 r.userId = AES_CMAC.asHexString(AES_CMAC.compute(key,field[ID_GTP_MSISDN_index]));
		 		 String tmpFAC = field[ID_GTP_IMEISV_index];
		 		 if (tmpFAC != null)  {
		 			 if (tmpFAC.length() >= 8)
		 				 r.TACFAC = tmpFAC.substring(0,8);
		 			 else {
		 				 r.TACFAC = tmpFAC;
		 			 }
		 		 }
		 		 else {
		 			 r.TACFAC=null;
		 		 }
		 		 
		 		 r.locationCGI = AES_CMAC.asHexString(AES_CMAC.compute(key,field[ID_GTP_MCC_index]+field[ID_GTP_MNC_index]+field[ID_GTP_CELL_LAC_index]));
		 		 r.hostName = field[ID_NETRO_HOSTNAME_index];
		 		 r.hostIP = field[ID_SERVER_IP_ADDRESS_index];
		 		 r.clientIP = field[ID_CLIENT_IP_ADDRESS_index];
		 		 r.hostTCPPort = field[ID_SERVER_PORT_NUMBER_index];
		 		 r.clientTCPPort = field[ID_CLIENT_PORT_NUMBER_index];
		 		 r.uplinkBytes = field[ID_NETRO_ALL_BYTES_UP_index];
		 		 r.downlinkBytes = field[ID_NETRO_ALL_BYTES_DOWN_index];
		 		 return r.toCSVString(",");
				 }
				 catch (Exception c) {
					 return "error";
				 }
			 }},String.class);
		  
		  
		  // Need to getupdated version so I can close the file
		 processed.sink(new Consumer<String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			transient BufferedWriter writer = null;
			private final String outfilename = "outfile.txt";
			
			 
			 @Override
			public void accept(String toWrite) {
				 try {
					 if (writer == null) {
						 final Charset charset = Charset.forName("US-ASCII");
						 writer = Files.newBufferedWriter((new File("outfile.txt")).toPath(), charset);
					 }
 				 writer.write(toWrite);
				 writer.newLine();
				 }
				 catch (IOException e) {
					e.printStackTrace();
				 }
			 } 
		 });
		  
		long before =  System.currentTimeMillis();
		 StreamsContextFactory.getStreamsContext(contextType).submit(flow).get(); 
		 long after = System.currentTimeMillis();
		 System.out.println("Duration: "+ (double)(after-before)/1000.0);

	}
}
