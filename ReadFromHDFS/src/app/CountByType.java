package app;

import com.ibm.streamsx.topology.function.Function;

public class CountByType implements Function<String,SummaryStats> {
				/**
				 * 
				 */
				CountByType() {
					
					System.out.println("*****I live!");
				}
				private static final long serialVersionUID = 1L;
				int numRecords =0;
				int rec6022 = 0;
				int rec6021 = 0;
				int rec6013 = 0;
				@Override
				public synchronized SummaryStats apply(String v) {
					
					if (numRecords==0) {
						System.out.println("*****Recalled to life.");  // dickens reference
					}
					numRecords++;
					if (v.startsWith("6022") ) {
						rec6022++;
					}
					else if (v.startsWith("6021")) {
						rec6021++;
					}
					else if (v.startsWith("6013")) {
						rec6013++;
					}
					else if (v.startsWith("done")) {
						numRecords--;
						SummaryStats s = new SummaryStats(numRecords,rec6013,rec6021,rec6022);
						numRecords = 0;
						rec6022=0;
						rec6021=0;
						rec6013=0;
						return s;
					}
					else {
						System.out.println("Malformed line: "+v);
						
					}
					return null;
	 			}
}
