package app;

	public class SummaryStats  implements java.io.Serializable{
		private static final long serialVersionUID = 1L;
		final int total;
		final int rec6022;
		final int rec6013;
		final int rec6021;
		
		SummaryStats(int t, int r13, int r21, int r22) {
			total = t;
			rec6022=r22;
			rec6013=r13;
			rec6021 = r21;
		}
		
		public String toString() {
			return total+" = "+rec6013+" + "+rec6021+" + "+rec6022;
		}
	}
