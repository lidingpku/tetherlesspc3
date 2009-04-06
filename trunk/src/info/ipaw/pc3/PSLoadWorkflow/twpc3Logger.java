package info.ipaw.pc3.PSLoadWorkflow;

public class twpc3Logger {
	static long 	START_TIME = System.currentTimeMillis();
	static int 		PROC_COUNT;
	static int		SCOPE_LEVEL = 0;
	
	private static long getCurrentSysTime() {
		return System.currentTimeMillis() - START_TIME;
	}
	
	private static int getCurrentLogTime() {
		return PROC_COUNT++;
	}
	
	private static String constructLogStr(String msg) {
		String beginning = "<REALTIME> " + getCurrentSysTime() + "\t   <LINENUM> " + getCurrentLogTime() + " \t | ";
		String middle = "";
		String end = msg;
		for(int i = 0; i < SCOPE_LEVEL; i++)
			middle = middle + "\t";
		
		return beginning+middle+end;
	}
	
	public static void logINFO (String msg, boolean waitForNewLine) {
		System.out.println("[INFO]  " + constructLogStr(msg));
		if(!waitForNewLine)
			System.out.println("[INFO]  " + constructLogStr(""));
	}

	public static void logFATAL (String msg) {
		System.out.println("[FATAL] " + constructLogStr(msg));
	}

	
	public static void logERROR (String msg) {
		System.out.println("[ERROR] " + constructLogStr(msg));
	}
	
	
	public static void enterScope() {
		SCOPE_LEVEL++;
	}
	public static void exitScope() {
		SCOPE_LEVEL--;
	}
	
}
