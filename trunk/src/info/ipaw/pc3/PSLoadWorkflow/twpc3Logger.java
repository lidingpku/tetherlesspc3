package info.ipaw.pc3.PSLoadWorkflow;

import org.apache.log4j.*;

public class twpc3Logger {
	static long 	START_TIME = System.currentTimeMillis();
	static int 		PROC_COUNT;
	static Logger  	logger = Logger.getLogger("twpc3Logger");
	static int		SCOPE_LEVEL = 0;
	
	public static void setLoggingLevel(Level l) {
		logger.setLevel(l);
	}
	
	private static long getCurrentSysTime() {
		return System.currentTimeMillis() - START_TIME;
	}
	
	private static int getCurrentLogTime() {
		return PROC_COUNT++;
	}
	
	private static String constructLogStr(String msg) {
		String beginning = "<LOGTIME> " + getCurrentLogTime() + " \t | ";
		String middle = "";
		String end = msg;
		for(int i = 0; i < SCOPE_LEVEL; i++)
			middle = middle + "\t";
		
		return beginning+middle+end;
	}
	
	public static void logINFO (String msg, boolean waitForNewLine) {
		logger.info(constructLogStr(msg));
		if(!waitForNewLine)
			logger.info(constructLogStr(""));
	}
	
	public static void logFATAL (String msg) {
		logger.fatal(constructLogStr(msg));
	}
	
	
	public static void enterScope() {
		SCOPE_LEVEL++;
	}
	public static void exitScope() {
		SCOPE_LEVEL--;
	}
	
}
