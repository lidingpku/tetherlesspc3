package info.ipaw.pc3.PSLoadWorkflow;

import java.util.*;


public class LoadWorkflow {

		private static long START_TIME = 0;
  public static void main(String[] args) throws Exception {
    START_TIME = System.currentTimeMillis();
    LoadAppLogic.setStartTime(START_TIME);
    String JobID = args[0], CSVRootPath = args[1];
    
    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running IsCSVReadyFileExists process");
    boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "IsCSVReadyFileExists evaluates to " + IsCSVReadyFileExistsOutput);
    
    // 2. Control Flow: Decision
    if (!IsCSVReadyFileExistsOutput) throw new RuntimeException("IsCSVReadyFileExists failed");


    // 3. ReadCSVReadyFile
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running ReadCSVReadyFile process");
    List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);

    // 4. IsMatchCSVFileTables
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running IsMatchCSVFileTablesOutput process");
    boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "IsMatchCSVFileTablesOutput evaluates to " + IsCSVReadyFileExistsOutput);
    // 5. Control Flow: Decision
    if (!IsMatchCSVFileTablesOutput) throw new RuntimeException("IsMatchCSVFileTables failed");


    // 6. CreateEmptyLoadDB
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running CreateEmptyLoadDBOutput process");
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);


    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Now, looping over each CSVFileEntry in ReadCSVReadyFileOutput and processing");
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Entering loop");
    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput) {
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t##############################");
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNow on FileEntry " + FileEntry.FilePath);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t##############################");
    	// ///////////////////////////////////
    	// //// Pre Load Validation //////
    	// ///////////////////////////////////
    	// 7.a. IsExistsCSVFile
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning IsExistsCSVFile");
    	boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsExistsCSVFile evaluates to " + IsExistsCSVFileOutput);
    	// 7.b. Control Flow: Decision
    	if (!IsExistsCSVFileOutput) throw new RuntimeException("IsExistsCSVFile failed");


    	// 7.c. ReadCSVFileColumnNames
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning ReadCSVFileColumnNamesOutput");
    	LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
    		LoadAppLogic.ReadCSVFileColumnNames(FileEntry);


    	// 7.d. IsMatchCSVFileColumnNames
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning IsMatchCSVFileColumnNamesOutput");
    	boolean IsMatchCSVFileColumnNamesOutput =
    		LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchCSVFileColumnNamesOutput evaluates to " + IsMatchCSVFileColumnNamesOutput);
    	// 7.e. Control Flow: Decision
    	if (!IsMatchCSVFileColumnNamesOutput)
    		throw new RuntimeException("IsMatchCSVFileColumnNames failed");


    	// ///////////////////////////////////
    	// //// Load File //////
    	// ///////////////////////////////////
    	// 7.f. LoadCSVFileIntoTable
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning LoadCSVFileIntoTableOutput");
    	boolean LoadCSVFileIntoTableOutput =
    		LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tLoadCSVFileIntoTableOutput evaluates to " + LoadCSVFileIntoTableOutput);
    	// 7.g. Control Flow: Decision
    	if (!LoadCSVFileIntoTableOutput) throw new RuntimeException("LoadCSVFileIntoTable failed");


    	// 7.h. UpdateComputedColumns
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning UpdateComputedColumnsOutput");
    	boolean UpdateComputedColumnsOutput =
    		LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdateComputedColumnsOutput evaluates to " + UpdateComputedColumnsOutput);
    	// 7.i. Control Flow: Decision
    	if (!UpdateComputedColumnsOutput) throw new RuntimeException("UpdateComputedColumns failed");


    	// ///////////////////////////////////
    	// //// PostLoad Validation //////
    	// ///////////////////////////////////
    	// 7.j. IsMatchTableRowCount
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning IsMatchTableRowCount");
    	boolean IsMatchTableRowCountOutput =
    		LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchTableRowCount evaluates to " + IsMatchTableRowCountOutput);
    	// 7.k. Control Flow: Decision
    	if (!IsMatchTableRowCountOutput) throw new RuntimeException("IsMatchTableRowCount failed");


    	// 7.l. IsMatchTableColumnRanges
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRunning IsMatchTableColumnRanges");
    	boolean IsMatchTableColumnRangesOutput =
    		LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
    				ReadCSVFileColumnNamesOutput);
    	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchTableColumnRanges evaluates to " + IsMatchTableColumnRangesOutput);
    	// 7.m. Control Flow: Decision
    	if (!IsMatchTableColumnRangesOutput)
    		throw new RuntimeException("IsMatchTableColumnRanges failed");

    }
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Ending loop");

    // 8. CompactDatabase
	System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running CompactDatabase");
    LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);
    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "END OF WORKFLOW");
  }
}
