package info.ipaw.pc3.PSLoadWorkflow;

import java.util.*;


public class LoadWorkflow {

		private static long START_TIME = 0;
  public static void main(String[] args) throws Exception {
    START_TIME = System.currentTimeMillis();
    LoadAppLogic.setStartTime(START_TIME);
    String JobID = args[0], CSVRootPath = args[1];
    
    twpc3Logger.logINFO("****************************************",true);  
    twpc3Logger.logINFO("***         START OF WORKFLOW        ***",true);  
    twpc3Logger.logINFO("****************************************",false);    
    
    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    twpc3Logger.logINFO("Step 1: running IsCSVReadyFileExists",false);
    twpc3Logger.logINFO("### Entering IsCSVReadyFileExists process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String CSVRootPath (VALUE:" + CSVRootPath + ")",false);     
    twpc3Logger.enterScope();
//    System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Running IsCSVReadyFileExists process");
    boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting IsCSVReadyFileExists process ###",true);      
    twpc3Logger.logINFO("RETURNED boolean IsCSVReadyFileExistsOutput (VALUE:" + IsCSVReadyFileExistsOutput + ")",false);        
    
    // 2. Control Flow: Decision
    twpc3Logger.logINFO("Step 2: Control Flow: Decision on IsCSVReadyFileExists (uses IsCSVReadyFileExistsOutput)",true);
    if (!IsCSVReadyFileExistsOutput) {
    	twpc3Logger.logFATAL("IsCSVReadyFileExists failed.  Halting now ...");
    	new RuntimeException("IsCSVReadyFileExists failed");
    } else {
    	twpc3Logger.logINFO("IsCSVReadyFileExists passed.  Continuing ...", false);
    }

    // 3. ReadCSVReadyFile
    twpc3Logger.logINFO("Step 3: running ReadCSVReadyFile",false);
    twpc3Logger.logINFO("### Entering ReadCSVReadyFile process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String CSVRootPath (VALUE:" + CSVRootPath + ")",false);  
    twpc3Logger.enterScope();
    List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting ReadCSVReadyFile process ###",true);      
    twpc3Logger.logINFO("RETURNED List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput (VALUE:" + ReadCSVReadyFileOutput + ")",false);   
    
    // 4. IsMatchCSVFileTables
    twpc3Logger.logINFO("Step 4: running IsMatchCSVFileTables",false);
    twpc3Logger.logINFO("### Entering IsMatchCSVFileTables process ###",true);    
    twpc3Logger.logINFO("PARAMETER: List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput (VALUE:" + ReadCSVReadyFileOutput + ")",false);  
    twpc3Logger.enterScope();
    boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting IsMatchCSVFileTables process ###",true);      
    twpc3Logger.logINFO("RETURNED boolean IsMatchCSVFileTablesOutput (VALUE:" + IsMatchCSVFileTablesOutput + ")",false);   

    
    // 5. Control Flow: Decision
    twpc3Logger.logINFO("Step 5: Control Flow: Decision on IsMatchCSVFileTables (uses IsMatchCSVFileTablesOutput)",true);
    if (!IsMatchCSVFileTablesOutput) {
    	twpc3Logger.logFATAL("IsMatchCSVFileTables failed.  Halting now ...");
    	new RuntimeException("IsMatchCSVFileTables failed");
    } else {
    	twpc3Logger.logINFO("IsMatchCSVFileTables passed.  Continuing ...", false);
    }

    // 6. CreateEmptyLoadDB
    twpc3Logger.logINFO("Step 6: running CreateEmptyLoadDB",false);
    twpc3Logger.logINFO("### Entering CreateEmptyLoadDB process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String JobID (VALUE:" + JobID + ")",false);  
    twpc3Logger.enterScope();
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting CreateEmptyLoadDB process ###",true);      
    twpc3Logger.logINFO("RETURNED LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput (VALUE:" + CreateEmptyLoadDBOutput + ")",false);   
    twpc3Logger.logINFO("=== Loop over each value in FileEntries ===",false);

    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    twpc3Logger.logINFO("Step 7: Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput",true);
    twpc3Logger.logINFO("=== Loop over each CSVFileEntry in ReadCSVReadyFileOutput ===",false);  
    twpc3Logger.enterScope();
    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput) {
		twpc3Logger.logINFO("--- Start Iteration ---",false);
		twpc3Logger.logINFO("Current value: " + FileEntry.FilePath,false);	
    	// ///////////////////////////////////
    	// //// Pre Load Validation //////
    	// ///////////////////////////////////
    	// 7.a. IsExistsCSVFile
		twpc3Logger.logINFO("Step 7a: Running IsExistsCSVFile",false);	
	    twpc3Logger.logINFO("### Entering IsExistsCSVFile process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry FileEntry (VALUE:" + FileEntry.FilePath + ")",false);  
        twpc3Logger.enterScope();
    	boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
        twpc3Logger.exitScope();
        twpc3Logger.logINFO("### Exiting IsExistsCSVFile process ###",true);      
        twpc3Logger.logINFO("RETURNED LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput (VALUE:" + CreateEmptyLoadDBOutput + ")",false);   
        
    	// 7.b. Control Flow: Decision
        twpc3Logger.logINFO("Step 7b: Control Flow: Decision on IsExistsCSVFile (uses IsExistsCSVFileOutput)",true);
        if (!IsExistsCSVFileOutput) {
        	twpc3Logger.logFATAL("IsExistsCSVFile failed.  Halting now ...");
        	new RuntimeException("IsExistsCSVFile failed");
        } else {
        	twpc3Logger.logINFO("IsExistsCSVFile passed.  Continuing ...", false);
        }

    	// 7.c. ReadCSVFileColumnNames
		twpc3Logger.logINFO("Step 7c: Running ReadCSVFileColumnNames",false);	
	    twpc3Logger.logINFO("### Entering ReadCSVFileColumnNames process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry FileEntry (VALUE:" + FileEntry.FilePath + ")",false);  
        twpc3Logger.enterScope();
	    LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
    		LoadAppLogic.ReadCSVFileColumnNames(FileEntry);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
	    twpc3Logger.logINFO("RETURNED CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);   


    	// 7.d. IsMatchCSVFileColumnNames
		twpc3Logger.logINFO("Step 7d: Running IsMatchCSVFileColumnNames",false);	
	    twpc3Logger.logINFO("### Entering IsMatchCSVFileColumnNames process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);  
        twpc3Logger.enterScope();
    	boolean IsMatchCSVFileColumnNamesOutput =
    		LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
	    twpc3Logger.logINFO("RETURNED CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);   

	    // 7.e. Control Flow: Decision
        twpc3Logger.logINFO("Step 7e: Control Flow: Decision on IsMatchCSVFileColumnNames (uses IsMatchCSVFileColumnNamesOutput)",true);
        if (!IsMatchCSVFileColumnNamesOutput) {
        	twpc3Logger.logFATAL("IsMatchCSVFileColumnNames failed.  Halting now ...");
        	new RuntimeException("IsMatchCSVFileColumnNames failed");
        } else {
        	twpc3Logger.logINFO("IsMatchCSVFileColumnNames passed.  Continuing ...", false);
        }


    	// ///////////////////////////////////
    	// //// Load File //////
    	// ///////////////////////////////////
    	// 7.f. LoadCSVFileIntoTable
		twpc3Logger.logINFO("Step 7f: Running LoadCSVFileIntoTable",false);	
	    twpc3Logger.logINFO("### Entering LoadCSVFileIntoTable process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry CreateEmptyLoadDBOutput (VALUE: "+ CreateEmptyLoadDBOutput + "), CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);  
        twpc3Logger.enterScope();
    	boolean LoadCSVFileIntoTableOutput =
    		LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting LoadCSVFileIntoTable process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean LoadCSVFileIntoTableOutput (VALUE:" + LoadCSVFileIntoTableOutput + ")",false);   

     	// 7.g. Control Flow: Decision
        twpc3Logger.logINFO("Step 7g: Control Flow: Decision on LoadCSVFileIntoTable (uses LoadCSVFileIntoTableOutput)",true);
        if (!IsMatchCSVFileColumnNamesOutput) {
        	twpc3Logger.logFATAL("LoadCSVFileIntoTable failed.  Halting now ...");
        	new RuntimeException("LoadCSVFileIntoTable failed");
        } else {
        	twpc3Logger.logINFO("LoadCSVFileIntoTable passed.  Continuing ...", false);
        }

    	// 7.h. UpdateComputedColumns
 		twpc3Logger.logINFO("Step 7h: Running UpdateComputedColumns",false);	
	    twpc3Logger.logINFO("### Entering UpdateComputedColumns process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry CreateEmptyLoadDBOutput (VALUE: "+ CreateEmptyLoadDBOutput + "), CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);  
        twpc3Logger.enterScope();
    	boolean UpdateComputedColumnsOutput =
    		LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting UpdateComputedColumns process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean UpdateComputedColumnsOutput (VALUE:" + UpdateComputedColumnsOutput + ")",false);   

    	// 7.i. Control Flow: Decision
        twpc3Logger.logINFO("Step 7i: Control Flow: Decision on UpdateComputedColumns (uses UpdateComputedColumnsOutput)",true);
        if (!IsMatchCSVFileColumnNamesOutput) {
        	twpc3Logger.logFATAL("UpdateComputedColumns failed.  Halting now ...");
        	new RuntimeException("UpdateComputedColumns failed");
        } else {
        	twpc3Logger.logINFO("UpdateComputedColumns passed.  Continuing ...", false);
        }


    	// ///////////////////////////////////
    	// //// PostLoad Validation //////
    	// ///////////////////////////////////
    	// 7.j. IsMatchTableRowCount
  		twpc3Logger.logINFO("Step 7j: Running IsMatchTableRowCount",false);	
	    twpc3Logger.logINFO("### Entering IsMatchTableRowCount process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry CreateEmptyLoadDBOutput (VALUE: "+ CreateEmptyLoadDBOutput + "), CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);  
        twpc3Logger.enterScope();
    	boolean IsMatchTableRowCountOutput =
    		LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting IsMatchTableRowCount process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean IsMatchTableRowCountOutput (VALUE:" + IsMatchTableRowCountOutput + ")",false);   

    	// 7.k. Control Flow: Decision
        twpc3Logger.logINFO("Step 7k: Control Flow: Decision on IsMatchTableRowCount (uses IsMatchTableRowCountOutput)",true);
        if (!IsMatchCSVFileColumnNamesOutput) {
        	twpc3Logger.logFATAL("IsMatchTableRowCount failed.  Halting now ...");
        	new RuntimeException("IsMatchTableRowCount failed");
        } else {
        	twpc3Logger.logINFO("IsMatchTableRowCount passed.  Continuing ...", false);
        }


    	// 7.l. IsMatchTableColumnRanges
   		twpc3Logger.logINFO("Step 7l: Running IsMatchTableColumnRanges",false);	
	    twpc3Logger.logINFO("### Entering IsMatchTableColumnRanges process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry CreateEmptyLoadDBOutput (VALUE: "+ CreateEmptyLoadDBOutput + "), CSVFileEntry ReadCSVFileColumnNamesOutput (VALUE:" + ReadCSVFileColumnNamesOutput + ")",false);  
        twpc3Logger.enterScope();
    	boolean IsMatchTableColumnRangesOutput =
    		LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
    				ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting IsMatchTableColumnRanges process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean IsMatchTableColumnRangesOutput (VALUE:" + IsMatchTableColumnRangesOutput + ")",false); 
	    
    	// 7.m. Control Flow: Decision
        twpc3Logger.logINFO("Step 7m: Control Flow: Decision on IsMatchTableColumnRanges (uses IsMatchTableColumnRangesOutput)",true);
        if (!IsMatchCSVFileColumnNamesOutput) {
        	twpc3Logger.logFATAL("IsMatchTableColumnRanges failed.  Halting now ...");
        	new RuntimeException("IsMatchTableColumnRanges failed");
        } else {
        	twpc3Logger.logINFO("IsMatchTableColumnRanges passed.  Continuing ...", false);
        }
		twpc3Logger.logINFO("--- End Iteration ---",false);
    }
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("=== Ending loop ===",false);  

    // 8. CompactDatabase
	twpc3Logger.logINFO("Step 8: Running CompactDatabase",false);	
	twpc3Logger.logINFO("### Entering CompactDatabase process ###",true);    
	twpc3Logger.logINFO("PARAMETER: DatabaseEntry CreateEmptyLoadDBOutput (VALUE: "+ CreateEmptyLoadDBOutput + ")",false);
    twpc3Logger.enterScope();
    LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting CompactDatabase process ###",true);      
    twpc3Logger.logINFO("RETURNED void",false); 

    twpc3Logger.logINFO("****************************************",true);  
    twpc3Logger.logINFO("***          END OF WORKFLOW         ***",true);  
    twpc3Logger.logINFO("****************************************",false);      
  }
}
