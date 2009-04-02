package info.ipaw.pc3.PSLoadWorkflow;

import info.ipaw.pc3.PSLoadWorkflow.LoadAppLogic.CSVFileEntry;

import java.util.*;


public class LoadWorkflow {

		private static long START_TIME = 0;
  public static void main(String[] args) throws Exception {
    START_TIME = System.currentTimeMillis();

    twpc3Logger.logINFO("****************************************",true);  
    twpc3Logger.logINFO("***         START OF WORKFLOW        ***",true);  
    twpc3Logger.logINFO("****************************************",false);    

    LoadAppLogic.setStartTime(START_TIME);
    LoadAppLogic.String_UUID JobID = new LoadAppLogic.String_UUID(args[0],UUID.randomUUID());
    twpc3Logger.logINFO("Passed Argument 1: JobID - String (ID:" + JobID.thisUUID + ", VALUE:" + JobID.thisString + ")",true);      
    LoadAppLogic.String_UUID CSVRootPath = new LoadAppLogic.String_UUID(args[1],UUID.randomUUID());
    twpc3Logger.logINFO("Passed Argument 2: CSVRootPath - String (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",true);  

    
    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    twpc3Logger.logINFO("Step 1: running IsCSVReadyFileExists",false);
    twpc3Logger.logINFO("### Entering IsCSVReadyFileExists process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String CSVRootPath (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",false);     
    twpc3Logger.enterScope();
    LoadAppLogic.Boolean_UUID IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting IsCSVReadyFileExists process ###",true);      
    twpc3Logger.logINFO("RETURNED boolean (ID:" + IsCSVReadyFileExistsOutput.thisUUID + ", VALUE:" + IsCSVReadyFileExistsOutput.thisBoolean + ")",false);        
    
    // 2. Control Flow: Decision
    twpc3Logger.logINFO("Step 2: Control Flow: Decision on IsCSVReadyFileExists (uses boolean " + IsCSVReadyFileExistsOutput.thisUUID + ")",true);
    if (!IsCSVReadyFileExistsOutput.thisBoolean) {
    	twpc3Logger.logFATAL("IsCSVReadyFileExists failed.  Halting now ...");
    	new RuntimeException("IsCSVReadyFileExists failed");
    } else {
    	twpc3Logger.logINFO("IsCSVReadyFileExists passed.  Continuing ...", false);
    }

    // 3. ReadCSVReadyFile
    twpc3Logger.logINFO("Step 3: running ReadCSVReadyFile",false);
    twpc3Logger.logINFO("### Entering ReadCSVReadyFile process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String CSVRootPath (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",false);     
    twpc3Logger.enterScope();
    LoadAppLogic.CSVFileEntryList ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting ReadCSVReadyFile process ###",true);      
    twpc3Logger.logINFO("RETURNED CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",false);   
    
    // 4. IsMatchCSVFileTables
    twpc3Logger.logINFO("Step 4: running IsMatchCSVFileTables",false);
    twpc3Logger.logINFO("### Entering IsMatchCSVFileTables process ###",true);    
    twpc3Logger.logINFO("PARAMETER: CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",false);  
    twpc3Logger.enterScope();
    LoadAppLogic.Boolean_UUID IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting IsMatchCSVFileTables process ###",true);      
    twpc3Logger.logINFO("RETURNED boolean IsMatchCSVFileTablesOutput (ID:" + IsMatchCSVFileTablesOutput.thisUUID + ", VALUE:" + IsMatchCSVFileTablesOutput.thisBoolean + ")",false);   

    
    // 5. Control Flow: Decision
    twpc3Logger.logINFO("Step 5: Control Flow: Decision on IsMatchCSVFileTables (uses boolean " + IsMatchCSVFileTablesOutput.thisUUID + ")",true);
    if (!IsMatchCSVFileTablesOutput.thisBoolean) {
    	twpc3Logger.logFATAL("IsMatchCSVFileTables failed.  Halting now ...");
    	new RuntimeException("IsMatchCSVFileTables failed");
    } else {
    	twpc3Logger.logINFO("IsMatchCSVFileTables passed.  Continuing ...", false);
    }

    // 6. CreateEmptyLoadDB
    twpc3Logger.logINFO("Step 6: running CreateEmptyLoadDB",false);
    twpc3Logger.logINFO("### Entering CreateEmptyLoadDB process ###",true);    
    twpc3Logger.logINFO("PARAMETER: String JobID (ID:" + JobID.thisUUID + ", VALUE:" + JobID.thisString + ")",false);     
    twpc3Logger.enterScope();
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);
    twpc3Logger.exitScope();
    twpc3Logger.logINFO("### Exiting CreateEmptyLoadDB process ###",true);      
    twpc3Logger.logINFO("RETURNED DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + ")",false);   
    twpc3Logger.logINFO("=== Loop over each value in FileEntries ===",false);

    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    twpc3Logger.logINFO("Step 7: Control Flow: Loop. ForEach CSVFileEntry in CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",true);
    twpc3Logger.logINFO("=== Start Loop ===",false);  
    twpc3Logger.enterScope();
    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput.thisCSVFileEntryList) {
		twpc3Logger.logINFO("--- Start Iteration ---",false);
		twpc3Logger.logINFO("Current value: " + FileEntry.FilePath,false);	
    	// ///////////////////////////////////
    	// //// Pre Load Validation //////
    	// ///////////////////////////////////
    	// 7.a. IsExistsCSVFile
		twpc3Logger.logINFO("Step 7a: Running IsExistsCSVFile",false);	
	    twpc3Logger.logINFO("### Entering IsExistsCSVFile process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + FileEntry.thisUUID + " PATH:" + FileEntry.FilePath + ")",false);  
        twpc3Logger.enterScope();
        LoadAppLogic.Boolean_UUID IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
        twpc3Logger.exitScope();
        twpc3Logger.logINFO("### Exiting IsExistsCSVFile process ###",true);      
        twpc3Logger.logINFO("RETURNED boolean (ID:" + IsExistsCSVFileOutput.thisUUID + ", VALUE:" + IsExistsCSVFileOutput.thisBoolean + ")",false);   
        
    	// 7.b. Control Flow: Decision
        twpc3Logger.logINFO("Step 7b: Control Flow: Decision on IsExistsCSVFile (uses boolean " + IsExistsCSVFileOutput.thisUUID + ")",true);
        if (!IsExistsCSVFileOutput.thisBoolean) {
        	twpc3Logger.logFATAL("IsExistsCSVFile failed.  Halting now ...");
        	new RuntimeException("IsExistsCSVFile failed");
        } else {
        	twpc3Logger.logINFO("IsExistsCSVFile passed.  Continuing ...", false);
        }

    	// 7.c. ReadCSVFileColumnNames
		twpc3Logger.logINFO("Step 7c: Running ReadCSVFileColumnNames",false);	
	    twpc3Logger.logINFO("### Entering ReadCSVFileColumnNames process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + FileEntry.thisUUID + " PATH:" + FileEntry.FilePath + ")",false);  
	    twpc3Logger.enterScope();
	    LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
    		LoadAppLogic.ReadCSVFileColumnNames(FileEntry);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
	    twpc3Logger.logINFO("RETURNED CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);  

    	// 7.d. IsMatchCSVFileColumnNames
		twpc3Logger.logINFO("Step 7d: Running IsMatchCSVFileColumnNames",false);	
	    twpc3Logger.logINFO("### Entering IsMatchCSVFileColumnNames process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);  
	    twpc3Logger.enterScope();
        LoadAppLogic.Boolean_UUID IsMatchCSVFileColumnNamesOutput =
    		LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
        twpc3Logger.logINFO("RETURNED boolean (ID:" + IsMatchCSVFileColumnNamesOutput.thisUUID + ", VALUE:" + IsMatchCSVFileColumnNamesOutput.thisBoolean + ")",false);   

	    // 7.e. Control Flow: Decision
        twpc3Logger.logINFO("Step 7e: Control Flow: Decision on IsMatchCSVFileColumnNames (uses boolean " + IsMatchCSVFileColumnNamesOutput.thisUUID + ")",true);
        if (!IsMatchCSVFileColumnNamesOutput.thisBoolean) {
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
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        twpc3Logger.enterScope();
        LoadAppLogic.Boolean_UUID LoadCSVFileIntoTableOutput =
    		LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting LoadCSVFileIntoTable process ###",true);      
        twpc3Logger.logINFO("RETURNED boolean (ID:" + LoadCSVFileIntoTableOutput.thisUUID + ", VALUE:" + LoadCSVFileIntoTableOutput.thisBoolean + ")",false);   

     	// 7.g. Control Flow: Decision
        twpc3Logger.logINFO("Step 7g: Control Flow: Decision on LoadCSVFileIntoTable (uses boolean " + LoadCSVFileIntoTableOutput.thisUUID + ")",true);
        if (!LoadCSVFileIntoTableOutput.thisBoolean) {
        	twpc3Logger.logFATAL("LoadCSVFileIntoTable failed.  Halting now ...");
        	new RuntimeException("LoadCSVFileIntoTable failed");
        } else {
        	twpc3Logger.logINFO("LoadCSVFileIntoTable passed.  Continuing ...", false);
        }

    	// 7.h. UpdateComputedColumns
 		twpc3Logger.logINFO("Step 7h: Running UpdateComputedColumns",false);	
	    twpc3Logger.logINFO("### Entering UpdateComputedColumns process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        twpc3Logger.enterScope();
        LoadAppLogic.Boolean_UUID UpdateComputedColumnsOutput =
    		LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting UpdateComputedColumns process ###",true);      
        twpc3Logger.logINFO("RETURNED boolean (ID:" + UpdateComputedColumnsOutput.thisUUID + ", VALUE:" + UpdateComputedColumnsOutput.thisBoolean + ")",false);   

    	// 7.i. Control Flow: Decision
        twpc3Logger.logINFO("Step 7i: Control Flow: Decision on UpdateComputedColumns (uses boolean " + UpdateComputedColumnsOutput.thisUUID + ")",true);

        if (!UpdateComputedColumnsOutput.thisBoolean) {
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
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        twpc3Logger.enterScope();
	    LoadAppLogic.Boolean_UUID IsMatchTableRowCountOutput =
    		LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting IsMatchTableRowCount process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean (ID:" + IsMatchTableRowCountOutput.thisUUID + ", VALUE:" + IsMatchTableRowCountOutput.thisBoolean + ")",false);   

    	// 7.k. Control Flow: Decision
	    twpc3Logger.logINFO("Step 7k: Control Flow: Decision on IsMatchTableRowCount (uses boolean " + IsMatchTableRowCountOutput.thisUUID + ")",true);
	    if (!IsMatchTableRowCountOutput.thisBoolean) {
        	twpc3Logger.logFATAL("IsMatchTableRowCount failed.  Halting now ...");
        	new RuntimeException("IsMatchTableRowCount failed");
        } else {
        	twpc3Logger.logINFO("IsMatchTableRowCount passed.  Continuing ...", false);
        }


    	// 7.l. IsMatchTableColumnRanges
   		twpc3Logger.logINFO("Step 7l: Running IsMatchTableColumnRanges",false);	
	    twpc3Logger.logINFO("### Entering IsMatchTableColumnRanges process ###",true);    
	    twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        twpc3Logger.enterScope();
        LoadAppLogic.Boolean_UUID IsMatchTableColumnRangesOutput =
    		LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
    				ReadCSVFileColumnNamesOutput);
	    twpc3Logger.exitScope();
	    twpc3Logger.logINFO("### Exiting IsMatchTableColumnRanges process ###",true);      
	    twpc3Logger.logINFO("RETURNED boolean (ID:" + IsMatchTableColumnRangesOutput.thisUUID + ", VALUE:" + IsMatchTableColumnRangesOutput.thisBoolean + ")",false);   
	    
    	// 7.m. Control Flow: Decision
	    twpc3Logger.logINFO("Step 7m: Control Flow: Decision on IsMatchTableColumnRanges (uses boolean " + IsMatchTableColumnRangesOutput.thisUUID + ")",true);
        if (!IsMatchTableColumnRangesOutput.thisBoolean) {
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
	twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:"+ CreateEmptyLoadDBOutput.thisUUID + ")",false);
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
