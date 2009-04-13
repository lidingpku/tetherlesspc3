package info.ipaw.pc3v0.PSLoadWorkflow;


import java.util.*;

import edu.rpi.tw.provenance.MyLogger;
import edu.rpi.tw.provenance.NameValue;


public class LoadWorkflow {

  public static void main(String[] args) {
		String szFunctionName="main";
	    String szSubFunctionName = "load";
	    
	    NameValue nv_args =new NameValue(szFunctionName, "args", args);
	    MyLogger.get().log1("input", szFunctionName, new NameValue []{nv_args});

		try {
		    MyLogger.get().log2("send", szFunctionName, szSubFunctionName,  new NameValue []{nv_args});
			
			load(args);

		    MyLogger.get().log2("receive", szFunctionName, szSubFunctionName, (NameValue) null);
		} catch (Exception e) {
		    NameValue nv_e =new NameValue(szFunctionName, "e", e);
		    MyLogger.get().log2("receive", szFunctionName, szSubFunctionName, new NameValue []{nv_e});
		}
		
	    MyLogger.get().log1("output", szFunctionName,  (NameValue[]) null);	  
  }
  
  public static void load(String[] args) throws Exception{
	String szFunctionName="load";
	
    NameValue nv_args =new NameValue(szFunctionName, "args", args);
    MyLogger.get().log1("input", szFunctionName, new NameValue []{nv_args});
    
    String JobID = args[0], CSVRootPath = args[1];
    
    NameValue nv_JobID =new NameValue(szFunctionName, "JobID", JobID);
    MyLogger.get().log_assign (szFunctionName, nv_JobID, nv_args);

    NameValue nv_CSVRootPath =new NameValue(szFunctionName, "CSVRootPath", CSVRootPath);
    MyLogger.get().log_assign (szFunctionName, nv_CSVRootPath, nv_args);
    
    
    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    
    String szSubFunctionName = "IsCSVReadyFileExists";
    MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CSVRootPath});
    
    boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    
    NameValue nv_IsCSVReadyFileExistsOutput =new NameValue(szFunctionName, "IsCSVReadyFileExistsOutput", IsCSVReadyFileExistsOutput);
    MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsCSVReadyFileExistsOutput});

    // 2. Control Flow: Decision
    MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsCSVReadyFileExistsOutput});

    if (!IsCSVReadyFileExistsOutput) throw new RuntimeException("IsCSVReadyFileExists failed");


    // 3. ReadCSVReadyFile
    szSubFunctionName = "ReadCSVReadyFile";
    MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CSVRootPath});
    
    List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);

    NameValue nv_ReadCSVReadyFileOutput = new NameValue(szFunctionName, "ReadCSVReadyFileOutput", ReadCSVReadyFileOutput);
    MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_ReadCSVReadyFileOutput});


    // 4. IsMatchCSVFileTables
    szSubFunctionName = "IsMatchCSVFileTables";
    MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_ReadCSVReadyFileOutput});

    boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    
    NameValue nv_IsMatchCSVFileTablesOutput = new NameValue(szFunctionName, "IsMatchCSVFileTablesOutput", IsMatchCSVFileTablesOutput);
    MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsMatchCSVFileTablesOutput});
    
    // 5. Control Flow: Decision
    MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsMatchCSVFileTablesOutput});

    if (!IsMatchCSVFileTablesOutput) throw new RuntimeException("IsMatchCSVFileTables failed");



    // 6. CreateEmptyLoadDB
    szSubFunctionName = "CreateEmptyLoadDB";
    MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_JobID});
    
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);

    NameValue nv_CreateEmptyLoadDBOutput = new NameValue(szFunctionName, "CreateEmptyLoadDBOutput", CreateEmptyLoadDBOutput);
    MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_CreateEmptyLoadDBOutput});

    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    int cnt=0;
	szSubFunctionName = "for";
    MyLogger.get().log1("for-input", szFunctionName, (NameValue[]) null);

    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput) {

    	cnt ++;

    	szFunctionName ="for"+cnt;
        MyLogger.get().log1("input", szFunctionName, (NameValue[]) null);

        NameValue nv_FileEntry = new NameValue(szFunctionName, "FileEntry", FileEntry);
        MyLogger.get().log_assign (szFunctionName, nv_FileEntry, nv_ReadCSVReadyFileOutput);
        
      // ///////////////////////////////////
      // //// Pre Load Validation //////
      // ///////////////////////////////////
      // 7.a. IsExistsCSVFile
      szSubFunctionName = "IsExistsCSVFile";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_FileEntry});
        
      boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
      
      NameValue nv_IsExistsCSVFileOutput = new NameValue(szFunctionName, "IsExistsCSVFileOutput", IsExistsCSVFileOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsExistsCSVFileOutput});
      
      // 7.b. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsExistsCSVFileOutput});

      if (!IsExistsCSVFileOutput) throw new RuntimeException("IsExistsCSVFile failed");


      // 7.c. ReadCSVFileColumnNames
      szSubFunctionName = "ReadCSVFileColumnNames";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_FileEntry});
      
      LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
          LoadAppLogic.ReadCSVFileColumnNames(FileEntry);

      NameValue nv_ReadCSVFileColumnNamesOutput = new NameValue(szFunctionName, "ReadCSVFileColumnNamesOutput", ReadCSVFileColumnNamesOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_ReadCSVFileColumnNamesOutput});

      // 7.d. IsMatchCSVFileColumnNames
      szSubFunctionName = "IsMatchCSVFileColumnNames";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_ReadCSVFileColumnNamesOutput});

      boolean IsMatchCSVFileColumnNamesOutput =
          LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
      
      NameValue nv_IsMatchCSVFileColumnNamesOutput = new NameValue(szFunctionName, "IsMatchCSVFileColumnNamesOutput", IsMatchCSVFileColumnNamesOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsMatchCSVFileColumnNamesOutput});
      
      // 7.e. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsMatchCSVFileColumnNamesOutput});

      if (!IsMatchCSVFileColumnNamesOutput)
        throw new RuntimeException("IsMatchCSVFileColumnNames failed");


      // ///////////////////////////////////
      // //// Load File //////
      // ///////////////////////////////////
      // 7.f. LoadCSVFileIntoTable
      szSubFunctionName = "LoadCSVFileIntoTable";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CreateEmptyLoadDBOutput, nv_ReadCSVFileColumnNamesOutput});

      boolean LoadCSVFileIntoTableOutput =
          LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
      
      NameValue nv_LoadCSVFileIntoTableOutput = new NameValue(szFunctionName, "LoadCSVFileIntoTableOutput", LoadCSVFileIntoTableOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_LoadCSVFileIntoTableOutput});

      // 7.g. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_LoadCSVFileIntoTableOutput});

      if (!LoadCSVFileIntoTableOutput) throw new RuntimeException("LoadCSVFileIntoTable failed");


      // 7.h. UpdateComputedColumns
      szSubFunctionName = "UpdateComputedColumns";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CreateEmptyLoadDBOutput, nv_ReadCSVFileColumnNamesOutput});

      boolean UpdateComputedColumnsOutput =
          LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
      
      NameValue nv_UpdateComputedColumnsOutput = new NameValue(szFunctionName, "UpdateComputedColumnsOutput", UpdateComputedColumnsOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_UpdateComputedColumnsOutput});
      
      // 7.i. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_UpdateComputedColumnsOutput});

      if (!UpdateComputedColumnsOutput) throw new RuntimeException("UpdateComputedColumns failed");


      // ///////////////////////////////////
      // //// PostLoad Validation //////
      // ///////////////////////////////////
      // 7.j. IsMatchTableRowCount
      szSubFunctionName = "IsMatchTableRowCount";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CreateEmptyLoadDBOutput, nv_ReadCSVFileColumnNamesOutput});
      
      boolean IsMatchTableRowCountOutput =
          LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);

      NameValue nv_IsMatchTableRowCountOutput = new NameValue(szFunctionName, "IsMatchTableRowCountOutput", IsMatchTableRowCountOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsMatchTableRowCountOutput});
      
      // 7.k. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsMatchTableRowCountOutput});

      if (!IsMatchTableRowCountOutput) throw new RuntimeException("IsMatchTableRowCount failed");


      // 7.l. IsMatchTableColumnRanges
      szSubFunctionName = "IsMatchTableColumnRanges";
      MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CreateEmptyLoadDBOutput, nv_ReadCSVFileColumnNamesOutput});

      boolean IsMatchTableColumnRangesOutput =
          LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
              ReadCSVFileColumnNamesOutput);

      NameValue nv_IsMatchTableColumnRangesOutput = new NameValue(szFunctionName, "IsMatchTableColumnRangesOutput", IsMatchTableColumnRangesOutput);
      MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  new NameValue []{nv_IsMatchTableColumnRangesOutput});
      
      // 7.m. Control Flow: Decision
      MyLogger.get().log1("check", szFunctionName, new NameValue []{nv_IsMatchTableColumnRangesOutput});

      if (!IsMatchTableColumnRangesOutput)
        throw new RuntimeException("IsMatchTableColumnRanges failed");

      MyLogger.get().log1("ouput", szFunctionName, (NameValue[]) null);
    }
    
    szFunctionName ="for";
    MyLogger.get().log1("for-output", szFunctionName, (NameValue[]) null);

    szFunctionName ="load";

    // 8. CompactDatabase
    szSubFunctionName = "CompactDatabase";
    MyLogger.get().log2("send", szFunctionName, szSubFunctionName, new NameValue []{nv_CreateEmptyLoadDBOutput});
    
    LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);
    
    MyLogger.get().log2("receive",szFunctionName, szSubFunctionName,  (NameValue) null);
    
    MyLogger.get().log1("output", szFunctionName,  (NameValue[]) null);
  }
}
