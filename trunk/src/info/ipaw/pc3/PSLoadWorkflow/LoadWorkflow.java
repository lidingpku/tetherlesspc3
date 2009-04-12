package info.ipaw.pc3.PSLoadWorkflow;

import info.ipaw.pc3.PSLoadWorkflow.LoadAppLogic.CSVFileEntry;
import info.ipaw.pc3.PSLoadWorkflow.LoadAppLogic.CSVFileEntryList;

import java.util.*;


public class LoadWorkflow {

		private static long START_TIME = 0;
  public static void main(String[] args) throws Exception {
    START_TIME = System.currentTimeMillis();

    //twpc3Logger.logINFO("****************************************",true);  
    //twpc3Logger.logINFO("***         START OF WORKFLOW        ***",true);  
    //twpc3Logger.logINFO("****************************************",false);    

    LoadAppLogic.setStartTime(START_TIME);
    LoadAppLogic.String_UUID JobID = new LoadAppLogic.String_UUID(args[0],UUID.randomUUID());
    //--\\
    UUID JobID_InfStep_UUID = ProtoPML.addNewProtoInfStep("assert", "User", null);
    UUID JobID_NS_UUID = ProtoPML.addNewProtoNS("JobID-"+args[0], "assert_JobID", JobID_InfStep_UUID);
    //--//
    //twpc3Logger.logINFO("Passed Argument 1: JobID - String (ID:" + JobID.thisUUID + ", VALUE:" + JobID.thisString + ")",true);      
    
    LoadAppLogic.String_UUID CSVRootPath = new LoadAppLogic.String_UUID(args[1],UUID.randomUUID());
    //--\\
    UUID CSVRootPath_InfStep_UUID = ProtoPML.addNewProtoInfStep("assert", "User", null);
    UUID CSVRootPath_NS_UUID = ProtoPML.addNewProtoNS("CSVRootPath-"+args[1], "assert", CSVRootPath_InfStep_UUID);
    //--//
    //twpc3Logger.logINFO("Passed Argument 2: CSVRootPath - String (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",true);  

    
    // ///////////////////////////////////
    // //// Batch Initialization //////
    // ///////////////////////////////////
    // 1. IsCSVReadyFileExists
    //twpc3Logger.logINFO("Step 1: running IsCSVReadyFileExists",false);
    //twpc3Logger.logINFO("### Entering IsCSVReadyFileExists process ###",true);    
    //twpc3Logger.logINFO("PARAMETER: String CSVRootPath (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",false);     
    //twpc3Logger.enterScope();
    //--\\
    List<UUID> IsCSVReadyFileExists_InfStep_Antecedents = new ArrayList<UUID>();
    IsCSVReadyFileExists_InfStep_Antecedents.add(CSVRootPath_NS_UUID);
    UUID IsCSVReadyFileExists_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsCSVReadyFileExists", "System", IsCSVReadyFileExists_InfStep_Antecedents);    
    boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
    UUID IsCSVReadyFileExists_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsCSVReadyFileExistsOutput, "IsCSVReadyFileExists", IsCSVReadyFileExists_InfStep_UUID);
    //--//
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("### Exiting IsCSVReadyFileExists process ###",true);      
    //twpc3Logger.logINFO("RETURNED (ID:" + IsCSVReadyFileExists_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsCSVReadyFileExists_NS_UUID) + ")",false);        
    
    // 2. Control Flow: Decision
    //twpc3Logger.logINFO("Step 2: Control Flow: Decision on IsCSVReadyFileExists uses (ID:" + IsCSVReadyFileExists_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsCSVReadyFileExists_NS_UUID) + ")",true);
    
    //--\\
    List<UUID> Check_IsCSVReadyFileExists_InfStep_Antecedents = new ArrayList<UUID>();
    Check_IsCSVReadyFileExists_InfStep_Antecedents.add(IsCSVReadyFileExists_NS_UUID);
    UUID Check_IsCSVReadyFileExists_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsCSVReadyFileExists_InfStep_Antecedents);    
    if (!IsCSVReadyFileExistsOutput) {
    	//twpc3Logger.logFATAL("IsCSVReadyFileExists failed.  Halting now ...");
    	new RuntimeException("IsCSVReadyFileExists failed");
    } else {
    	//twpc3Logger.logINFO("IsCSVReadyFileExists passed.  Continuing ...", false);
    }
    UUID Check_IsCSVReadyFileExists_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsCSVReadyFileExistsOutput, "Check-IsCSVReadyFileExists", Check_IsCSVReadyFileExists_InfStep_UUID);
    //--//
    
    // 3. ReadCSVReadyFile
    //twpc3Logger.logINFO("Step 3: running ReadCSVReadyFile",false);
    //twpc3Logger.logINFO("### Entering ReadCSVReadyFile process ###",true);    
    //twpc3Logger.logINFO("PARAMETER: String CSVRootPath (ID:" + CSVRootPath.thisUUID + ", VALUE:" + CSVRootPath.thisString + ")",false);     
    //twpc3Logger.enterScope();
    //--\\
    List<UUID> ReadCSVReadyFile_Antecedents = new ArrayList<UUID>();
    ReadCSVReadyFile_Antecedents.add(CSVRootPath_NS_UUID);    
    ReadCSVReadyFile_Antecedents.add(Check_IsCSVReadyFileExists_NS_UUID);
    UUID ReadCSVReadyFile_Antecedents_InfStep_UUID = ProtoPML.addNewProtoInfStep("ReadCSVReadyFile", "System", ReadCSVReadyFile_Antecedents);    
    LoadAppLogic.CSVFileEntryList ReadCSVReadyFileOutput =
        LoadAppLogic.ReadCSVReadyFile(CSVRootPath);
    UUID ReadCSVReadyFile_NS_UUID = ProtoPML.addNewProtoNS("List<CSVFileEntry>", "ReadCSVReadyFile", ReadCSVReadyFile_Antecedents_InfStep_UUID);
    //--//
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("### Exiting ReadCSVReadyFile process ###",true);      
    //twpc3Logger.logINFO("RETURNED CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",false);   
    
    // 4. IsMatchCSVFileTables
    //twpc3Logger.logINFO("Step 4: running IsMatchCSVFileTables",false);
    //twpc3Logger.logINFO("### Entering IsMatchCSVFileTables process ###",true);    
    //twpc3Logger.logINFO("PARAMETER: CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",false);  
    //twpc3Logger.enterScope();
    //--\\
    List<UUID> IsMatchCSVFileTablesOutput_Antecedents = new ArrayList<UUID>();
    IsMatchCSVFileTablesOutput_Antecedents.add(ReadCSVReadyFile_NS_UUID);        
    UUID IsMatchCSVFileTablesOutput_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsMatchCSVFileTablesOutput", "System", IsMatchCSVFileTablesOutput_Antecedents);    
    boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);
    UUID IsMatchCSVFileTablesOutput_NS_UUID = ProtoPML.addNewProtoNS("boolean-" +IsMatchCSVFileTablesOutput, "IsMatchCSVFileTablesOutput", IsMatchCSVFileTablesOutput_InfStep_UUID);
    //--//
    
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("### Exiting IsMatchCSVFileTables process ###",true);      
    //twpc3Logger.logINFO("RETURNED (ID:" + IsMatchCSVFileTablesOutput_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchCSVFileTablesOutput_NS_UUID) + ")",false);        
    
    
    // 5. Control Flow: Decision
    //twpc3Logger.logINFO("Step 5: Control Flow: Decision on IsMatchCSVFileTables uses (ID:" + IsMatchCSVFileTablesOutput_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchCSVFileTablesOutput_NS_UUID) + ")",true);

    //--\\
    List<UUID> Check_IsMatchCSVFileTablesOutput_InfStep_Antecedents = new ArrayList<UUID>();
    Check_IsMatchCSVFileTablesOutput_InfStep_Antecedents.add(IsMatchCSVFileTablesOutput_NS_UUID);
    UUID Check_IsMatchCSVFileTablesOutput_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsMatchCSVFileTablesOutput_InfStep_Antecedents);    
    if (!IsMatchCSVFileTablesOutput) {
    	//twpc3Logger.logFATAL("IsMatchCSVFileTables failed.  Halting now ...");
    	new RuntimeException("IsMatchCSVFileTables failed");
    } else {
    	//twpc3Logger.logINFO("IsMatchCSVFileTables passed.  Continuing ...", false);
    }
    UUID Check_IsMatchCSVFileTablesOutput_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsCSVReadyFileExistsOutput, "Check-IsMatchCSVFileTables", Check_IsMatchCSVFileTablesOutput_InfStep_UUID);
    //--//

    // 6. CreateEmptyLoadDB
    //twpc3Logger.logINFO("Step 6: running CreateEmptyLoadDB",false);
    //twpc3Logger.logINFO("### Entering CreateEmptyLoadDB process ###",true);    
    //twpc3Logger.logINFO("PARAMETER: String JobID (ID:" + JobID.thisUUID + ", VALUE:" + JobID.thisString + ")",false);     
    //twpc3Logger.enterScope();

    //--\\
    List<UUID> CreateEmptyLoadDB_InfStep_Antecedents = new ArrayList<UUID>();
    CreateEmptyLoadDB_InfStep_Antecedents.add(JobID_NS_UUID);    
    CreateEmptyLoadDB_InfStep_Antecedents.add(Check_IsMatchCSVFileTablesOutput_NS_UUID);
    UUID CreateEmptyLoadDB_InfStep_InfStep_UUID = ProtoPML.addNewProtoInfStep("CreateEmptyLoadDB", "System", CreateEmptyLoadDB_InfStep_Antecedents);    
    LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);
    UUID CreateEmptyLoadDB_InfStep_NS_UUID = ProtoPML.addNewProtoNS("DatabaseEntry", "CreateEmptyLoadDB", CreateEmptyLoadDB_InfStep_InfStep_UUID);
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("### Exiting CreateEmptyLoadDB process ###",true);      
    //twpc3Logger.logINFO("RETURNED DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + ")",false);   
    //twpc3Logger.logINFO("=== Loop over each value in FileEntries ===",false);

    // 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
    // Do...
    //twpc3Logger.logINFO("Step 7: Control Flow: Loop. ForEach CSVFileEntry in CSVFileEntryList (ID:" + ReadCSVReadyFileOutput.thisUUID + ")",true);
    //twpc3Logger.logINFO("=== Start Loop ===",false);  
    //twpc3Logger.enterScope();
    
    //LoadAppLogic.CSVFileEntryList CSVFileListUpdated = new CSVFileEntryList(new ArrayList<CSVFileEntry>(), UUID.randomUUID());
    
   // CSVFileListUpdated.thisCSVFileEntryList 
    
    
    int iteration = 0;
  
    UUID LastIteration = null;
    for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput.thisCSVFileEntryList) {
    	//--\\
    	List<UUID> ForEach_InfStep_Antecedents = new ArrayList<UUID>();
    	ForEach_InfStep_Antecedents.add(ReadCSVReadyFile_NS_UUID);
    	if(iteration > 0)
        	ForEach_InfStep_Antecedents.add(LastIteration);
    		
        UUID ForEach_InfStep_UUID = ProtoPML.addNewProtoInfStep("ForEach", "System", ForEach_InfStep_Antecedents);    
        UUID ForEach_NS_UUID = ProtoPML.addNewProtoNS("CSVFileEntry-"+FileEntry.FilePath+"-"+FileEntry.TargetTable, "ForEach", ForEach_InfStep_UUID);        	
    	//--//
    	//twpc3Logger.logINFO("--- Start Iteration ---",false);
		//twpc3Logger.logINFO("Current value: " + FileEntry.FilePath,false);	
    	// ///////////////////////////////////
    	// //// Pre Load Validation //////
    	// ///////////////////////////////////
    	// 7.a. IsExistsCSVFile
		//twpc3Logger.logINFO("Step 7a: Running IsExistsCSVFile",false);	
	    //twpc3Logger.logINFO("### Entering IsExistsCSVFile process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + FileEntry.thisUUID + " PATH:" + FileEntry.FilePath + ")",false);  
        //twpc3Logger.enterScope();
        //--\\
    	List<UUID> IsExistsCSVFile_InfStep_Antecedents = new ArrayList<UUID>();
    	IsExistsCSVFile_InfStep_Antecedents.add(ForEach_NS_UUID);
        UUID IsExistsCSVFileOutput_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsExistsCSVFileOutput", "System", IsExistsCSVFile_InfStep_Antecedents);    
        boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
        UUID IsExistsCSVFile_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsExistsCSVFileOutput, "IsExistsCSVFile", IsExistsCSVFileOutput_InfStep_UUID);        	
        //--//Check_IsExistsCSVFile_NS_UUID
        //twpc3Logger.exitScope();
        //twpc3Logger.logINFO("### Exiting IsExistsCSVFile process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + IsExistsCSVFile_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsExistsCSVFile_NS_UUID) + ")",false);        

    	// 7.b. Control Flow: Decision
        //twpc3Logger.logINFO("Step 7b: Control Flow: Decision on IsExistsCSVFile uses (ID:" + IsExistsCSVFile_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsExistsCSVFile_NS_UUID) + ")",true);

        //--\\
        List<UUID> Check_IsExistsCSVFile_InfStep_Antecedents = new ArrayList<UUID>();
        Check_IsExistsCSVFile_InfStep_Antecedents.add(IsExistsCSVFile_NS_UUID);
        UUID Check_IsExistsCSVFile_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsExistsCSVFile_InfStep_Antecedents);    
        if (!IsExistsCSVFileOutput) {
        	//twpc3Logger.logFATAL("IsExistsCSVFile failed.  Halting now ...");
        	new RuntimeException("IsExistsCSVFile failed");
        } else {
        	//twpc3Logger.logINFO("IsExistsCSVFile passed.  Continuing ...", false);
        }
        UUID Check_IsExistsCSVFile_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsExistsCSVFileOutput, "Check-IsExistsCSVFile", Check_IsExistsCSVFile_InfStep_UUID);
        //--//
        
    	// 7.c. ReadCSVFileColumnNames
		//twpc3Logger.logINFO("Step 7c: Running ReadCSVFileColumnNames",false);	
	    //twpc3Logger.logINFO("### Entering ReadCSVFileColumnNames process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + FileEntry.thisUUID + " PATH:" + FileEntry.FilePath + ")",false);  
	    //twpc3Logger.enterScope();

	    //--\\
        List<UUID> ReadCSVFileColumnNames_InfStep_Antecedents = new ArrayList<UUID>();
        ReadCSVFileColumnNames_InfStep_Antecedents.add(ForEach_NS_UUID);
        ReadCSVFileColumnNames_InfStep_Antecedents.add(Check_IsExistsCSVFile_NS_UUID);
        UUID ReadCSVFileColumnNames_InfStep_UUID = ProtoPML.addNewProtoInfStep("ReadCSVFileColumnNames", "System", ReadCSVFileColumnNames_InfStep_Antecedents);            
	    LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
    		LoadAppLogic.ReadCSVFileColumnNames(FileEntry);
        UUID ReadCSVFileColumnNames_NS_UUID = ProtoPML.addNewProtoNS("CSVFileEntry-"+FileEntry.FilePath+"-"+FileEntry.TargetTable, "ReadCSVFileColumnNames", ReadCSVFileColumnNames_InfStep_UUID);
        //--//
        
	    //CSVFileListUpdated.thisCSVFileEntryList.add(FileEntry);
	    
	  
	    //twpc3Logger.exitScope();
	    //twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
	    //twpc3Logger.logINFO("RETURNED CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);  

    	// 7.d. IsMatchCSVFileColumnNames
		//twpc3Logger.logINFO("Step 7d: Running IsMatchCSVFileColumnNames",false);	
	    //twpc3Logger.logINFO("### Entering IsMatchCSVFileColumnNames process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);  
	    //twpc3Logger.enterScope();
        //--\\
        List<UUID> IsMatchCSVFileColumnNames_InfStep_Antecedents = new ArrayList<UUID>();
        IsMatchCSVFileColumnNames_InfStep_Antecedents.add(ReadCSVFileColumnNames_NS_UUID);	    
        UUID IsMatchCSVFileColumnNames_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsMatchCSVFileColumnNames", "System", IsMatchCSVFileColumnNames_InfStep_Antecedents); 
        boolean IsMatchCSVFileColumnNamesOutput =
    		LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);
        UUID IsMatchCSVFileColumnNames_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchCSVFileColumnNamesOutput, "IsMatchCSVFileColumnNames", IsMatchCSVFileColumnNames_InfStep_UUID);
        //--//

        //twpc3Logger.exitScope();
	    //twpc3Logger.logINFO("### Exiting ReadCSVFileColumnNames process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + IsMatchCSVFileColumnNames_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchCSVFileColumnNames_NS_UUID) + ")",false);        

	    // 7.e. Control Flow: Decision
        //twpc3Logger.logINFO("Step 7e: Control Flow: Decision on IsMatchCSVFileColumnNames uses (ID:" + IsMatchCSVFileColumnNames_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchCSVFileColumnNames_NS_UUID) + ")",true);
        //--\\
        List<UUID> Check_IsMatchCSVFileColumnNames_InfStep_Antecedents = new ArrayList<UUID>();
        Check_IsMatchCSVFileColumnNames_InfStep_Antecedents.add(IsMatchCSVFileColumnNames_NS_UUID);
        UUID Check_IsMatchCSVFileColumnNames_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsMatchCSVFileColumnNames_InfStep_Antecedents);    
        if (!IsMatchCSVFileColumnNamesOutput) {
        	//twpc3Logger.logFATAL("IsMatchCSVFileColumnNames failed.  Halting now ...");
        	new RuntimeException("IsMatchCSVFileColumnNames failed");
        } else {
        	//twpc3Logger.logINFO("IsMatchCSVFileColumnNames passed.  Continuing ...", false);
        }
        UUID Check_IsMatchCSVFileColumnNames_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchCSVFileColumnNamesOutput, "Check-IsMatchCSVFileColumnNames", Check_IsMatchCSVFileColumnNames_InfStep_UUID);
        //--//


    	// ///////////////////////////////////
    	// //// Load File //////
    	// ///////////////////////////////////
    	// 7.f. LoadCSVFileIntoTable
		//twpc3Logger.logINFO("Step 7f: Running LoadCSVFileIntoTable",false);	
	    //twpc3Logger.logINFO("### Entering LoadCSVFileIntoTable process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        //twpc3Logger.enterScope();
        
	    //--\\
        List<UUID> LoadCSVFileIntoTable_InfStep_Antecedents = new ArrayList<UUID>();
        LoadCSVFileIntoTable_InfStep_Antecedents.add(ReadCSVFileColumnNames_NS_UUID);     
        LoadCSVFileIntoTable_InfStep_Antecedents.add(CreateEmptyLoadDB_InfStep_NS_UUID);
        LoadCSVFileIntoTable_InfStep_Antecedents.add(Check_IsMatchCSVFileColumnNames_NS_UUID); 
        UUID LoadCSVFileIntoTable_InfStep_UUID = ProtoPML.addNewProtoInfStep("LoadCSVFileIntoTable", "System", LoadCSVFileIntoTable_InfStep_Antecedents);    
        boolean LoadCSVFileIntoTableOutput =
    		LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
        UUID LoadCSVFileIntoTable_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+LoadCSVFileIntoTableOutput, "LoadCSVFileIntoTable", LoadCSVFileIntoTable_InfStep_UUID);
        //--//
        //twpc3Logger.exitScope();
	    //twpc3Logger.logINFO("### Exiting LoadCSVFileIntoTable process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + LoadCSVFileIntoTable_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(LoadCSVFileIntoTable_NS_UUID) + ")",false);        

     	// 7.g. Control Flow: Decision
        //twpc3Logger.logINFO("Step 7g: Control Flow: Decision on LoadCSVFileIntoTable uses (ID:" + LoadCSVFileIntoTable_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(LoadCSVFileIntoTable_NS_UUID) + ")",true);
        //--\\
        List<UUID> Check_LoadCSVFileIntoTable_InfStep_Antecedents = new ArrayList<UUID>();
        Check_LoadCSVFileIntoTable_InfStep_Antecedents.add(LoadCSVFileIntoTable_NS_UUID);
        UUID Check_LoadCSVFileIntoTable_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_LoadCSVFileIntoTable_InfStep_Antecedents);    
        if (!LoadCSVFileIntoTableOutput) {
        	//twpc3Logger.logFATAL("LoadCSVFileIntoTable failed.  Halting now ...");
        	new RuntimeException("LoadCSVFileIntoTable failed");
        } else {
        	//twpc3Logger.logINFO("LoadCSVFileIntoTable passed.  Continuing ...", false);
        }
        UUID Check_LoadCSVFileIntoTable_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+LoadCSVFileIntoTableOutput, "Check-LoadCSVFileIntoTable", Check_LoadCSVFileIntoTable_InfStep_UUID);
        //--//

    	// 7.h. UpdateComputedColumns
 		//twpc3Logger.logINFO("Step 7h: Running UpdateComputedColumns",false);	
	    //twpc3Logger.logINFO("### Entering UpdateComputedColumns process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        //twpc3Logger.enterScope();

        
	    //--\\
        List<UUID> UpdateComputedColumns_InfStep_Antecedents = new ArrayList<UUID>();
        UpdateComputedColumns_InfStep_Antecedents.add(ReadCSVFileColumnNames_NS_UUID);     
        UpdateComputedColumns_InfStep_Antecedents.add(CreateEmptyLoadDB_InfStep_NS_UUID);
        UpdateComputedColumns_InfStep_Antecedents.add(Check_LoadCSVFileIntoTable_NS_UUID); 
        UUID UpdateComputedColumns_InfStep_UUID = ProtoPML.addNewProtoInfStep("UpdateComputedColumns", "System", UpdateComputedColumns_InfStep_Antecedents);    
        boolean UpdateComputedColumnsOutput =
    		LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    //twpc3Logger.exitScope();
        UUID UpdateComputedColumns_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+UpdateComputedColumnsOutput, "LoadCSVFileIntoTable", UpdateComputedColumns_InfStep_UUID);
        //--//
        
	    //twpc3Logger.logINFO("### Exiting UpdateComputedColumns process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + UpdateComputedColumns_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(UpdateComputedColumns_NS_UUID) + ")",false);        

    	// 7.i. Control Flow: Decision
        //twpc3Logger.logINFO("Step 7i: Control Flow: Decision on UpdateComputedColumns uses (ID:" + UpdateComputedColumns_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(UpdateComputedColumns_NS_UUID) + ")",true);

        //--\\
        List<UUID> Check_UpdateComputedColumns_InfStep_Antecedents = new ArrayList<UUID>();
        Check_UpdateComputedColumns_InfStep_Antecedents.add(UpdateComputedColumns_NS_UUID);
        UUID Check_UpdateComputedColumns_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_UpdateComputedColumns_InfStep_Antecedents);    
        if (!UpdateComputedColumnsOutput) {
        	//twpc3Logger.logFATAL("UpdateComputedColumns failed.  Halting now ...");
        	new RuntimeException("UpdateComputedColumns failed");
        } else {
        	//twpc3Logger.logINFO("UpdateComputedColumns passed.  Continuing ...", false);
        }
        UUID Check_UpdateComputedColumns_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+UpdateComputedColumnsOutput, "Check-UpdateComputedColumns", Check_UpdateComputedColumns_InfStep_UUID);
        //--//

    	// ///////////////////////////////////
    	// //// PostLoad Validation //////
    	// ///////////////////////////////////
    	// 7.j. IsMatchTableRowCount
  		//twpc3Logger.logINFO("Step 7j: Running IsMatchTableRowCount",false);	
	    //twpc3Logger.logINFO("### Entering IsMatchTableRowCount process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        //twpc3Logger.enterScope();
        
        List<UUID> IsMatchTableRowCount_InfStep_Antecedents = new ArrayList<UUID>();
        IsMatchTableRowCount_InfStep_Antecedents.add(ReadCSVFileColumnNames_NS_UUID);     
        IsMatchTableRowCount_InfStep_Antecedents.add(CreateEmptyLoadDB_InfStep_NS_UUID);
        IsMatchTableRowCount_InfStep_Antecedents.add(Check_UpdateComputedColumns_NS_UUID);
        UUID IsMatchTableRowCount_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsMatchTableRowCount", "System", IsMatchTableRowCount_InfStep_Antecedents);    
	    boolean IsMatchTableRowCountOutput =
    		LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
	    UUID IsMatchTableRowCount_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchTableRowCountOutput, "IsMatchTableRowCount", IsMatchTableRowCount_InfStep_UUID);
	    //--//

	    //twpc3Logger.exitScope();
	    //twpc3Logger.logINFO("### Exiting IsMatchTableRowCount process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + IsMatchTableRowCount_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchTableRowCount_NS_UUID) + ")",false);        

    	// 7.k. Control Flow: Decision
	    //twpc3Logger.logINFO("Step 7k: Control Flow: Decision on IsMatchTableRowCount uses (ID:" + IsMatchTableRowCount_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchTableRowCount_NS_UUID) + ")",true);

        //--\\
        List<UUID> Check_IsMatchTableRowCountOutput_InfStep_Antecedents = new ArrayList<UUID>();
        Check_IsMatchTableRowCountOutput_InfStep_Antecedents.add(IsMatchTableRowCount_NS_UUID);
        UUID Check_IsMatchTableRowCountOutput_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsMatchTableRowCountOutput_InfStep_Antecedents);    
	    if (!IsMatchTableRowCountOutput) {
        	//twpc3Logger.logFATAL("IsMatchTableRowCount failed.  Halting now ...");
        	new RuntimeException("IsMatchTableRowCount failed");
        } else {
        	//twpc3Logger.logINFO("IsMatchTableRowCount passed.  Continuing ...", false);
        }
	    UUID Check_IsMatchTableRowCountOutput_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchTableRowCountOutput, "Check-IsMatchTableRowCount", Check_IsMatchTableRowCountOutput_InfStep_UUID);
	    //--//


    	// 7.l. IsMatchTableColumnRanges
   		//twpc3Logger.logINFO("Step 7l: Running IsMatchTableColumnRanges",false);	
	    //twpc3Logger.logINFO("### Entering IsMatchTableColumnRanges process ###",true);    
	    //twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:" + CreateEmptyLoadDBOutput.thisUUID + "), CSVFileEntry (ID:" + ReadCSVFileColumnNamesOutput.thisUUID + ", PATH:" + ReadCSVFileColumnNamesOutput.FilePath + ")",false);   
        //twpc3Logger.enterScope();

        //--\\
	    List<UUID> IsMatchTableColumnRanges_InfStep_Antecedents = new ArrayList<UUID>();
	    IsMatchTableColumnRanges_InfStep_Antecedents.add(ReadCSVFileColumnNames_NS_UUID);     
	    IsMatchTableColumnRanges_InfStep_Antecedents.add(CreateEmptyLoadDB_InfStep_NS_UUID);
	    IsMatchTableColumnRanges_InfStep_Antecedents.add(Check_IsMatchTableRowCountOutput_NS_UUID);
        UUID IsMatchTableColumnRanges_InfStep_UUID = ProtoPML.addNewProtoInfStep("IsMatchTableColumnRanges", "System", IsMatchTableColumnRanges_InfStep_Antecedents);    
        boolean IsMatchTableColumnRangesOutput =
    		LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
    				ReadCSVFileColumnNamesOutput);
	    UUID IsMatchTableColumnRanges_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchTableColumnRangesOutput, "IsMatchTableColumnRanges", IsMatchTableColumnRanges_InfStep_UUID);
	    //--//

        
        //twpc3Logger.exitScope();
	    //twpc3Logger.logINFO("### Exiting IsMatchTableColumnRanges process ###",true);      
        //twpc3Logger.logINFO("RETURNED (ID:" + IsMatchTableColumnRanges_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsMatchTableColumnRanges_NS_UUID) + ")",false);        
	    
    	// 7.m. Control Flow: Decision
	    //twpc3Logger.logINFO("Step 7m: Control Flow: Decision on IsMatchTableColumnRanges uses (ID:" + IsCSVReadyFileExists_NS_UUID + ", VALUE:" + ProtoPML.getConclusionFromProtoNS(IsCSVReadyFileExists_NS_UUID) + ")",true);

	    //--\\
        List<UUID> Check_IsMatchTableColumnRanges_InfStep_Antecedents = new ArrayList<UUID>();
        Check_IsMatchTableColumnRanges_InfStep_Antecedents.add(IsMatchTableColumnRanges_NS_UUID);
        UUID Check_IsMatchTableColumnRanges_InfStep_UUID = ProtoPML.addNewProtoInfStep("Check", "System", Check_IsMatchTableColumnRanges_InfStep_Antecedents);    
	    if (!IsMatchTableColumnRangesOutput) {
        	//twpc3Logger.logFATAL("IsMatchTableColumnRanges failed.  Halting now ...");
        	new RuntimeException("IsMatchTableColumnRanges failed");
        } else {
        	//twpc3Logger.logINFO("IsMatchTableColumnRanges passed.  Continuing ...", false);
        }
	    UUID Check_IsMatchTableColumnRanges_NS_UUID = ProtoPML.addNewProtoNS("boolean-"+IsMatchTableColumnRangesOutput, "Check-IsMatchTableColumnRanges", Check_IsMatchTableColumnRanges_InfStep_UUID);
		//--//
	    
	    LastIteration = UUID.randomUUID();
	    //twpc3Logger.logINFO("--- End Iteration ---",false);
	    iteration++;
    	//--\\
    	List<UUID> EndForEach_InfStep_Antecedents = new ArrayList<UUID>();
    	EndForEach_InfStep_Antecedents.add(Check_IsMatchTableColumnRanges_NS_UUID);    		
        UUID EndForEach_InfStep_UUID = ProtoPML.addNewProtoInfStep("EndForEach", "System", EndForEach_InfStep_Antecedents);    
        UUID EndForEach_NS_UUID = ProtoPML.addNewProtoNS("CSVFileEntry-"+FileEntry.FilePath+"-"+FileEntry.TargetTable, "EndForEach", EndForEach_InfStep_UUID);        	
        LastIteration = EndForEach_NS_UUID;
        //--//
    }
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("=== Ending loop ===",false);  

    // 8. CompactDatabase
	//twpc3Logger.logINFO("Step 8: Running CompactDatabase",false);	
	//twpc3Logger.logINFO("### Entering CompactDatabase process ###",true);    
	//twpc3Logger.logINFO("PARAMETER: DatabaseEntry (ID:"+ CreateEmptyLoadDBOutput.thisUUID + ")",false);
    //twpc3Logger.enterScope();
    //--\\
    List<UUID> CompactDatabase_InfStep_Antecedents = new ArrayList<UUID>();
    CompactDatabase_InfStep_Antecedents.add(CreateEmptyLoadDB_InfStep_NS_UUID);
    CompactDatabase_InfStep_Antecedents.add(LastIteration);
    UUID IsMatchTableColumnRanges_InfStep_UUID = ProtoPML.addNewProtoInfStep("CompactDatabase", "System", CompactDatabase_InfStep_Antecedents);    
    LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);
    UUID EndForEach_NS_UUID = ProtoPML.addNewProtoNS("void", "CompactDatabase", IsMatchTableColumnRanges_InfStep_UUID); 
    //--// 
   
    
    ProtoPML.generatePMLProof(EndForEach_NS_UUID);
    //twpc3Logger.exitScope();
    //twpc3Logger.logINFO("### Exiting CompactDatabase process ###",true);      
    //twpc3Logger.logINFO("RETURNED void",false); 

    //twpc3Logger.logINFO("****************************************",true);  
    //twpc3Logger.logINFO("***          END OF WORKFLOW         ***",true);  
    //twpc3Logger.logINFO("****************************************",false);      
    
  }
}
