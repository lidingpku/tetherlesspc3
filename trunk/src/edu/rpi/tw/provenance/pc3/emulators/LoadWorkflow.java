package edu.rpi.tw.provenance.pc3.emulators;

import java.util.*;

import edu.rpi.tw.provenance.protoprov.*;

public class LoadWorkflow {

	public static void main(String[] args) throws Exception {
		String scope = "main";
		
		// ANNOTATION //
		String provGroup = "ALL";
		List<String> provGroups = new ArrayList<String>();
		provGroups.add(provGroup);
		List<List<String>> intersect = new ArrayList<List<String>>();
		intersect.add(provGroups);
		ProtoProv.AddGroups(provGroups, intersect);
		// END //

		// ANNOTATION //
		String provCtlUsr = ProtoProv.AddController("USER", "USER", provGroups);
		String provCtlSys = ProtoProv.AddController("SYSTEM", "SYSTEM", provGroups);
		// END //
		String JobID = args[0], CSVRootPath = args[1];

		// ANNOTATION //
		String provVarJobID = ProtoProv.AddVariable("UserGen", "JobId", scope, "String", JobID, provGroups);
		String provVarCSVRootPath = ProtoProv.AddVariable("UserGen", "CSVRootPath", scope, "String", CSVRootPath, provGroups);
		String provFxnDirectAssertion = ProtoProv.AddFunction("DirectAssertion", scope, "DirectAssertion", provGroups);
		ProtoProv.AddWGB("WGB", provVarJobID, "generated by", provFxnDirectAssertion, provGroups);
		ProtoProv.AddWGB("WGB", provVarCSVRootPath, "generated by", provFxnDirectAssertion, provGroups);
		ProtoProv.AddWCB("WCB", provFxnDirectAssertion, "controlled by", provCtlUsr, provGroups);
		// END //

		// ///////////////////////////////////
		// //// Batch Initialization //////
		// ///////////////////////////////////
		// 1. IsCSVReadyFileExists
		boolean IsCSVReadyFileExistsOutput = LoadAppLogic.IsCSVReadyFileExists(CSVRootPath);
		// ANNOTATION //
		String provVarIsCSVReadyFileExistsOutput = ProtoProv.AddVariable("Control", "IsCSVReadyFileExistsOutput", scope, "boolean", IsCSVReadyFileExistsOutput, provGroups);
		String provFxnIsCSVReadyFileExists = ProtoProv.AddFunction("IsCSVReadyFileExists", scope, "IsCSVReadyFileExists", provGroups);
		ProtoProv.AddUsed("Used", provFxnIsCSVReadyFileExists, "file path", provVarCSVRootPath, provGroups);
		ProtoProv.AddWGB("WGB", provVarIsCSVReadyFileExistsOutput, "output", provFxnIsCSVReadyFileExists, provGroups);
		ProtoProv.AddWCB("WCB", provFxnIsCSVReadyFileExists, "controlled by", provCtlSys, provGroups);
		// END //
		
		// 2. Control Flow: Decision
		if (!IsCSVReadyFileExistsOutput) throw new RuntimeException("IsCSVReadyFileExists failed");


		// 3. ReadCSVReadyFile
		List<LoadAppLogic.CSVFileEntry> ReadCSVReadyFileOutput =
			LoadAppLogic.ReadCSVReadyFile(CSVRootPath);

		// ANNOTATION //
		String provVarReadCSVReadyFileOutput = ProtoProv.AddVariable("FxnGen","ReadCSVReadyFileOutput", scope, "List-CSVFileEntry", "ReadCSVReadyFileOutput", provGroups);
		String provFxnReadCSVReadyFile = ProtoProv.AddFunction("ReadCSVReadyFile", scope, "ReadCSVReadyFile", provGroups);
		ProtoProv.AddUsed("Used", provFxnReadCSVReadyFile, "file path", provVarCSVRootPath, provGroups);
		ProtoProv.AddUsed("Used", provFxnReadCSVReadyFile, "check IsCSVReadyFileExistsOutput", provVarIsCSVReadyFileExistsOutput, provGroups);
		ProtoProv.AddWGB("WGB", provVarReadCSVReadyFileOutput, "output", provFxnReadCSVReadyFile, provGroups);
		ProtoProv.AddWCB("WCB", provFxnReadCSVReadyFile, "controlled by", provCtlSys, provGroups);
		// END //		
		
		// 4. IsMatchCSVFileTables
		boolean IsMatchCSVFileTablesOutput = LoadAppLogic.IsMatchCSVFileTables(ReadCSVReadyFileOutput);

		// ANNOTATION //
		String provVarIsMatchCSVFileTablesOutput = ProtoProv.AddVariable("Control", "IsMatchCSVFileTablesOutput", scope, "boolean", IsMatchCSVFileTablesOutput, provGroups);
		String provFxnIsMatchCSVFileTables = ProtoProv.AddFunction("IsMatchCSVFileTables", scope, "IsMatchCSVFileTables", provGroups);
		ProtoProv.AddUsed("Used", provFxnIsMatchCSVFileTables, "file list", provVarReadCSVReadyFileOutput, provGroups);
		ProtoProv.AddWGB("WGB", provVarIsMatchCSVFileTablesOutput, "output", provFxnIsMatchCSVFileTables, provGroups);
		ProtoProv.AddWCB("WCB", provFxnIsMatchCSVFileTables, "controlled by", provCtlSys, provGroups);
		// END //		
				
		// 5. Control Flow: Decision
		if (!IsMatchCSVFileTablesOutput) throw new RuntimeException("IsMatchCSVFileTables failed");

		// 6. CreateEmptyLoadDB
		LoadAppLogic.DatabaseEntry CreateEmptyLoadDBOutput = LoadAppLogic.CreateEmptyLoadDB(JobID);

		// ANNOTATION //
		String provVarCreateEmptyLoadDBOutput = ProtoProv.AddVariable("FxnGen","CreateEmptyLoadDBOutput", scope, "DatabaseEntry", "CreateEmptyLoadDBOutput", provGroups);
		String provFxnCreateEmptyLoadDB = ProtoProv.AddFunction("CreateEmptyLoadDB", scope, "CreateEmptyLoadDB", provGroups);
		ProtoProv.AddUsed("Used", provFxnCreateEmptyLoadDB, "Job ID", provVarJobID, provGroups);
		ProtoProv.AddUsed("Used", provFxnCreateEmptyLoadDB, "check IsMatchCSVFileTablesOutput", provVarIsMatchCSVFileTablesOutput, provGroups);
		ProtoProv.AddWGB("WGB", provVarCreateEmptyLoadDBOutput, "output", provFxnCreateEmptyLoadDB, provGroups);
		ProtoProv.AddWCB("WCB", provFxnCreateEmptyLoadDB, "controlled by", provCtlSys, provGroups);
		// END //				
		
		int counter = 1;
		String lastCheck = "";
		String provVarDbEntryP2Detection = "";
		String provVarDbEntryP2ImageMeta = "";
		// 7. Control Flow: Loop. ForEach CSVFileEntry in ReadCSVReadyFileOutput
		// Do...
		for (LoadAppLogic.CSVFileEntry FileEntry : ReadCSVReadyFileOutput) {
			scope = "ForIter" + counter;

			// ANNOTATION //
			String provVarFileEntry = ProtoProv.AddVariable("LoopGen","FileEntry", scope, "CSVFileEntry", FileEntry.FilePath + "-" + FileEntry.TargetTable, provGroups);
			String provFxnForEach = ProtoProv.AddFunction("ForEach", scope, "ForEach", provGroups);
			ProtoProv.AddUsed("Used", provFxnForEach, "file list", provVarReadCSVReadyFileOutput, provGroups);
			if(counter > 1)
				ProtoProv.AddUsed("Used", provFxnForEach, "IsMatchTableColumnRangesOutput check", lastCheck, provGroups);
			ProtoProv.AddWGB("WGB", provVarFileEntry, "output", provFxnForEach, provGroups);			
			ProtoProv.AddWCB("WCB", provFxnForEach, "controlled by", provCtlSys, provGroups);
			// END //				
			
			// ///////////////////////////////////
			// //// Pre Load Validation //////
			// ///////////////////////////////////
			// 7.a. IsExistsCSVFile
			boolean IsExistsCSVFileOutput = LoadAppLogic.IsExistsCSVFile(FileEntry);
			// ANNOTATION //
			String provVarIsExistsCSVFileOutput = ProtoProv.AddVariable("Control", "IsExistsCSVFileOutput", scope, "boolean", IsExistsCSVFileOutput, provGroups);
			String provFxnIsExistsCSVFile = ProtoProv.AddFunction("IsExistsCSVFile", scope, "IsExistsCSVFile", provGroups);
			ProtoProv.AddUsed("Used", provFxnIsExistsCSVFile, "file entry", provVarFileEntry, provGroups);
			ProtoProv.AddWGB("WGB", provVarIsExistsCSVFileOutput, "output", provFxnIsExistsCSVFile, provGroups);
			ProtoProv.AddWCB("WCB", provFxnIsExistsCSVFile, "controlled by", provCtlSys, provGroups);
			// END //		
			
			// 7.b. Control Flow: Decision
			if (!IsExistsCSVFileOutput) throw new RuntimeException("IsExistsCSVFile failed");

			// 7.c. ReadCSVFileColumnNames
			LoadAppLogic.CSVFileEntry ReadCSVFileColumnNamesOutput =
				LoadAppLogic.ReadCSVFileColumnNames(FileEntry);


			// ANNOTATION //
			String provVarReadCSVFileColumnNamesOutput = ProtoProv.AddVariable("FxnGen","ReadCSVFileColumnNamesOutput", scope, "CSVFileEntry", FileEntry.FilePath + "-" + FileEntry.TargetTable, provGroups);
			String provFxnReadCSVFileColumnNames = ProtoProv.AddFunction("ReadCSVFileColumnNames", scope, "ReadCSVFileColumnNames", provGroups);
			ProtoProv.AddUsed("Used", provFxnReadCSVFileColumnNames, "check IsExistsCSVFileOutput", provVarIsExistsCSVFileOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnReadCSVFileColumnNames, "file entry", provVarFileEntry, provGroups);
			ProtoProv.AddWGB("WGB", provVarReadCSVFileColumnNamesOutput, "output", provFxnReadCSVFileColumnNames, provGroups);
			ProtoProv.AddWCB("WCB", provFxnReadCSVFileColumnNames, "controlled by", provCtlSys, provGroups);
			// END //				

			// 7.d. IsMatchCSVFileColumnNames
			boolean IsMatchCSVFileColumnNamesOutput =
				LoadAppLogic.IsMatchCSVFileColumnNames(ReadCSVFileColumnNamesOutput);

			// ANNOTATION //
			String provVarIsMatchCSVFileColumnNamesOutput = ProtoProv.AddVariable("Control", "IsMatchCSVFileColumnNamesOutput", scope, "boolean", IsMatchCSVFileColumnNamesOutput, provGroups);
			String provFxnIsMatchCSVFileColumnNames = ProtoProv.AddFunction("IsMatchCSVFileColumnNames", scope, "IsMatchCSVFileColumnNames", provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchCSVFileColumnNames, "file entry", provVarReadCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddWGB("WGB", provVarIsMatchCSVFileColumnNamesOutput, "output", provFxnIsMatchCSVFileColumnNames, provGroups);
			ProtoProv.AddWCB("WCB", provFxnIsMatchCSVFileColumnNames, "controlled by", provCtlSys, provGroups);
			// END //				
			
			// 7.e. Control Flow: Decision
			if (!IsMatchCSVFileColumnNamesOutput)
				throw new RuntimeException("IsMatchCSVFileColumnNames failed");


			// ///////////////////////////////////
			// //// Load File //////
			// ///////////////////////////////////
			// 7.f. LoadCSVFileIntoTable
			boolean LoadCSVFileIntoTableOutput =
				LoadAppLogic.LoadCSVFileIntoTable(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
			
			// ANNOTATION //
			String provVarLoadCSVFileIntoTableOutput = ProtoProv.AddVariable("FxnGen","LoadCSVFileIntoTableOutput", scope, "boolean", LoadCSVFileIntoTableOutput, provGroups);
			String provFxnLoadCSVFileIntoTable = ProtoProv.AddFunction("LoadCSVFileIntoTable", scope, "LoadCSVFileIntoTable", provGroups);
			ProtoProv.AddUsed("Used", provFxnLoadCSVFileIntoTable, "file entry", provVarReadCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnLoadCSVFileIntoTable, "database", provVarCreateEmptyLoadDBOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnLoadCSVFileIntoTable, "check IsMatchCSVFileColumnNamesOutput", provVarIsMatchCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddWGB("WGB", provVarLoadCSVFileIntoTableOutput, "output", provFxnLoadCSVFileIntoTable, provGroups);
			ProtoProv.AddWCB("WCB", provFxnLoadCSVFileIntoTable, "controlled by", provCtlSys, provGroups);
			// END //	
						
			if(ReadCSVFileColumnNamesOutput.TargetTable.equalsIgnoreCase("P2Detection")) {
				String dbEntry = LoadAppLogic.getDBEntry(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
				provVarDbEntryP2Detection = ProtoProv.AddVariable("FxnGen","DBEntryP2Detection", scope, "DBEntry_"+FileEntry.TargetTable, dbEntry, provGroups);
				ProtoProv.AddWGB("WGB", provVarDbEntryP2Detection, "created by", provFxnLoadCSVFileIntoTable, provGroups);
			}
			else if(ReadCSVFileColumnNamesOutput.TargetTable.equalsIgnoreCase("P2ImageMeta")) {
				String dbEntry = LoadAppLogic.getDBEntry(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);
				provVarDbEntryP2ImageMeta = ProtoProv.AddVariable("FxnGen","DBEntryP2ImageMeta", scope, "DBEntry_"+FileEntry.TargetTable, dbEntry, provGroups);
				ProtoProv.AddWGB("WGB", provVarDbEntryP2ImageMeta, "created by", provFxnLoadCSVFileIntoTable, provGroups);
			}

			
			// 7.g. Control Flow: Decision
			if (!LoadCSVFileIntoTableOutput) throw new RuntimeException("LoadCSVFileIntoTable failed");


			// 7.h. UpdateComputedColumns
			boolean UpdateComputedColumnsOutput =
				LoadAppLogic.UpdateComputedColumns(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);

			// ANNOTATION //
			String provVarUpdateComputedColumnsOutput = ProtoProv.AddVariable("FxnGen","UpdateComputedColumnsOutput", scope, "boolean", UpdateComputedColumnsOutput, provGroups);
			String provFxnUpdateComputedColumns = ProtoProv.AddFunction("UpdateComputedColumns", scope, "UpdateComputedColumns", provGroups);
			ProtoProv.AddUsed("Used", provFxnUpdateComputedColumns, "file entry", provVarReadCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnUpdateComputedColumns, "database", provVarCreateEmptyLoadDBOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnUpdateComputedColumns, "check LoadCSVFileIntoTableOutput", provVarLoadCSVFileIntoTableOutput, provGroups);
			ProtoProv.AddWGB("WGB", provVarUpdateComputedColumnsOutput, "output", provFxnUpdateComputedColumns, provGroups);
			ProtoProv.AddWCB("WCB", provFxnUpdateComputedColumns, "controlled by", provCtlSys, provGroups);
			// END //	
						
			// 7.i. Control Flow: Decision
			if (!UpdateComputedColumnsOutput) throw new RuntimeException("UpdateComputedColumns failed");


			// ///////////////////////////////////
			// //// PostLoad Validation //////
			// ///////////////////////////////////
			// 7.j. IsMatchTableRowCount
			boolean IsMatchTableRowCountOutput =
				LoadAppLogic.IsMatchTableRowCount(CreateEmptyLoadDBOutput, ReadCSVFileColumnNamesOutput);

			// ANNOTATION //
			String provVarIsMatchTableRowCountOutput = ProtoProv.AddVariable("Control", "IsMatchTableRowCountOutput", scope, "boolean", IsMatchTableRowCountOutput, provGroups);
			String provFxnIsMatchTableRowCount = ProtoProv.AddFunction("IsMatchTableRowCount", scope, "IsMatchTableRowCount", provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableRowCount, "file entry", provVarReadCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableRowCount, "database", provVarCreateEmptyLoadDBOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableRowCount, "check UpdateComputedColumnsOutput", provVarUpdateComputedColumnsOutput, provGroups);
			ProtoProv.AddWGB("WGB", provVarIsMatchTableRowCountOutput, "output", provFxnIsMatchTableRowCount, provGroups);
			ProtoProv.AddWCB("WCB", provFxnIsMatchTableRowCount, "controlled by", provCtlSys, provGroups);
			// END //	
			
			// 7.k. Control Flow: Decision
			if (!IsMatchTableRowCountOutput) throw new RuntimeException("IsMatchTableRowCount failed");


			// 7.l. IsMatchTableColumnRanges
			boolean IsMatchTableColumnRangesOutput =
				LoadAppLogic.IsMatchTableColumnRanges(CreateEmptyLoadDBOutput,
						ReadCSVFileColumnNamesOutput);
			

			// ANNOTATION //
			String provVarIsMatchTableColumnRangesOutput = ProtoProv.AddVariable("Control", "IsMatchTableColumnRangesOutput", scope, "boolean", IsMatchTableColumnRangesOutput, provGroups);
			String provFxnIsMatchTableColumnRanges = ProtoProv.AddFunction("IsMatchTableColumnRanges", scope, "IsMatchTableColumnRanges", provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableColumnRanges, "file entry", provVarReadCSVFileColumnNamesOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableColumnRanges, "database", provVarCreateEmptyLoadDBOutput, provGroups);
			ProtoProv.AddUsed("Used", provFxnIsMatchTableColumnRanges, "check IsMatchTableRowCountOutput", provVarIsMatchTableRowCountOutput, provGroups);
			ProtoProv.AddWGB("WGB", provVarIsMatchTableColumnRangesOutput, "output", provFxnIsMatchTableColumnRanges, provGroups);
			ProtoProv.AddWCB("WCB", provFxnIsMatchTableColumnRanges, "controlled by", provCtlSys, provGroups);
			// END //	
						
			// 7.m. Control Flow: Decision
			if (!IsMatchTableColumnRangesOutput)
				throw new RuntimeException("IsMatchTableColumnRanges failed");

			lastCheck = provVarIsMatchTableColumnRangesOutput;
			counter++;
		}
		scope = "main";
		
		// 8. CompactDatabase
		LoadAppLogic.CompactDatabase(CreateEmptyLoadDBOutput);


		// ANNOTATION //
		String provVarCompactDatabaseOutput = ProtoProv.AddVariable("FxnGen","CompactDatabaseOutput", scope, "void", "void", provGroups);
		String provFxnCompactDatabase = ProtoProv.AddFunction("CompactDatabase", scope, "CompactDatabase", provGroups);
		ProtoProv.AddUsed("Used", provFxnCompactDatabase, "database", provVarCreateEmptyLoadDBOutput, provGroups);
		ProtoProv.AddUsed("Used", provFxnCompactDatabase, "IsMatchTableColumnRangesOutput check", lastCheck, provGroups);
		ProtoProv.AddWGB("WGB", provVarCompactDatabaseOutput, "output", provFxnCompactDatabase, provGroups);
		ProtoProv.AddWCB("WCB", provFxnCompactDatabase, "controlled by", provCtlSys, provGroups);
		// END //	

		
//		OPMGen.printOPMGraph();
//		PMLGen.generatePMLProof(provVarCompactDatabaseOutput);
//		PMLGen.generatePMLProof(provVarDbEntryP2Detection);
//		PMLGen.generatePMLProof(provVarDbEntryP2ImageMeta);
//		RDFGen.genRDFStore();
	}
}
