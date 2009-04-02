package info.ipaw.pc3.PSLoadWorkflow;

import java.sql.*;
import java.util.*;
import java.io.*;
import java.util.UUID;

/***
 * LoadAppLogic
 * 
 * @author yoges@microsoft.com
 */
public class LoadAppLogic {
	// ////////////////////////////////////////////////////////////
	// / Database Constants ///////////////////////////////////////
	// ////////////////////////////////////////////////////////////
	private static final String SQL_SERVER = "jdbc:derby:";
	private static final String SQL_USER = "IPAW";
	private static final String SQL_PASSWORD = "pc3_load-2009";
	private static long START_TIME = 0;


	public static void setStartTime(long time) {
		START_TIME = time;
	}

	// Initialize Apache Derby JDBC Driver
	private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	// private static final String DERBY_DRIVER = "org.apache.derby.jdbc.ClientDriver"
	static {
		try {
			Class.forName(DERBY_DRIVER).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// ////////////////////////////////////////////////////////////
	// / Helper data structures ///////////////////////////////////
	// ////////////////////////////////////////////////////////////
	
	public static class String_UUID {
		public String thisString;
		public UUID thisUUID;
		
		String_UUID (String thisString, UUID thisUUID) {
			this.thisString = thisString;
			this.thisUUID = thisUUID;
		}
	}
	
	public static class Boolean_UUID {
		public boolean thisBoolean;
		public UUID thisUUID;
		
		Boolean_UUID (boolean thisBoolean, UUID thisUUID) {
			this.thisBoolean = thisBoolean;
			this.thisUUID = thisUUID;
		}
	}
	
	
	
	/***
	 * 
	 */
	public static class CSVFileEntry {
		public String FilePath;
		public String HeaderPath;
		public int RowCount;
		public String TargetTable;
		public String Checksum;
		public List<String> ColumnNames;
		public UUID thisUUID;

		public String getFilePath() {
			return FilePath;
		}

		public void setFilePath(String filePath) {
			FilePath = filePath;
		}

		public String getHeaderPath() {
			return HeaderPath;
		}

		public void setHeaderPath(String headerPath) {
			HeaderPath = headerPath;
		}

		public int getRowCount() {
			return RowCount;
		}

		public void setRowCount(int rowCount) {
			RowCount = rowCount;
		}

		public String getTargetTable() {
			return TargetTable;
		}

		public void setTargetTable(String targetTable) {
			TargetTable = targetTable;
		}

		public String getChecksum() {
			return Checksum;
		}

		public void setChecksum(String checksum) {
			Checksum = checksum;
		}

		public List<String> getColumnNames() {
			return ColumnNames;
		}

		public void setColumnNames(List<String> columnNames) {
			ColumnNames = columnNames;
		}
	}

	
	public static class CSVFileEntryList {
		public List<CSVFileEntry> thisCSVFileEntryList;
		public UUID thisUUID;
		
		CSVFileEntryList (List<CSVFileEntry> thisCSVFileEntryList, UUID thisUUID) {
			this.thisCSVFileEntryList = thisCSVFileEntryList;
			this.thisUUID = thisUUID;			
		}
		
	}
	
	/***
	 * 
	 */
	public static class DatabaseEntry {
		public String DBGuid;
		public String DBName;
		public String ConnectionString;
		public UUID thisUUID;
		
		public String getDBGuid() {
			return DBGuid;
		}

		public void setDBGuid(String guid) {
			DBGuid = guid;
		}

		public String getDBName() {
			return DBName;
		}

		public void setDBName(String name) {
			DBName = name;
		}

		public String getConnectionString() {
			return ConnectionString;
		}

		public void setConnectionString(String connectionString) {
			ConnectionString = connectionString;
		}
	}

	// ////////////////////////////////////////////////////////////
	// / Pre-Load Sanity Checks ///////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/***
	 * Checks if the CSV Ready File exists in the given rooth path to the CSV
	 * Batch
	 * 
	 * @param CSVRootPath
	 *            Path to the root directory for the batch
	 * @return true if the csv_ready.csv file exists in the CSVRoothPath. False
	 *         otherwise.
	 */
	public static boolean IsCSVReadyFileExists(String CSVRootPath) {

		// 1. Check if parent directory exists.
		twpc3Logger.logINFO("1. Checking if parent directory exists: " + CSVRootPath,false);
		File RootDirInfo = new File(CSVRootPath);
		if (!RootDirInfo.exists()) {
			twpc3Logger.logINFO("ERROR: Parent directory doesn't exist!", false);
			return false;
		}

		// 2. Check if CSV Ready file exists. We assume a static name for the
		// ready file.
		twpc3Logger.logINFO("2. Now checking if CSV Ready file exists. We assume name csv_ready.csv for the ready file. ",false);
		File ReadyFileInfo = new File(CSVRootPath, "csv_ready.csv");
		if(!ReadyFileInfo.exists())
			twpc3Logger.logINFO("ERROR: csv_ready.csv not found at " + CSVRootPath + "!", false);

		return ReadyFileInfo.exists();
	}

	/**
	 * @param CSVRootPath
	 * @return
	 */
	public static List<CSVFileEntry> ReadCSVReadyFile(String CSVRootPath)
	throws IOException {

		// 1. Initialize output list of file entries
		twpc3Logger.logINFO("1. Initializing output list of file entries: List<CSVFileEntry> CSVFileEntryList",false);
		List<CSVFileEntry> CSVFileEntryList = new ArrayList<CSVFileEntry>();

		// 2. Open input stream to read from CSV Ready File
		twpc3Logger.logINFO("2. Opening input stream ReadyFileStream to read from CSV Ready File: " + CSVRootPath+"csv_ready.csv",false);
		File CSVReadyFile = new File(CSVRootPath, "csv_ready.csv");
		BufferedReader ReadyFileStream = 
			new BufferedReader(new InputStreamReader(new FileInputStream(CSVReadyFile)));

		// 3. Read each line in CSV Ready file and split the lines into
		// individual columns separated by commas
		twpc3Logger.logINFO("3. Reading each line in CSV Ready file and split the lines into individual columns separated by commas",false);
		twpc3Logger.logINFO("=== Loop over each line in ReadyFileStream ===",false);
		twpc3Logger.enterScope();
		String ReadyFileLine;
		while ((ReadyFileLine = ReadyFileStream.readLine()) != null) {
			twpc3Logger.logINFO("--- Start Iteration ---",false);
			// 3.a. Expect each line in the CSV ready file to be of the format:
			// <FileName>,<NumRows>,<TargetTable>,<MD5Checksum>
			twpc3Logger.logINFO("3.a. Split current line (of form <FileName>,<NumRows>,<TargetTable>,<MD5Checksum>) by commas",true);
			twpc3Logger.logINFO("CURRENT LINE: " + ReadyFileLine, false);
			String[] ReadyFileLineTokens = ReadyFileLine.split(",");

			// 3.b. Create an empty FileEntry and populate it with the columns
			twpc3Logger.logINFO("3.b. Creating an empty FileEntry, and populating it with the four values",true);
			CSVFileEntry FileEntry = new CSVFileEntry();
			FileEntry.FilePath = 
				CSVRootPath + File.separator + ReadyFileLineTokens[0].trim(); // column 1
			FileEntry.HeaderPath = FileEntry.FilePath + ".hdr";
			FileEntry.RowCount = 
				Integer.parseInt(ReadyFileLineTokens[1].trim()); // column 2
			FileEntry.TargetTable = ReadyFileLineTokens[2].trim(); // column 3
			FileEntry.Checksum = ReadyFileLineTokens[3].trim(); // column 4

			// 3.c. Add file entry to output list
			twpc3Logger.logINFO("3.c. Add new file entry to output list CSVFileEntryList",false);
			CSVFileEntryList.add(FileEntry);
			twpc3Logger.logINFO("--- End Iteration ---",false);
		}
		twpc3Logger.exitScope();
		twpc3Logger.logINFO("=== End of loop ===",false);
		// 4. Close input stream and return output file entry list
		twpc3Logger.logINFO("4. Close input stream ReadyFileStream and return output file entry list CSVFileEntryList",false);
		ReadyFileStream.close();
		return CSVFileEntryList;
	}

	/**
	 * Check if the correct list of files/table names are present in this
	 * 
	 * @param FileEntries
	 * @return
	 */
	public static boolean IsMatchCSVFileTables(List<CSVFileEntry> FileEntries) {
		// check if the file count and the expected number of tables match
		twpc3Logger.logINFO("1. Checking if the file count and the expected number of tables match",true);
		twpc3Logger.logINFO("ACTUAL (from CSV Ready file): " + FileEntries.size() + " EXPECTED (LoadConstants.EXPECTED_TABLES.size()): " + LoadConstants.EXPECTED_TABLES.size(),false);

		if (LoadConstants.EXPECTED_TABLES.size() != FileEntries.size()) {
			twpc3Logger.logINFO("ERROR: File count mismatch!", false);
			return false;
		}

		// for each expected table name, check if it is present in the list of
		// file entries

		twpc3Logger.logINFO("2. For each expected table name (LoadConstants.EXPECTED_TABLES), checking if it is present in the list of found FileEntries",true);

		twpc3Logger.logINFO("=== Loop over each value in LoadConstants.EXPECTED_TABLES ===",false);
		twpc3Logger.enterScope();
		for (String TableName : LoadConstants.EXPECTED_TABLES) {
			twpc3Logger.logINFO("--- Start Iteration ---",false);
			twpc3Logger.logINFO("Current value: " + TableName,false);			
			boolean TableExists = false;
			twpc3Logger.logINFO("=== Loop over each value in FileEntries ===",false);
			twpc3Logger.enterScope();
			for (CSVFileEntry FileEntry : FileEntries) {
				twpc3Logger.logINFO("Current value: " + FileEntry.TargetTable,false);	
				if (!TableName.equals(FileEntry.TargetTable)) {
					continue;
				}
				else {
					twpc3Logger.logINFO("Match in FileEntries found!",false);
				}

				TableExists = true; // found a match
				break;
			}
			twpc3Logger.exitScope();
			twpc3Logger.logINFO("=== End of loop ===",false);

			// if the table name did not exist in list of CSV files, this check
			// fails.
			if (!TableExists) {
				twpc3Logger.logINFO("ERROR: No table" + TableName + " found in FileEntries!",false);				
				return false;
			}
			twpc3Logger.logINFO("--- End Iteration ---",false);
		}
		twpc3Logger.exitScope();
		twpc3Logger.logINFO("=== End of loop ===",false);
		return true;
	}

	/**
	 * Test if a CSV File defined in the CSV Ready list actually exists on disk.
	 * 
	 * @param FileEntry
	 *            FileEntry for CSVFile to test
	 * @return True if the FilePath in the given FileEntry exists on disk. False
	 *         otherwise.
	 */
	public static boolean IsExistsCSVFile(CSVFileEntry FileEntry) {
		twpc3Logger.logINFO("1. Checking if the given FileEntry " + FileEntry.FilePath + " exists",false);	
		File CSVFileInfo = new File(FileEntry.FilePath);
		if(!CSVFileInfo.exists()) { 
			twpc3Logger.logINFO("ERROR: FileEntry " + FileEntry.FilePath + " doesn't exist!",false);	
			return false;
		}
		File CSVFileHeaderInfo = new File(FileEntry.HeaderPath);
		twpc3Logger.logINFO("2. Checking if the given FileEntry header file " + FileEntry.HeaderPath + " exists",false);	
		if(!CSVFileHeaderInfo.exists()) {
			twpc3Logger.logINFO("ERROR: FileEntry header file " + FileEntry.HeaderPath + " doesn't exist!",false);	
		}

		return CSVFileHeaderInfo.exists();
	}

	/**
	 * @param FileEntry
	 * @return
	 * @throws Exception
	 */
	public static CSVFileEntry ReadCSVFileColumnNames(CSVFileEntry FileEntry)
	throws Exception {
		// 2. Read the header line of the CSV File.
		twpc3Logger.logINFO("1. Read the header line of the CSV File " + FileEntry.HeaderPath,false);  
		BufferedReader CSVFileReader = 
			new BufferedReader(new InputStreamReader(new FileInputStream(FileEntry.HeaderPath)));
		String HeaderRow = CSVFileReader.readLine();

		// 3. Extract the comma-separated columns names of the CSV File from its
		// header line.
		// Strip empty spaces around column names.
		twpc3Logger.logINFO("2. Split header line by commas, and adding each to list of colum names in FileEntry.ColumnNames",true);
		twpc3Logger.logINFO("CURRENT LINE: " + HeaderRow, false);

		String[] ColumnNames = HeaderRow.split(",");
		FileEntry.ColumnNames = new ArrayList<String>();
		for (String ColumnName : ColumnNames) {
			FileEntry.ColumnNames.add(ColumnName.trim());
		}

		CSVFileReader.close();
		return FileEntry;
	}

	/**
	 * Checks if the correct list of column headers is present for the CSV file
	 * to match the table
	 * 
	 * @param FileEntry
	 *            FileEntry for CSV File whose column headers to test
	 * @return True if the column headers present in the CSV File are the same
	 *         as the expected table columns. False otherwise.
	 */
	public static boolean IsMatchCSVFileColumnNames(CSVFileEntry FileEntry) {
		twpc3Logger.logINFO("1. Checking whether the table" + FileEntry.TargetTable + " matches one of the expected tables",true); 
		// determine expected columns
		List<String> ExpectedColumns = null;
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
			twpc3Logger.logINFO("It is a P2Detection table",false); 
			ExpectedColumns = LoadConstants.EXPECTED_DETECTION_COLS;
		} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			twpc3Logger.logINFO("It is a P2FrameMeta table",false); 
			ExpectedColumns = LoadConstants.EXPECTED_FRAME_META_COLS;
		} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			twpc3Logger.logINFO("It is a P2ImageMeta table",false); 
			ExpectedColumns = LoadConstants.EXPECTED_IMAGE_META_COLS;
		} else {
			twpc3Logger.logINFO("ERROR: Table is not one of the expected kinds!",false); 
			// none of the table types match...invalid
			return false;
		}

		twpc3Logger.logINFO("2. Testing whether the expected and present column name counts are the same", true); 
		twpc3Logger.logINFO("ACTUAL (Column count from FileEntry): " + FileEntry.ColumnNames.size() + " EXPECTED (size of " + FileEntry.TargetTable +" table): " + LoadConstants.EXPECTED_TABLES.size(),false);

		// test if the expected and present column name counts are the same
		if (ExpectedColumns.size() != FileEntry.ColumnNames.size()) {
			twpc3Logger.logINFO("ERROR: Column count mismatch!", false);
			return false;
		}

		// test of all expected names exist in the columns present
		twpc3Logger.logINFO("3. Testing whether all expected names exist in the columns present", true); 

		for (String ColumnName : ExpectedColumns) {
			if (!FileEntry.ColumnNames.contains(ColumnName)) {
				twpc3Logger.logINFO("ERROR: Column " + ColumnName + " missing!", false);
				return false; // mismatch
			}
		}

		// all columns match
		return true;
	}

	// ////////////////////////////////////////////////////////////
	// / Loading Section //////////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/**
	 * @param JobID
	 * @return
	 * @throws Exception
	 */
	public static DatabaseEntry CreateEmptyLoadDB(String JobID)
	throws Exception {

		// initialize database entry for storing database properties
		DatabaseEntry DBEntry = new DatabaseEntry();
		twpc3Logger.logINFO("1. Initialize database entry" + DBEntry + "for storing database properties", true);

		DBEntry.DBName = JobID + "_LoadDB";
		DBEntry.DBGuid = UUID.randomUUID().toString();
		twpc3Logger.logINFO("Database Name: " + DBEntry.DBName, true);
		twpc3Logger.logINFO("Database ID: " + DBEntry.DBGuid, false);

		// initialize Sql Connection String to sql server
		twpc3Logger.logINFO("2. Initialize Sql Connection String to sql server", true);

		StringBuilder ConnStr = new StringBuilder(SQL_SERVER);
		ConnStr.append(";databaseName=");
		ConnStr.append(DBEntry.DBName);
		ConnStr.append(";user=");
		ConnStr.append(SQL_USER);
		ConnStr.append(";password=");
		ConnStr.append(SQL_PASSWORD);
		twpc3Logger.logINFO("ConnStr: " + ConnStr.toString(), false);

		// Create empty database instance
		twpc3Logger.logINFO("3. Create empty database instance on Sql server", false);
		Connection SqlConn = null;
		try {
			String CreateDBConnStr = ConnStr.toString() + ";create=true";
			twpc3Logger.logINFO("3a. Connecting using string " + CreateDBConnStr, false);
			SqlConn = DriverManager.getConnection(CreateDBConnStr);
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("3b. Disconnecting from database", false);
				SqlConn.close();
			}
		}

		// update Sql Connection String to new create tables

		twpc3Logger.logINFO("4. linking created connection string ConnStr to DBEntry instance " + DBEntry.DBGuid, false);
		DBEntry.ConnectionString = ConnStr.toString();

		// create tables
		SqlConn = null;

		twpc3Logger.logINFO("5. creating tables on Sql server ", false);
		try {
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// Create P2 Table
			Statement SqlCmd = SqlConn.createStatement();
			twpc3Logger.logINFO("5a. Creating P2 Detection table", false);
			SqlCmd.executeUpdate(LoadSql.CREATE_DETECTION_TABLE);
			// Create P2FrameMeta Table
			twpc3Logger.logINFO("5b. Creating P2 Frame Metadata table", false);
			SqlCmd.executeUpdate(LoadSql.CREATE_FRAME_META_TABLE);
			// Create P2ImageMeta Table
			twpc3Logger.logINFO("5c. Creating P2 Image Metadata table", false);
			SqlCmd.executeUpdate(LoadSql.CREATE_IMAGE_META_TABLE);

		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("5d. Disconnecting from database", false);
				SqlConn.close();
			}
		}
		return DBEntry;
	}

	/**
	 * Loads a CSV File into an existing table using derby bulk load:
	 * SYSCS_UTIL.SYSCS_IMPORT_TABLE
	 * 
	 * @param DBEntry
	 *            Database into which to load the CSV file
	 * @param FileEntry
	 *            File to be bulk loaded into database table
	 * @return True if the bulk load ran without exceptions. False otherwise.
	 */
	public static boolean LoadCSVFileIntoTable(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception {
		Connection SqlConn = null;
		try {
			// connect to database instance
			twpc3Logger.logINFO("1. Connecting to Sql server through DatabaseEntry ConnectionString", false);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// build bulk insert SQL command
			twpc3Logger.logINFO("2. Inserting CSVFileEntry into Sql database via bulk insert SQL command", false);

			CallableStatement SqlCmd = 
				SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?,?,?,?,?,?,?)");

			SqlCmd.setString(1, null);
			SqlCmd.setString(2, FileEntry.TargetTable.toUpperCase());
			SqlCmd.setString(3, FileEntry.FilePath);
			SqlCmd.setString(4, ",");
			SqlCmd.setString(5, null);
			SqlCmd.setString(6, null);
			SqlCmd.setShort(7, (short)0);


			// execute bulk insert command
			SqlCmd.execute();

		} catch (Exception ex) {
			twpc3Logger.logINFO("ERROR: Bulk insert SQL command failed!", false);
			// bulk insert failed
			return false;
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("3. Disconnecting from database", false);
				SqlConn.close();
			}
		}

		// bulk insert success
		return true;
	}

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean UpdateComputedColumns(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception {
		Connection SqlConn = null;
		try {
			twpc3Logger.logINFO("1. Connecting to Sql server through DatabaseEntry ConnectionString", false);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			twpc3Logger.logINFO("2. Checking whether the table" + FileEntry.TargetTable + " matches one of the expected tables",true); 
			if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
				twpc3Logger.logINFO("FileEntry target table is a P2 Detection table",true); 

				// Update ZoneID
				twpc3Logger.logINFO("Updating ZoneID, with SQL command " + "UPDATE P2Detection SET zoneID = (\"dec\"+(90.0))/(0.0083333)", true);
				Statement SqlCmd = SqlConn.createStatement();
				SqlCmd.executeUpdate(
				"UPDATE P2Detection SET zoneID = (\"dec\"+(90.0))/(0.0083333)");

				// Update cx
				twpc3Logger.logINFO("Updating cx, with SQL command " + "UPDATE P2Detection SET cx = (COS(RADIANS(\"dec\"))*COS(RADIANS(ra)))", true);

				SqlCmd.executeUpdate(
				"UPDATE P2Detection SET cx = (COS(RADIANS(\"dec\"))*COS(RADIANS(ra)))");

				// Update cy
				twpc3Logger.logINFO("Updating cy, with SQL command " + "UPDATE P2Detection SET cy = COS(RADIANS(\"dec\"))*SIN(RADIANS(ra))", true);

				SqlCmd.executeUpdate(
				"UPDATE P2Detection SET cy = COS(RADIANS(\"dec\"))*SIN(RADIANS(ra))");

				// Update cz
				twpc3Logger.logINFO("Updating cz, with SQL command " + "UPDATE P2Detection SET cz = (SIN(RADIANS(\"dec\")))", false);


				SqlCmd.executeUpdate(
				"UPDATE P2Detection SET cz = (SIN(RADIANS(\"dec\")))");

			} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
				twpc3Logger.logINFO("FileEntry target table is a P2 Frame Metadata table", true);
				twpc3Logger.logINFO("No columns to be updated for the Frame Metadata table", false);				
				// No columns to be updated for FrameMeta
			} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
				twpc3Logger.logINFO("FileEntry target table is a P2 Image Metadata table", true);
				twpc3Logger.logINFO("No columns to be updated for the Image Metadata table", false);
				// No columns to be updated for ImageMeta
			} else {
				twpc3Logger.logINFO("ERROR: The table doesn't match any of the expected tables!", false);
				// none of the table types matches...invalid
				return false;
			}

		} catch (Exception ex) {
			// update column failed
			twpc3Logger.logINFO("ERROR: Column updating failed for " + FileEntry.TargetTable + "!", false);
			return false;
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("3. Disconnecting from database", false);
				SqlConn.close();
			}
		}
		// update column success
		return true;
	}

	// ////////////////////////////////////////////////////////////
	// / Post-Load Checks /////////////////////////////////////
	// ////////////////////////////////////////////////////////////

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean IsMatchTableRowCount(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception {
		// does the number of rows expected match the number of rows loaded
		Connection SqlConn = null;
		try {
			twpc3Logger.logINFO("1. Connecting to Sql server through DatabaseEntry ConnectionString", false);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			Statement SqlCmd = SqlConn.createStatement();
			twpc3Logger.logINFO("2. For FileEntry.TargetTable, Executing row count command " + "SELECT COUNT(*) FROM " + FileEntry.TargetTable, false);

			// execute row count command
			ResultSet Results = 
				SqlCmd.executeQuery("SELECT COUNT(*) FROM " + FileEntry.TargetTable);
			if (Results.next()) {
				twpc3Logger.logINFO("3. Checking if row count matches expected row count", true);

				// check if row count matches expected row count
				int RowCount = (int) Results.getInt(1);
				twpc3Logger.logINFO("Row count on Sql server: " + RowCount + " Expected row count: " + FileEntry.RowCount, false);

				if(RowCount != FileEntry.RowCount) {
					twpc3Logger.logINFO("ERROR: Row count mismatch detected!", false);
				} 
				return RowCount == FileEntry.RowCount;
			} else {
				twpc3Logger.logINFO("ERROR: No row count returned from SQL command!", false);
				return false; // error case!
			}
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("4. Disconnecting from database", false);
				SqlConn.close();
			}
		}

	}

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean IsMatchTableColumnRanges(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception {
		// determine expected column ranges
		List<LoadConstants.ColumnRange> ExpectedColumnRanges = null;
		twpc3Logger.logINFO("1. Checking whether the table" + FileEntry.TargetTable + " matches one of the expected tables",true); 
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
			twpc3Logger.logINFO("FileEntry target table is a P2 Detection table", true);
			twpc3Logger.logINFO("Getting expected table column range: " + LoadConstants.EXPECTED_DETECTION_COL_RANGES, false);
			ExpectedColumnRanges = LoadConstants.EXPECTED_DETECTION_COL_RANGES;
		} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			// No columns range values available for FrameMeta
			twpc3Logger.logINFO("FileEntry target table is a P2 Frame Metadata table", true);
			twpc3Logger.logINFO("No columns range values available for P2 Frame Metadata", false);
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			// No columns range values available for ImageMeta
			twpc3Logger.logINFO("FileEntry target table is a P2 Image Metadata table", true);
			twpc3Logger.logINFO("No columns range values available for P2 Image Metadata", false);
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} else {
			// none of the table types matches...invalid
			twpc3Logger.logINFO("ERROR: Table isn't any of the expected tables, and is therefore invalid", false);
			return false;
		}

		// connect to database instance
		Connection SqlConn = null;
		try {
			twpc3Logger.logINFO("2. Connecting to Sql server through DatabaseEntry ConnectionString", false);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// For each column in available list, test if rows in table fall
			// outside expected range
			twpc3Logger.logINFO("3. Checking each column in the available list, and testing if rows in table fall outside expected range", false);
			twpc3Logger.enterScope();
			for (LoadConstants.ColumnRange Column : ExpectedColumnRanges) {
				twpc3Logger.logINFO("Current column " + Column, false);
				twpc3Logger.logINFO("3a. Building/Executing SQL command to count number of rows falling outside expected range", true);
				// build SQL command for error count
				String SqlStr = 
					String.format(
							"SELECT COUNT(*) FROM %1$s WHERE (%2$s < %3$s OR %2$s > %4$s) AND %2$s != -999",
							FileEntry.TargetTable, Column.ColumnName,
							Column.MinValue, Column.MaxValue);
				twpc3Logger.logINFO(SqlStr, true);

				// execute range error count command
				Statement SqlCmd = SqlConn.createStatement();
				ResultSet Results = SqlCmd.executeQuery(SqlStr);
				if (Results.next()) {
					int ErrorCount = Results.getInt(1);
					twpc3Logger.logINFO("Error count: " + ErrorCount, true);
					if (ErrorCount > 0) {
						twpc3Logger.logINFO("ERROR: Range error found!", true);
						return false; // found a range error
					}
				} else {
					twpc3Logger.logINFO("ERROR: No range value returned by SQL command!", true);
					return false; // error case
				}

			}
			twpc3Logger.exitScope();
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("4. Disconnecting from database", false);
				SqlConn.close();
			}
		}

		return true; // no range errors found
	}

	/**
	 * @param DBEntry
	 */
	public static void CompactDatabase(DatabaseEntry DBEntry) throws Exception {
		// Shrink database instance
		Connection SqlConn = null;
		try {
			twpc3Logger.logINFO("1. Connecting to database instance " + DBEntry.ConnectionString,false);
			twpc3Logger.logINFO("2. Iterating over tables in database, and running compression commands",false);
			twpc3Logger.logINFO("Entering loop", false);
			twpc3Logger.enterScope();
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			for (String TableName : LoadConstants.EXPECTED_TABLES) {
				twpc3Logger.logINFO("Current table: " + TableName, true);
				CallableStatement SqlCmd = 
					SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
				SqlCmd.setString(1, SQL_USER.toUpperCase());
				SqlCmd.setString(2, TableName.toUpperCase());
				SqlCmd.setShort(3, (short) 1);
				SqlCmd.setShort(4, (short) 1);
				SqlCmd.setShort(5, (short) 1);
				twpc3Logger.logINFO("2a. Executing SQL command: " + SqlCmd, true);
				SqlCmd.execute();
			}
			twpc3Logger.exitScope();
			twpc3Logger.logINFO("Exiting loop", false);		    
		} finally {
			if (SqlConn != null) {
				twpc3Logger.logINFO("3. Disconnecting from database", false);
				SqlConn.close();
			}
		}	
	}
}
