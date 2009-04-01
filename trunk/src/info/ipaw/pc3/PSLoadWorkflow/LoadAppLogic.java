package info.ipaw.pc3.PSLoadWorkflow;

import java.sql.*;
import java.util.*;
import java.io.*;

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

	/***
	 * 
	 */
	public static class DatabaseEntry {
		public String DBGuid;
		public String DBName;
		public String ConnectionString;
    
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Starting IsCSVReadyFileExists process");
		// 1. Check if parent directory exists.
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Verifying CSV Root Path exists: " + CSVRootPath);
		File RootDirInfo = new File(CSVRootPath);
		if (!RootDirInfo.exists()) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "CSV Root Path does not exist");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "IsCSVReadyFileExists evaluates to false");			
			return false;
		}

		// 2. Check if CSV Ready file exists. We assume a static name for the
		// ready file.
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Verifying csv_ready.csv exists");		
		File ReadyFileInfo = new File(CSVRootPath, "csv_ready.csv");
		if(!ReadyFileInfo.exists())
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "csv_ready.csv does not exist");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting IsCSVReadyFileExists process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println();	
		
		return ReadyFileInfo.exists();
	}

	/**
	 * @param CSVRootPath
	 * @return
	 */
	public static List<CSVFileEntry> ReadCSVReadyFile(String CSVRootPath)
			throws IOException {

		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Starting ReadCSVReadyFile process");
		// 1. Initialize output list of file entries
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Initializing output list of file entries");
		List<CSVFileEntry> CSVFileEntryList = new ArrayList<CSVFileEntry>();

		// 2. Open input stream to read from CSV Ready File
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Opening input stream to read from csv_ready.csv");
		File CSVReadyFile = new File(CSVRootPath, "csv_ready.csv");
		BufferedReader ReadyFileStream = 
			new BufferedReader(new InputStreamReader(new FileInputStream(CSVReadyFile)));

		// 3. Read each line in CSV Ready file and split the lines into
		// individual columns separated by commas
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Preparing to read each line in from csv_ready.csv (<FileName>,<NumRows>,<TargetTable>,<MD5Checksum>)");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Entering loop");
		String ReadyFileLine;
		while ((ReadyFileLine = ReadyFileStream.readLine()) != null) {

			// 3.a. Expect each line in the CSV ready file to be of the format:
			// <FileName>,<NumRows>,<TargetTable>,<MD5Checksum>
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Splitting line " + ReadyFileLine);
			
			String[] ReadyFileLineTokens = ReadyFileLine.split(",");

			// 3.b. Create an empty FileEntry and populate it with the columns
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Creating CSVFileEntry for this line, based on the four parameters");
			CSVFileEntry FileEntry = new CSVFileEntry();
			FileEntry.FilePath = 
				CSVRootPath + File.separator + ReadyFileLineTokens[0].trim(); // column 1
			FileEntry.HeaderPath = FileEntry.FilePath + ".hdr";
			FileEntry.RowCount = 
				Integer.parseInt(ReadyFileLineTokens[1].trim()); // column 2
			FileEntry.TargetTable = ReadyFileLineTokens[2].trim(); // column 3
			FileEntry.Checksum = ReadyFileLineTokens[3].trim(); // column 4

			// 3.c. Add file entry to output list
			CSVFileEntryList.add(FileEntry);
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Leaving loop");
		// 4. Close input stream and return output file entry list
		ReadyFileStream.close();
		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting ReadCSVReadyFile process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println();	
		return CSVFileEntryList;
	}

	/**
	 * Check if the correct list of files/table names are present in this
	 * 
	 * @param FileEntries
	 * @return
	 */
	public static boolean IsMatchCSVFileTables(List<CSVFileEntry> FileEntries) {
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Starting IsMatchCSVFileTables process");
		// check if the file count and the expected number of tables match
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Verifying the file count and the expected number of tables match");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Expected: " + LoadConstants.EXPECTED_TABLES.size() + " Actual: " + FileEntries.size());
		if (LoadConstants.EXPECTED_TABLES.size() != FileEntries.size()) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Count mismatch detected");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "IsMatchCSVFileTables evaluates to false");	
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
			System.out.println();
			return false;
		}

		// for each expected table name, check if it is present in the list of
		// file entries
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Now checking whether each expected table name is present in the list of file entries");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Entering loop");
		for (String TableName : LoadConstants.EXPECTED_TABLES) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tCurrent iteration on: " + TableName);
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tEntering loop");
			boolean TableExists = false;
			for (CSVFileEntry FileEntry : FileEntries) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tCurrent iteration on: " + FileEntry.TargetTable);
				if (!TableName.equals(FileEntry.TargetTable)) continue;
				else System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tTarget table found");
				
				TableExists = true; // found a match
				break;
			}
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting loop");			
			// if the table name did not exist in list of CSV files, this check
			// fails.
			if (!TableExists) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Target table did not exist in list of CSV files");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "IsMatchCSVFileTables evaluates to false");	
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
				System.out.println();
				return false;
			}
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting loop");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting IsMatchCSVFileTables process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println();
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting IsExistsCSVFile process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tChecking if the given FileEntry " + FileEntry.FilePath + " exists");
	
		File CSVFileInfo = new File(FileEntry.FilePath);
		if(!CSVFileInfo.exists()) { 
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tGiven FileEntry does not exist");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsExistsCSVFile evaluates to false");	
			return false;
		}
		File CSVFileHeaderInfo = new File(FileEntry.HeaderPath);

		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tLooking up header info at " + FileEntry.HeaderPath);
		if(!CSVFileHeaderInfo.exists()) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tHeader info does not exist");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsExistsCSVFile evaluates to false");				
		}
		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting IsExistsCSVFile process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");	
		System.out.println();

		return CSVFileHeaderInfo.exists();
	}

	/**
	 * @param FileEntry
	 * @return
	 * @throws Exception
	 */
	public static CSVFileEntry ReadCSVFileColumnNames(CSVFileEntry FileEntry)
			throws Exception {
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting ReadCSVFileColumnNames process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tReading header information for " + FileEntry.FilePath);
		// 2. Read the header line of the CSV File.
		BufferedReader CSVFileReader = 
			new BufferedReader(new InputStreamReader(new FileInputStream(FileEntry.HeaderPath)));
		String HeaderRow = CSVFileReader.readLine();

		// 3. Extract the comma-separated columns names of the CSV File from its
		// header line.
		// Strip empty spaces around column names.
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tProcessing header line " + HeaderRow);
		String[] ColumnNames = HeaderRow.split(",");
		FileEntry.ColumnNames = new ArrayList<String>();
		for (String ColumnName : ColumnNames) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tAdding column " + ColumnName.trim() + " to FileEntry");
			FileEntry.ColumnNames.add(ColumnName.trim());
		}

		CSVFileReader.close();
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting ReadCSVFileColumnNames process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
		System.out.println();	
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting IsMatchCSVFileColumnNames process");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tChecking whether the table" + FileEntry.TargetTable + " matches one of the expected tables");
		// determine expected columns
		List<String> ExpectedColumns = null;
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIt is a P2Detection table");		
			ExpectedColumns = LoadConstants.EXPECTED_DETECTION_COLS;
		} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIt is a P2FrameMeta table");		
			ExpectedColumns = LoadConstants.EXPECTED_FRAME_META_COLS;
		} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIt is a P2ImageMeta table");		
			ExpectedColumns = LoadConstants.EXPECTED_IMAGE_META_COLS;
		} else {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tThe table doesn't match any of the expected tables, and is therefore invalid");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchCSVFileColumnNames evaluates to false");	
			// none of the table types match...invalid
			return false;
		}

		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNow, testing whether the expected and present column name counts are the same");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExpected: " + ExpectedColumns.size() + " Present: " + FileEntry.ColumnNames.size());

		// test if the expected and present column name counts are the same
		if (ExpectedColumns.size() != FileEntry.ColumnNames.size()) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tColumn count mismatch detected");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchCSVFileColumnNames evaluates to false");				
			return false;
		}

		// test of all expected names exist in the columns present
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNow, verifying that all expected names exist in the columns present");
		for (String ColumnName : ExpectedColumns) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tChecking for column name " + ColumnName);
			if (!FileEntry.ColumnNames.contains(ColumnName)) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tThis column name wasn't found");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchCSVFileColumnNames evaluates to false");					
				return false; // mismatch
			}
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting ReadCSVFileColumnNames process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
		System.out.println();	
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Starting CreateEmptyLoadDB process");
		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Initializing database entry for storing database properties");
		
		// initialize database entry for storing database properties
		DatabaseEntry DBEntry = new DatabaseEntry();
		DBEntry.DBName = JobID + "_LoadDB";
		DBEntry.DBGuid = UUID.randomUUID().toString();
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Database Name: " + DBEntry.DBName);
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Database ID: " + DBEntry.DBGuid);

		// initialize Sql Connection String to sql server
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Initializing Sql Connection String to sql server");
		
		StringBuilder ConnStr = new StringBuilder(SQL_SERVER);
		ConnStr.append(";databaseName=");
		ConnStr.append(DBEntry.DBName);
		ConnStr.append(";user=");
		ConnStr.append(SQL_USER);
		ConnStr.append(";password=");
		ConnStr.append(SQL_PASSWORD);
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + ConnStr.toString());
		
		// Create empty database instance
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Creating empty database instance");
		Connection SqlConn = null;
		try {
			String CreateDBConnStr = ConnStr.toString() + ";create=true";
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Connecting using string " + CreateDBConnStr);
			SqlConn = DriverManager.getConnection(CreateDBConnStr);
		} finally {
			if (SqlConn != null) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Disconnecting from database");				
				SqlConn.close();
			}
		}

		// update Sql Connection String to new create tables
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Adding created connection string to DBEntry instance " + DBEntry.DBGuid);
		DBEntry.ConnectionString = ConnStr.toString();

		// create tables
		SqlConn = null;

		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Now, creating tables for each of the three expected tables");
		try {
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// Create P2 Table
			Statement SqlCmd = SqlConn.createStatement();
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Creating P2 Detection table");
			SqlCmd.executeUpdate(LoadSql.CREATE_DETECTION_TABLE);
			// Create P2FrameMeta Table
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Creating P2 Frame Metadata table");
			SqlCmd.executeUpdate(LoadSql.CREATE_FRAME_META_TABLE);
			// Create P2ImageMeta Table
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Creating P2 Image Metadata table");
			SqlCmd.executeUpdate(LoadSql.CREATE_IMAGE_META_TABLE);

		} finally {
			if (SqlConn != null) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Disconnecting from database");	
				SqlConn.close();
			}
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting CreateEmptyLoadDB process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");
		System.out.println();	
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting LoadCSVFileIntoTable process");
		Connection SqlConn = null;
		try {
			// connect to database instance
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tConnecting to database instance " + DBEntry.ConnectionString);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// build bulk insert SQL command
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tBuilding bulk insert SQL command");	

			CallableStatement SqlCmd = 
				SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (?,?,?,?,?,?,?)");

			SqlCmd.setString(1, null);
			SqlCmd.setString(2, FileEntry.TargetTable.toUpperCase());
			SqlCmd.setString(3, FileEntry.FilePath);
			SqlCmd.setString(4, ",");
			SqlCmd.setString(5, null);
			SqlCmd.setString(6, null);
			SqlCmd.setShort(7, (short)0);
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t"+SqlCmd);	
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExecuting bulk insert command");	

			// execute bulk insert command
			SqlCmd.execute();

		} catch (Exception ex) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tBulk insert failed");	
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tLoadCSVFileIntoTable evaluates to false");	
			// bulk insert failed
			return false;
		} finally {
			if (SqlConn != null) SqlConn.close();
		}

		// bulk insert success
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting LoadCSVFileIntoTable process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
		System.out.println();	
		return true;
	}

	/**
	 * @param DBEntry
	 * @param FileEntry
	 * @return
	 */
	public static boolean UpdateComputedColumns(DatabaseEntry DBEntry,
			CSVFileEntry FileEntry) throws Exception {
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting UpdateComputedColumns process");
		Connection SqlConn = null;
		try {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tConnecting to database instance " + DBEntry.ConnectionString);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Detection table");
				// Update ZoneID
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdating ZoneID, with SQL command " + "UPDATE P2Detection SET zoneID = (\"dec\"+(90.0))/(0.0083333)");
				Statement SqlCmd = SqlConn.createStatement();
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET zoneID = (\"dec\"+(90.0))/(0.0083333)");

				// Update cx
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdating cx, with SQL command " + "UPDATE P2Detection SET cx = (COS(RADIANS(\"dec\"))*COS(RADIANS(ra)))");

				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cx = (COS(RADIANS(\"dec\"))*COS(RADIANS(ra)))");

				// Update cy
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdating cy, with SQL command " + "UPDATE P2Detection SET cy = COS(RADIANS(\"dec\"))*SIN(RADIANS(ra))");

				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cy = COS(RADIANS(\"dec\"))*SIN(RADIANS(ra))");

				// Update cz
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdating cz, with SQL command " + "UPDATE P2Detection SET cz = (SIN(RADIANS(\"dec\")))");

				
				SqlCmd.executeUpdate(
						"UPDATE P2Detection SET cz = (SIN(RADIANS(\"dec\")))");
				
			} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Frame Metadata table");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo columns to be updated for the Frame Metadata table");				
				// No columns to be updated for FrameMeta
			} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Image Metadata table");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo columns to be updated for the Image Metadata table");
				// No columns to be updated for ImageMeta
			} else {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tThe table doesn't match any of the expected tables, and is therefore invalid");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tUpdateComputedColumns evaluates to false");	
				// none of the table types matches...invalid
				return false;
			}

		} catch (Exception ex) {
			// update column failed
			return false;
		} finally {
			if (SqlConn != null) SqlConn.close();
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting UpdateComputedColumns process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
		System.out.println();
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting IsMatchTableRowCount process");
		// does the number of rows expected match the number of rows loaded
		Connection SqlConn = null;
		try {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tConnecting to database instance " + DBEntry.ConnectionString);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			Statement SqlCmd = SqlConn.createStatement();
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExecuting row count command " + "SELECT COUNT(*) FROM " + FileEntry.TargetTable);
			
			// execute row count command
			ResultSet Results = 
				SqlCmd.executeQuery("SELECT COUNT(*) FROM " + FileEntry.TargetTable);
			if (Results.next()) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tChecking if row count matches expected row count");
				
				// check if row count matches expected row count
				int RowCount = (int) Results.getInt(1);
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tRow count: " + RowCount + " Expected row count: " + FileEntry.RowCount);

				if(RowCount != FileEntry.RowCount) {
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tColumn count mismatch detected");
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchTableRowCount evaluates to false");	
				} else {
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting IsMatchTableRowCount process");		
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
					System.out.println();
				}
				return RowCount == FileEntry.RowCount;
			} else {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo row count returned");
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchTableRowCount evaluates to false");	
				return false; // error case!
			}
		} finally {
			if (SqlConn != null) {
				
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
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tStarting IsMatchTableColumnRanges process");
		// determine expected column ranges
		List<LoadConstants.ColumnRange> ExpectedColumnRanges = null;
		if ("P2Detection".equalsIgnoreCase(FileEntry.TargetTable)) {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Detection table");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tGetting expected table column range: " + LoadConstants.EXPECTED_DETECTION_COL_RANGES);
			ExpectedColumnRanges = LoadConstants.EXPECTED_DETECTION_COL_RANGES;
		} else if ("P2FrameMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			// No columns range values available for FrameMeta
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Frame Metadata table");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo columns range values available for P2 Frame Metadata");
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} else if ("P2ImageMeta".equalsIgnoreCase(FileEntry.TargetTable)) {
			// No columns range values available for ImageMeta
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tFileEntry target table is a P2 Image Metadata table");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo columns range values available for P2 Image Metadata");
			ExpectedColumnRanges = new ArrayList<LoadConstants.ColumnRange>();
		} else {
			// none of the table types matches...invalid
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tThe table doesn't match any of the expected tables, and is therefore invalid");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tIsMatchTableColumnRanges evaluates to false");	
			return false;
		}

		// connect to database instance
		Connection SqlConn = null;
		try {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tConnecting to database instance " + DBEntry.ConnectionString);
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);

			// For each column in available list, test if rows in table fall
			// outside expected range
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNow, checking each column in the available list, and testing if rows in table fall outside expected range");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tEntering loop");
			for (LoadConstants.ColumnRange Column : ExpectedColumnRanges) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tChecking for column name " + Column);
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tBuilding SQL command to count number of rows falling outside expected range");
				// build SQL command for error count
				String SqlStr = 
					String.format(
							"SELECT COUNT(*) FROM %1$s WHERE (%2$s < %3$s OR %2$s > %4$s) AND %2$s != -999",
							FileEntry.TargetTable, Column.ColumnName,
							Column.MinValue, Column.MaxValue);
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tExecuting SQL command: " + SqlStr);

				// execute range error count command
				Statement SqlCmd = SqlConn.createStatement();
				ResultSet Results = SqlCmd.executeQuery(SqlStr);
				if (Results.next()) {
					int ErrorCount = Results.getInt(1);
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tError count: " + ErrorCount);
					if (ErrorCount > 0) {
						System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tRange error found");
						System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tIsMatchTableColumnRanges evaluates to false");
						return false; // found a range error
					}
				} else {
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tNo range values returned");
					System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t\tIsMatchTableColumnRanges evaluates to false");
					return false; // error case
				}

			}
		} finally {
			if (SqlConn != null) SqlConn.close();
		}

		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tNo range errors found");
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExiting IsMatchTableColumnRanges process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\t-------------------------------------");
		System.out.println();
		return true; // no range errors found
	}

	/**
	 * @param DBEntry
	 */
	public static void CompactDatabase(DatabaseEntry DBEntry) throws Exception {
		// Shrink database instance
		System.out.println();	
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Starting CompactDatabase process");
		Connection SqlConn = null;
		try {
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Connecting to database instance " + DBEntry.ConnectionString);
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Iterating over tables in database, and running compression commands");
			System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Entering loop");
			SqlConn = DriverManager.getConnection(DBEntry.ConnectionString);
			for (String TableName : LoadConstants.EXPECTED_TABLES) {
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tCurrent table: " + TableName);
				CallableStatement SqlCmd = 
					SqlConn.prepareCall("CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?, ?, ?, ?, ?)");
				SqlCmd.setString(1, SQL_USER.toUpperCase());
				SqlCmd.setString(2, TableName.toUpperCase());
				SqlCmd.setShort(3, (short) 1);
				SqlCmd.setShort(4, (short) 1);
				SqlCmd.setShort(5, (short) 1);
				System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "\tExecuting SQL command: " + SqlCmd);
				SqlCmd.execute();
			}
		} finally {
			if (SqlConn != null) SqlConn.close();
		}
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "Exiting CompactDatabase process");		
		System.out.println("<TIME> " + (System.currentTimeMillis()-START_TIME) + "\t" + "-------------------------------------");
		System.out.println();		
	}
}
