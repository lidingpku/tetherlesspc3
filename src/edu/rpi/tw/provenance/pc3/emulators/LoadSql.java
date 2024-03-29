package edu.rpi.tw.provenance.pc3.emulators;

public class LoadSql {

	public static final String CREATE_DETECTION_TABLE = 
	  "CREATE TABLE P2Detection(" + 
	    "\"objID\" bigint NOT NULL, " +
	    "detectID bigint NOT NULL, " +
	    "ippObjID bigint NOT NULL, " +
	    "ippDetectID bigint NOT NULL, " +
	    "filterID smallint NOT NULL, " +
	    "imageID bigint NOT NULL, " +
	    "obsTime float NOT NULL DEFAULT -999, " +
	    "xPos real NOT NULL DEFAULT -999, " +
	    "yPos real NOT NULL DEFAULT -999, " +
	    "xPosErr real NOT NULL DEFAULT -999, " +
	    "yPosErr real NOT NULL DEFAULT -999, " +
	    "instFlux real NOT NULL DEFAULT -999, " +
	    "instFluxErr real NOT NULL DEFAULT -999, " +
	    "psfWidMajor real NOT NULL DEFAULT -999, " +
	    "psfWidMinor real NOT NULL DEFAULT -999, " +
	    "psfTheta real NOT NULL DEFAULT -999, " +
	    "psfLikelihood real NOT NULL DEFAULT -999, " +
	    "psfCf real NOT NULL DEFAULT -999, " +
	    "infoFlag int NOT NULL DEFAULT -999, " +
	    "htmID float NOT NULL DEFAULT -999, " +
	    "zoneID float NOT NULL DEFAULT -999, " +
	    "assocDate date NOT NULL DEFAULT '28881231', " +
	    "modNum smallint NOT NULL DEFAULT 0, " +
	    "ra float NOT NULL, " +
	    "\"dec\" float NOT NULL, " +
	    "raErr real NOT NULL DEFAULT 0, " +
	    "decErr real NOT NULL DEFAULT 0, " +
	    "cx float NOT NULL DEFAULT -999, " +
	    "cy float NOT NULL DEFAULT -999, " +
	    "cz float NOT NULL DEFAULT -999, " +
	    "peakFlux real NOT NULL DEFAULT -999, " +
	    "calMag real NOT NULL DEFAULT -999, " +
	    "calMagErr real NOT NULL DEFAULT -999, " +
	    "calFlux real NOT NULL DEFAULT -999, " +
	    "calFluxErr real NOT NULL DEFAULT -999, " +
	    "calColor real NOT NULL DEFAULT -999, " +
	    "calColorErr real NOT NULL DEFAULT -999, " +
	    "sky real NOT NULL DEFAULT -999, " +
	    "skyErr real NOT NULL DEFAULT -999, " +
	    "sgSep real NOT NULL DEFAULT -999, " +
	    "dataRelease smallint NOT NULL, " +
	    "CONSTRAINT PK_P2Detection_objID_detectID PRIMARY KEY " +
	    "( " +
	    "\"objID\", " +
	    "detectID " +
	    ")) ";
	    
	
	public static final String CREATE_FRAME_META_TABLE = 
	  "CREATE TABLE P2FrameMeta( " +
	  "frameID int NOT NULL PRIMARY KEY, " +
	  "surveyID smallint NOT NULL, " +
	  "filterID smallint NOT NULL, " +
	  "cameraID smallint NOT NULL, " +
	  "telescopeID smallint NOT NULL, " +
	  "analysisVer smallint NOT NULL, " +
	  "p1Recip smallint NOT NULL DEFAULT -999, " +
	  "p2Recip smallint NOT NULL DEFAULT -999, " +
	  "p3Recip smallint NOT NULL DEFAULT -999, " +
	  "nP2Images smallint NOT NULL DEFAULT -999, " +
	  "astroScat real NOT NULL DEFAULT -999, " +
	  "photoScat real NOT NULL DEFAULT -999, " +
	  "nAstRef int NOT NULL DEFAULT -999, " +
	  "nPhoRef int NOT NULL DEFAULT -999, " +
	  "expStart float NOT NULL DEFAULT -999, " +
	  "expTime real NOT NULL DEFAULT -999, " +
	  "airmass real NOT NULL DEFAULT -999, " +
	  "raBore float NOT NULL DEFAULT -999, " +
	  "decBore float NOT NULL DEFAULT -999 " +
	  ") ";
	
	public static final String CREATE_IMAGE_META_TABLE = 
	  "CREATE TABLE P2ImageMeta( " +
	  "imageID bigint NOT NULL PRIMARY KEY, " +
	  "frameID int NOT NULL, " +
	  "ccdID smallint NOT NULL, " +
	  "photoCalID int NOT NULL, " +
	  "filterID smallint NOT NULL, " +
	  "bias real NOT NULL DEFAULT -999, " +
	  "biasScat real NOT NULL DEFAULT -999, " +
	  "sky real NOT NULL DEFAULT -999, " +
	  "skyScat real NOT NULL DEFAULT -999, " +
	  "nDetect int NOT NULL DEFAULT -999, " +
	  "magSat real NOT NULL DEFAULT -999, " +
	  "completMag real NOT NULL DEFAULT -999, " +
	  "astroScat real NOT NULL DEFAULT -999, " +
	  "photoScat real NOT NULL DEFAULT -999, " +
	  "nAstRef int NOT NULL DEFAULT -999, " +
	  "nPhoRef int NOT NULL DEFAULT -999, " +
	  "nx smallint NOT NULL DEFAULT -999, " +
	  "ny smallint NOT NULL DEFAULT -999, " +
	  "psfFwhm real NOT NULL DEFAULT -999, " +
	  "psfModelID int NOT NULL DEFAULT -999, " +
	  "psfSigMajor real NOT NULL DEFAULT -999, " +
	  "psfSigMinor real NOT NULL DEFAULT -999, " +
	  "psfTheta real NOT NULL DEFAULT -999, " +
	  "psfExtra1 real NOT NULL DEFAULT -999, " +
	  "psfExtra2 real NOT NULL DEFAULT -999, " +
	  "apResid real NOT NULL DEFAULT -999, " +
	  "dapResid real NOT NULL DEFAULT -999, " +
	  "detectorID smallint NOT NULL DEFAULT -999, " +
	  "qaFlags int NOT NULL DEFAULT -999, " +
	  "detrend1 bigint NOT NULL DEFAULT -999, " +
	  "detrend2 bigint NOT NULL DEFAULT -999, " +
	  "detrend3 bigint NOT NULL DEFAULT -999, " +
	  "detrend4 bigint NOT NULL DEFAULT -999, " +
	  "detrend5 bigint NOT NULL DEFAULT -999, " +
	  "detrend6 bigint NOT NULL DEFAULT -999, " +
	  "detrend7 bigint NOT NULL DEFAULT -999, " +
	  "detrend8 bigint NOT NULL DEFAULT -999, " +
	  "photoZero real NOT NULL DEFAULT -999, " +
	  "photoColor real NOT NULL DEFAULT -999, " +
	  "projection1 varchar(8000) NOT NULL DEFAULT '-999', " +
	  "projection2 varchar(8000) NOT NULL DEFAULT '-999', " +
	  "crval1 float NOT NULL DEFAULT -999, " +
	  "crval2 float NOT NULL DEFAULT -999, " +
	  "crpix1 float NOT NULL DEFAULT -999, " +
	  "crpix2 float NOT NULL DEFAULT -999, " +
	  "pc001001 float NOT NULL DEFAULT -999, " +
	  "pc001002 float NOT NULL DEFAULT -999, " +
	  "pc002001 float NOT NULL DEFAULT -999, " +
	  "pc002002 float NOT NULL DEFAULT -999, " +
	  "polyOrder int NOT NULL DEFAULT -999, " +
	  "pca1x3y0 float NOT NULL DEFAULT -999, " +
	  "pca1x2y1 float NOT NULL DEFAULT -999, " +
	  "pca1x1y2 float NOT NULL DEFAULT -999, " +
	  "pca1x0y3 float NOT NULL DEFAULT -999, " +
	  "pca1x2y0 float NOT NULL DEFAULT -999, " +
	  "pca1x1y1 float NOT NULL DEFAULT -999, " +
	  "pca1x0y2 float NOT NULL DEFAULT -999, " +
	  "pca2x3y0 float NOT NULL DEFAULT -999, " +
	  "pca2x2y1 float NOT NULL DEFAULT -999, " +
	  "pca2x1y2 float NOT NULL DEFAULT -999, " +
	  "pca2x0y3 float NOT NULL DEFAULT -999, " +
	  "pca2x2y0 float NOT NULL DEFAULT -999, " +
	  "pca2x1y1 float NOT NULL DEFAULT -999, " +
	  "pca2x0y2 float NOT NULL DEFAULT -999 " +
	  ") ";

}