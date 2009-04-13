package edu.rpi.tw.provenance;


public class MyLogger {
	
	static  MyLogger  gLog = new MyLogger();
	public static MyLogger get(){
		return gLog;
	}
	
	public void log_assign( String myFunctionName, NameValue paramLeft, NameValue paramRight){
		log2("send", myFunctionName,"assign", new NameValue[]{paramRight});
		log2("recieve", myFunctionName,"assign", new NameValue[]{paramLeft});
		
	}
	
	public void log2(String operation, String myFunctionName, String subFunctionName, NameValue param){
		if (null!=param)
			log2(operation, myFunctionName,subFunctionName, new NameValue[]{param});
		else
			log2(operation, myFunctionName,subFunctionName, (NameValue[]) null);
	}
	
	public void log2(String operation, String myFunction, String subFunction, NameValue[] params){

		//DataSmartMap entry = new DataSmartMap();
		//entry.put("operation",operation);
		//entry.put("myFunctionName",myFunction);
		//entry.put("subFunctionName",subFunction);
		System.out.println(operation);
		System.out.println(myFunction);
		System.out.println(subFunction);

		//System.out.println(entry);
		
		for (int i=0; null!=params && i<params.length; i++){
			NameValue param = params[i];

			System.out.println(param.toString());
		}

	}
	public void log1(String operation, String myFunction, NameValue param){
		if (null!=param)
			log1(operation, myFunction, new NameValue[] {param});
		else
			log1(operation, myFunction, ( NameValue[]) null);
	}	
	
	public void log1(String operation, String myFunction, NameValue[] params){

		//DataSmartMap entry = new DataSmartMap();
		//entry.put("operation",operation);
		//entry.put("myFunction",myFunction);
		System.out.println(operation);
		System.out.println(myFunction);

		//System.out.println(entry);
		
		for (int i=0; null!=params && i<params.length; i++){
			NameValue param = params[i];

			System.out.println(param.toString());
		}

	}
}
