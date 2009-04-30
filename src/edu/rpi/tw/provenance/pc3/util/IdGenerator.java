package edu.rpi.tw.provenance.pc3.util;

import java.util.HashMap;
import java.util.Map;

public class IdGenerator {
		
	public class IDTracker {
		private Map<String, Integer> ProcessIDCounter = new HashMap<String, Integer>();

		public IDTracker() {
			
		}
		
		public String getIdentifierTicket(String identifier) {
			Integer counter = 0;
			String ticket = identifier;
			if(ProcessIDCounter.containsKey(identifier)) {
				counter = ProcessIDCounter.get(identifier);
				counter++;
				ProcessIDCounter.put(identifier, counter);
				ticket += "_";
				ticket += counter.toString();			
			}
			else {
				ProcessIDCounter.put(identifier, counter);
				ticket += "_";
				ticket += counter.toString();			
			}
			return ticket;
		}		
	}
	
	public IDTracker variableIDs = new IDTracker();
	public IDTracker functionIDs = new IDTracker();
	public IDTracker RelationIds = new IDTracker();

	public IdGenerator () {
		
	}
	
	
	
	
	public String newVariableId(String scope, String type, String ID) {
		String newID = "";
		if(scope == null) {
			newID = scope + "_";
		}
		newID += ID;
		String retID = variableIDs.getIdentifierTicket(newID);
		return retID;
	}
	
	public String newFunctionId(String scope, String ID) {
		String newID = "";
		if(scope == null) {
			newID = scope + "_";
		}
		newID += ID;
		String retID = functionIDs.getIdentifierTicket(newID);
		return retID;
	}
	
	public String newRelationId(String ID) {
		String retID = RelationIds.getIdentifierTicket(ID);
		return retID;
	}	

}
