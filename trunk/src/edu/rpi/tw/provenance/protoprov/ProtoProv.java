package edu.rpi.tw.provenance.protoprov;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProtoProv {
	
	public static class IDTracker {
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
	
	public static class Function {
		public String getScope() {
			return scope;
		}
		public void setScope(String scope) {
			this.scope = scope;
		}
		public Object getValue() {
			return value;
		}
		public void setValue(Object value) {
			this.value = value;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Function(String scope, Object value, Collection<String> accounts) {
			this.accounts = accounts;
			this.scope = scope;
			this.value = value;
		}
		private String scope;
		private Object value;
		private Collection<String> accounts;		
	}
	
	public static class Variable {
		public String getCategory() {
			return category;
		}
		public void setCategory(String scope) {
			this.scope = scope;
		}
		public String getScope() {
			return scope;
		}
		public void setScope(String scope) {
			this.scope = scope;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public Object getValue() {
			return value;
		}
		public void setValue(Object value) {
			this.value = value;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Variable(String category, String scope, String type, Object value, Collection<String> accounts) {
			this.category = category;
			this.accounts = accounts;
			this.type = type;
			this.scope = scope;
			this.value = value;
		}
		
		private String category;
		private String scope;
		private String type;
		private Object value;
		private Collection<String> accounts;
		
	}
	
	public static class Controller {
		public Object getValue() {
			return value;
		}
		public void setValue(Object value) {
			this.value = value;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Controller(Object value, Collection<String> accounts) {
			this.accounts = accounts;
			this.value = value;
		}
		private Object value;
		private Collection<String> accounts;		
	}
	
	public static class Usd {
		public String getRole() {
			return role;
		}
		public void setRole(String role) {
			this.role = role;
		}
		public String getVariable() {
			return variable;
		}
		public void setVariable(String variable) {
			this.variable = variable;
		}
		public String getFunction() {
			return function;
		}
		public void setFunction(String function) {
			this.function = function;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Usd(String function, String variable, String role, Collection<String> accounts) {
			this.accounts = accounts;
			this.function = function;
			this.role = role;
			this.variable = variable;
		}
		private String role;
		private String variable;
		private String function;
		private Collection<String> accounts;		
	}

	public static class WGB {
		public String getRole() {
			return role;
		}
		public void setRole(String role) {
			this.role = role;
		}
		public String getVariable() {
			return variable;
		}
		public void setVariable(String variable) {
			this.variable = variable;
		}
		public String getFunction() {
			return function;
		}
		public void setFunction(String function) {
			this.function = function;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public WGB(String function, String variable, String role, Collection<String> accounts) {
			this.accounts = accounts;
			this.function = function;
			this.role = role;
			this.variable = variable;
		}
		private String role;
		private String variable;
		private String function;
		private Collection<String> accounts;		
	}
	
	public static class WCB {
		public String getRole() {
			return role;
		}
		public void setRole(String role) {
			this.role = role;
		}
		public String getFunction() {
			return function;
		}
		public void setFunction(String function) {
			this.function = function;
		}
		public String getController() {
			return controller;
		}
		public void setController(String controller) {
			this.controller = controller;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public WCB(String function, String controller, String role, Collection<String> accounts) {
			this.accounts = accounts;
			this.function = function;
			this.role = role;
			this.controller = controller;
		}
		private String role;
		private String function;
		private String controller;
		private Collection<String> accounts;		
	}
	
	static private List<String> groups = new ArrayList<String>();
	static private List<List<String>> intersects = new ArrayList<List<String>>();

	static private IDTracker fxnIdt = new IDTracker();
	static private IDTracker varIdt = new IDTracker();
	static private IDTracker ctlIdt = new IDTracker();
	static private IDTracker usdIdt = new IDTracker();
	static private IDTracker wgbIdt = new IDTracker();
	static private IDTracker wcbIdt = new IDTracker();
	
	static public Map <String, Function> fxnStore = new HashMap<String, Function>();
	static public Map <String, Variable> varStore = new HashMap<String, Variable>();
	static public Map <String, Controller> ctlStore = new HashMap<String, Controller>();
	static public Map <String, Usd> usdStore = new HashMap<String, Usd>();
	static public Map <String, WGB> wgbStore = new HashMap<String, WGB>();
	static public Map <String, WCB> wcbStore = new HashMap<String, WCB>();
		
	static public Map <String, Collection<String>> usdLink = new HashMap<String, Collection<String>>();
	static public Map <String, Collection<String>> wgbLink = new HashMap<String, Collection<String>>();
	static public Map <String, Collection<String>> wcbLink = new HashMap<String, Collection<String>>();
	
	
	public static void AddGroups(List<String> grps, List<List<String>> intr) {
		groups = grps;
		intersects = intr;
	}
	
	public static List<String> getGroups() {
		return groups;
	}
	
	public static List<List<String>> getIntersects() {
		return intersects;
	}
	
	
	public static String AddVariable(String category, String id, String scope, String type, Object value, Collection<String> accountIds) {	
		String assignedID = varIdt.getIdentifierTicket(id);
		assignedID = assignedID + "_" + scope;
		Variable variable = new Variable(category, scope, type, value, accountIds);
		varStore.put(assignedID, variable);
		return assignedID;
	}
	
	public static String AddFunction(String id, String scope, Object value, Collection<String> accountIds) {	
		String assignedID = fxnIdt.getIdentifierTicket(id);
		assignedID = assignedID + "_" + scope;
		Function function = new Function(scope, value, accountIds);
		fxnStore.put(assignedID, function);
		return assignedID;
	}
	
	public static String AddController(String id, Object value, Collection<String> accountIds) {
		String assignedID = ctlIdt.getIdentifierTicket(id);
		Controller controller = new Controller(value, accountIds);
		ctlStore.put(assignedID, controller);
		return assignedID;
	}
	
	public static String AddUsed(String id, String function, String role, String variable, Collection <String> accountIds) {
		String assignedId = usdIdt.getIdentifierTicket(id);
		Collection<String> UsdIds = new ArrayList<String>();
		if(usdLink.containsKey(function))
			UsdIds = usdLink.get(function);
		Usd used = new Usd(function, variable, role, accountIds);
		usdStore.put(assignedId, used);
		UsdIds.add(assignedId);
		usdLink.put(function, UsdIds);
		
		return assignedId;
	}
	
	public static String AddWGB(String id, String variable, String role, String function, Collection <String> accountIds) {
		String assignedId = wgbIdt.getIdentifierTicket(id);
		Collection<String> WGBIds = new ArrayList<String>();
		if(wgbLink.containsKey(variable))
			WGBIds = wgbLink.get(variable);
		WGB wgb = new WGB(function, variable, role, accountIds);
		wgbStore.put(assignedId, wgb);
		WGBIds.add(assignedId);
		wgbLink.put(variable, WGBIds);
		
		return assignedId;
	}

	public static String AddWCB(String id, String function, String role, String controller, Collection <String> accountIds) {
		String assignedId = wcbIdt.getIdentifierTicket(id);
		Collection<String> WCBIds = new ArrayList<String>();
		if(wcbLink.containsKey(function))
			WCBIds = wcbLink.get(function);
		WCB wcb = new WCB(function, controller, role, accountIds);
		wcbStore.put(assignedId, wcb);
		WCBIds.add(assignedId);
		wcbLink.put(function, WCBIds);

		return assignedId;
	}	
}
