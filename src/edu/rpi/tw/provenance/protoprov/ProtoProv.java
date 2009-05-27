package edu.rpi.tw.provenance.protoprov;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class ProtoProv {
	
	public ProtoProv() {
		
	}
	
	public class Function {
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Function(String name, Collection<String> accounts) {
			this.accounts = accounts;
			this.name = name;
		}
		private String name;
		private Collection<String> accounts;		
	}
	
	public class Variable {
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
		public Variable(Object value, Collection<String> accounts) {
			this.accounts = accounts;
			this.value = value;
		}
		
		private Object value;
		private Collection<String> accounts;
		
	}
	
	public class Controller {
		public Collection<String> getAccounts() {
			return accounts;
		}
		public void setAccounts(Collection<String> accounts) {
			this.accounts = accounts;
		}
		public Controller(Collection<String> accounts) {
			this.accounts = accounts;
		}
		private Collection<String> accounts;		
	}
	
	public class Role {
		public Role(String label) {
			this.label = label;
		}
		
		public String getLabel() {
			return label;
		}

		public void setLabel(String label) {
			this.label = label;
		}

		private String label;
		
	}
	
	public class Group {
		public Group(String label) {
			this.label = label;
		}
		
		public String getLabel() {
			return label;
		}

		public void setLabel(String label) {
			this.label = label;
		}

		private String label;
		
	}
	
	public class Intersect {
		public Intersect(List<String> label) {
			Iterator <String> x = label.iterator();
			while(x.hasNext()) {
				String thisLabel = x.next();
				Group thisGroup = new Group(thisLabel);
				groups.add(thisGroup);
			}
		}
		
		public List<Group> groups;
		
	}

	public class Relation {
		public String getSource() {
			return source;
		}
		public void setSource(String source) {
			this.source = source;
		}

		public String getTarget() {
			return target;
		}		
		public void setTarget(String target) {
			this.target = target;
		}
		
		public Collection<String> getContexts() {
			return contexts;
		}
		public void setContexts(Collection<String> contexts) {
			this.contexts = contexts;
		}
		
		public String getRole() {
			return role.getLabel();
		}
		public void setRole(String newLabel) {
			role.setLabel(newLabel);
		}
		
		protected Role role;
		protected String source;
		protected String target;
		protected Collection<String> contexts;				

	}

	public class Usd extends Relation {
		public Usd(String source, String target, String role, Collection<String> contexts) {
			this.contexts = contexts;
			this.source = source;
			this.role = new Role(role);
			this.target = target;
		}
	}
	public class WGB extends Relation {
		public WGB(String source, String target, String role, Collection<String> contexts) {
			this.contexts = contexts;
			this.source = source;
			this.role = new Role(role);
			this.target = target;
		}
	}
	public class WTB extends Relation {
		public WTB(String source, String target, String role, Collection<String> contexts) {
			this.contexts = contexts;
			this.source = source;
			this.role = new Role(role);
			this.target = target;
		}
	}
	public class WDF extends Relation {
		public WDF(String source, String target, String role, Collection<String> contexts) {
			this.contexts = contexts;
			this.source = source;
			this.role = new Role(role);
			this.target = target;
		}
	}
	public class WCB extends Relation {
		public WCB(String source, String target, String role, Collection<String> contexts) {
			this.contexts = contexts;
			this.source = source;
			this.role = new Role(role);
			this.target = target;
		}
	}

	

	
	
	
	 private List<Group> groups = new ArrayList<Group>();
	 private List<Intersect> intersects = new ArrayList<Intersect>();

	 public Map <String, Function> fxnStore = new HashMap<String, Function>();
	 public Map <String, Variable> varStore = new HashMap<String, Variable>();
	 public Map <String, Controller> ctlStore = new HashMap<String, Controller>();
	 public Map <String, Usd> usdStore = new HashMap<String, Usd>();
	 public Map <String, WGB> wgbStore = new HashMap<String, WGB>();
	 public Map <String, WCB> wcbStore = new HashMap<String, WCB>();
	 public Map <String, WDF> wdfStore = new HashMap<String, WDF>();
	 public Map <String, WTB> wtbStore = new HashMap<String, WTB>();
	
	 
	 //public Map<String, Role> RelationRoles = new HashMap<String, Role>();
	 
	 public Map <String, Collection<String>> usdLink = new HashMap<String, Collection<String>>();
	 public Map <String, Collection<String>> wgbLink = new HashMap<String, Collection<String>>();
	 public Map <String, Collection<String>> wcbLink = new HashMap<String, Collection<String>>();
	 public Map <String, Collection<String>> wdfLink = new HashMap<String, Collection<String>>();
	 public Map <String, Collection<String>> wtbLink = new HashMap<String, Collection<String>>();
	
	
	public void AddGroup(String id) {
		Group g = new Group(id);
		groups.add(g);
	}
	
	public void AddIntersect(List<String> ids) {
		Intersect i = new Intersect(ids);
		intersects.add(i);
	}

	
	
	public List<Group> getGroups() {
		return groups;
	}
	
	public List<Intersect> getIntersects() {
		return intersects;
	}
	
	
	public void AddVariable(String id, Object value, Collection<String> accountIds) {	
		Variable variable = new Variable(value, accountIds);
		varStore.put(id, variable);
	}
	
	public void AddFunction(String id, String name, Collection<String> accountIds) {	
		Function function = new Function(name, accountIds);
		fxnStore.put(id, function);
	}
	
	public void AddController(String id, Collection<String> accountIds) {
		Controller controller = new Controller(accountIds);
		ctlStore.put(id, controller);
	}
	
	public void AddUsed(String id, String function, String role, String variable, Collection <String> accountIds) {

		Collection<String> UsdIds = new ArrayList<String>();
		if(usdLink.containsKey(function))
			UsdIds = usdLink.get(function);
		Usd used = new Usd(function, variable, role, accountIds);
		//RelationRoles.put(id, new Role(role));
		usdStore.put(id, used);
		UsdIds.add(id);
		usdLink.put(function, UsdIds);
		
	}
	
	public void AddWGB(String id, String variable, String role, String function, Collection <String> accountIds) {

		Collection<String> WGBIds = new ArrayList<String>();
		if(wgbLink.containsKey(variable))
			WGBIds = wgbLink.get(variable);
		WGB wgb = new WGB(variable, function, role, accountIds);
		//RelationRoles.put(id, new Role(role));
		wgbStore.put(id, wgb);
		WGBIds.add(id);
		wgbLink.put(variable, WGBIds);
		
	}

	public void AddWCB(String id, String function, String role, String controller, Collection <String> accountIds) {
		Collection<String> WCBIds = new ArrayList<String>();
		if(wcbLink.containsKey(function))
			WCBIds = wcbLink.get(function);
		WCB wcb = new WCB(function, controller, role, accountIds);
		//RelationRoles.put(id, new Role(role));
		wcbStore.put(id, wcb);
		WCBIds.add(id);
		wcbLink.put(function, WCBIds);

	}	
	
	public void AddWTB(String id, String function, String role, String function2, Collection <String> accountIds) {

		Collection<String> WTBIds = new ArrayList<String>();
		if(wtbLink.containsKey(function))
			WTBIds = wtbLink.get(function);
		WTB wtb = new WTB(function, function2, role, accountIds);
		//RelationRoles.put(id, new Role(role));
		wtbStore.put(id, wtb);
		WTBIds.add(id);
		wtbLink.put(function, WTBIds);
		
	}

	public void AddWDF(String id, String function, String role, String controller, Collection <String> accountIds) {
		Collection<String> WCBIds = new ArrayList<String>();
		if(wcbLink.containsKey(function))
			WCBIds = wcbLink.get(function);
		WDF wdf = new WDF(function, controller, role, accountIds);
		//RelationRoles.put(id, new Role(role));
		wdfStore.put(id,wdf);
		WCBIds.add(id);
		wcbLink.put(function, WCBIds);

	}	
	
}
