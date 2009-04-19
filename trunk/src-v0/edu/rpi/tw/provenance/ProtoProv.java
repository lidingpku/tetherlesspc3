package edu.rpi.tw.provenance;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import org.openprovenance.model.Account;
import org.openprovenance.model.AccountId;
import org.openprovenance.model.Accounts;
import org.openprovenance.model.Agent;
import org.openprovenance.model.AgentId;
import org.openprovenance.model.Agents;
import org.openprovenance.model.Artifact;
import org.openprovenance.model.ArtifactId;
import org.openprovenance.model.Artifacts;
import org.openprovenance.model.CausalDependencies;
import org.openprovenance.model.OPMFactory;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.OPMSerialiser;
import org.openprovenance.model.OTime;
import org.openprovenance.model.ObjectFactory;
import org.openprovenance.model.Overlaps;
import org.openprovenance.model.Process;
import org.openprovenance.model.ProcessId;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Role;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasControlledBy;
import org.openprovenance.model.WasGeneratedBy;

public class ProtoProv {
	
	// OPM Accounts
//	Collection<String> owner;
	static ObjectFactory gObjectFactory = new ObjectFactory();	
	static OPMFactory gOPMFactory = new OPMFactory();
	//static OPMSerialiser gOPMSerialiser = new OPMSerialiser();
	
	static List<String> GrpIDs = new ArrayList<String>();
	static List<String> CtlIDs = new ArrayList<String>();
	
	static Collection<Collection<String>> intersects = new ArrayList<Collection<String>>();
	
	// OPM Processes
	static Collection<String>				FxnIDs = new ArrayList<String>();
	static Map<String,Object> 			    FxnIDtoValueMap = new HashMap<String,Object>();
	static Map<String, Collection<String>> FxnIDtoAccountsMap = new HashMap<String,Collection<String>>();
	
	// FxnToVar: (OPM: Used, PML: AntecedentList)
	static Map<String, Collection<UUID>> FxnToVarUUIDs = new HashMap<String, Collection<UUID>>();
	static Map<UUID, String> FxnToVarRoles = new HashMap<UUID, String>();
	static Map<UUID, String> FxnToVarIds = new HashMap<UUID, String>();
	static Map<UUID, Collection<String>> FxnToVarAccountIds = new HashMap<UUID, Collection<String>>();

	// FxnToCtl: (OPM: WasControlledBy, PML: hasInferenceEngine)
	static Map<String, Collection<UUID>> FxnToCtlUUIDs = new HashMap<String, Collection<UUID>>();
	static Map<UUID, String> FxnToCtlRoles = new HashMap<UUID, String>();
	static Map<UUID, String> FxnToCtlIds = new HashMap<UUID, String>();
	static Map<UUID, Collection<String>> FxnToCtlAccountIds = new HashMap<UUID, Collection<String>>();
	
	// OPM Artifacts
	static Collection<String>				VarIDs = new ArrayList<String>();	
	static Map<String,Object> 				VarIDtoValueMap = new HashMap<String,Object>();
	static Map<String, Collection<String>> VarIDtoAccountsMap = new HashMap<String,Collection<String>>();
	
	// VarToFxn:  (OPM: wasGeneratedBy, PML: isConsequentOf)
	static Map<String, Collection<UUID>> VarToFxnUUIDs = new HashMap<String, Collection<UUID>>();
	static Map<UUID, String> VarToFxnRoles = new HashMap<UUID, String>();
	static Map<UUID, String> VarToFxnIds = new HashMap<UUID, String>();
	static Map<UUID, Collection<String>> VarToFxnAccountIds = new HashMap<UUID, Collection<String>>();

	
	// OPM Agents
	static Map<String, Collection<String>> CtlIDtoAccountsMap = new HashMap<String,Collection<String>>();
	
	
	public static void AddVariable(String varId, Object value, Collection<String> accountIds) {	
		VarIDs.add(varId);
		VarIDtoValueMap.put(varId, value);
		VarIDtoAccountsMap.put(varId, accountIds);
	}
	
	public static void AddFunction(String fxnId, Object value, Collection<String> accountIds) {	
		FxnIDs.add(fxnId);
		FxnIDtoValueMap.put(fxnId, value);
		FxnIDtoAccountsMap.put(fxnId, accountIds);
	}
	
	public static void AddController(String ctlId, Collection<String> accountIds) {	
		CtlIDs.add(ctlId);
		CtlIDtoAccountsMap.put(ctlId, accountIds);
	}
	
	public static void AddGroup(String id) {
		GrpIDs.add(id);
	}
	
	public static void AddIntersect(Collection<String> owners) {
		intersects.add(owners);
	}
	
	public static void AddVarToFxn(String varId, String fxnId, String role, Collection<String> grpIds) {	
		UUID thisUUID = UUID.randomUUID();
		
		if(!VarToFxnUUIDs.containsKey(varId))
			VarToFxnUUIDs.put(varId, new ArrayList<UUID>());
		Collection<UUID> thisUUIDs = VarToFxnUUIDs.get(varId);
		thisUUIDs.add(thisUUID);
		VarToFxnUUIDs.put(varId, thisUUIDs);
		
		VarToFxnRoles.put(thisUUID, role);
		VarToFxnIds.put(thisUUID, fxnId);
		VarToFxnAccountIds.put(thisUUID, grpIds);
	}
	
	public static void AddFxnToVar(String fxnId, String varId, String role, Collection<String> grpIds) {	
		UUID thisUUID = UUID.randomUUID();
		if(!FxnToVarUUIDs.containsKey(fxnId))
			FxnToVarUUIDs.put(fxnId, new ArrayList<UUID>());
		Collection<UUID> thisUUIDs = FxnToVarUUIDs.get(fxnId);
		thisUUIDs.add(thisUUID);
		FxnToVarUUIDs.put(fxnId, thisUUIDs);
		
		FxnToVarRoles.put(thisUUID, role);
		FxnToVarIds.put(thisUUID, varId);
		FxnToVarAccountIds.put(thisUUID, grpIds);
	}
	
	public static void AddFxnToCtl(String fxnId, String ctlId, String role, Collection<String> grpIds) {	
		UUID thisUUID = UUID.randomUUID();
		if(!FxnToCtlUUIDs.containsKey(fxnId))
			FxnToCtlUUIDs.put(fxnId, new ArrayList<UUID>());
		Collection<UUID> thisUUIDs = FxnToCtlUUIDs.get(fxnId);
		thisUUIDs.add(thisUUID);
		FxnToCtlUUIDs.put(fxnId, thisUUIDs);
		
		FxnToCtlRoles.put(thisUUID, role);
		FxnToCtlIds.put(thisUUID, ctlId);
		FxnToCtlAccountIds.put(thisUUID, grpIds);
	}
}
