package edu.rpi.tw.provenance.protoprov;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.openprovenance.model.Account;
import org.openprovenance.model.Accounts;
import org.openprovenance.model.Agents;
import org.openprovenance.model.Artifact;
import org.openprovenance.model.Artifacts;
import org.openprovenance.model.CausalDependencies;
import org.openprovenance.model.OPMFactory;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.Overlaps;
import org.openprovenance.model.Process;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Role;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasGeneratedBy;

public class GenOPM {

	private OPMFactory gOPMFactory = new OPMFactory();
	private Map<String, Artifact> artifactMap = new HashMap <String, Artifact>();
	private Map<String, Process> processMap = new HashMap <String, Process>();
	
	public OPMGraph genOPMGraph(ProtoProv p) throws Exception {
		Accounts accounts = generateOPMAccounts(p);
		Processes processes = generateOPMProcesses(p);
		Artifacts artifacts =  generateOPMArtifacts(p);
		Agents agents = new Agents();		
		CausalDependencies causalDependencies =  generateOPMCausalDependencies(p);
		
		OPMGraph thisOPMGraph = gOPMFactory.newOPMGraph(accounts, processes, artifacts, agents, causalDependencies);

		return thisOPMGraph;
	}
	/*
	public void printOPMGraph() throws Exception {
		Accounts accounts = generateOPMAccounts();
		Processes processes = generateOPMProcesses();
		Artifacts artifacts =  generateOPMArtifacts();
			//new Artifacts();
		Agents agents = new Agents();		
		CausalDependencies causalDependencies =  generateOPMCausalDependencies();
		//new CausalDependencies();
		
		OPMGraph thisOPMGraph = gOPMFactory.newOPMGraph(accounts, processes, artifacts, agents, causalDependencies);
		
		
		
		StringWriter sw = new StringWriter();
		
		System.out.println(OPMSerialiser.getThreadOPMSerialiser().serialiseOPMGraph(sw, thisOPMGraph, true));
	}
	*/
	private Accounts generateOPMAccounts(ProtoProv p) {
		List<Account> accountList = new ArrayList<Account>();
	
		Iterator <ProtoProv.Group> groupList = p.getGroups().iterator();
		
		while(groupList.hasNext()) {
			ProtoProv.Group thisGroup = groupList.next();
			accountList.add(gOPMFactory.newAccount(thisGroup.getLabel()));
		}
		
		List<Overlaps> overlapsList = new ArrayList<Overlaps>();

		Iterator <ProtoProv.Intersect> intersectIter = p.getIntersects().iterator();
		while(intersectIter.hasNext()) {
			ProtoProv.Intersect thisIntersect = intersectIter.next();
			Iterator <ProtoProv.Group> ctlIter = thisIntersect.groups.iterator();
			List <Account> overlaps = new ArrayList<Account>();
			while(ctlIter.hasNext()) {
				String thisCtl = ctlIter.next().getLabel();
				Account thisAccount = gOPMFactory.newAccount(thisCtl);
				overlaps.add(thisAccount);
			}
			Overlaps newOverlaps = gOPMFactory.newOverlaps(overlaps);
			overlapsList.add(newOverlaps);
		}
	    
	    
		Accounts accounts = gOPMFactory.newAccounts(accountList, overlapsList);
	    return accounts;
	}
	
	private Processes generateOPMProcesses(ProtoProv p) {
		Processes processes = new Processes();

		Iterator <String> fxnIter = p.fxnStore.keySet().iterator();
		while(fxnIter.hasNext()) {
			String thisFxnKey = fxnIter.next();		
			ProtoProv.Function thisFxn = p.fxnStore.get(thisFxnKey);
			String thisFxnValue = thisFxn.getName().toString();
			
			Iterator<String> thisFxnAccountIds = thisFxn.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisFxnAccountIds.hasNext()) {
				String thisAccountStr = thisFxnAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			Process curProcess = gOPMFactory.newProcess(thisFxnKey, accounts, thisFxnValue);
			processMap.put(thisFxnKey, curProcess);
			processes.getProcess().add(curProcess);
		}
		return processes;
	}

	private Artifacts generateOPMArtifacts(ProtoProv p) {
		Artifacts artifacts = new Artifacts();

		Iterator <String> varIter = p.varStore.keySet().iterator();
		while(varIter.hasNext()) {
			String thisVarKey = varIter.next();		
			ProtoProv.Variable thisVar = p.varStore.get(thisVarKey);
			String thisVarValue = thisVar.getValue().toString();
			
			Iterator<String> thisFxnAccountIds = thisVar.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisFxnAccountIds.hasNext()) {
				String thisAccountStr = thisFxnAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			Artifact curArtifact = gOPMFactory.newArtifact(thisVarKey, accounts, thisVarValue);
			artifactMap.put(thisVarKey, curArtifact);
			artifacts.getArtifact().add(curArtifact);
		}

		return artifacts;
	}
	
	
	private CausalDependencies generateOPMCausalDependencies(ProtoProv p) {
		CausalDependencies causalDependencies = new CausalDependencies();

		Iterator <String> wgbIter = p.wgbStore.keySet().iterator();
		while(wgbIter.hasNext()) {
			String thisWgbKey = wgbIter.next();
			ProtoProv.WGB thisWGB = p.wgbStore.get(thisWgbKey);
			Artifact thisArtifact = artifactMap.get(thisWGB.getVariable());
			Process thisProcess = processMap.get(thisWGB.getFunction());
			Role thisWgbRole = gOPMFactory.newRole(p.RelationRoles.get(thisWgbKey).getLabel());

			Iterator<String> thisWgbAccountIds = thisWGB.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisWgbAccountIds.hasNext()) {
				String thisAccountStr = thisWgbAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			WasGeneratedBy curWasGeneratedBy = gOPMFactory.newWasGeneratedBy(thisArtifact, thisWgbRole, thisProcess, accounts);
			causalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(curWasGeneratedBy);		
		}

		Iterator <String> usdIter = p.usdStore.keySet().iterator();
		while(usdIter.hasNext()) {
			String thisUsdKey = usdIter.next();
			ProtoProv.Usd thisUsed = p.usdStore.get(thisUsdKey);
			Artifact thisArtifact = artifactMap.get(thisUsed.getVariable());
			Process thisProcess = processMap.get(thisUsed.getFunction());
			Role thisUsdRole = gOPMFactory.newRole(thisUsed.getRole());

			Iterator<String> thisUsdAccountIds = thisUsed.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisUsdAccountIds.hasNext()) {
				String thisAccountStr = thisUsdAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			Used curUsed = gOPMFactory.newUsed(thisProcess, thisUsdRole, thisArtifact, accounts);
			causalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(curUsed);		
		}	
		return causalDependencies;
	}

}
