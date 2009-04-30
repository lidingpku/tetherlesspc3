package edu.rpi.tw.provenance.protoprov;

import java.io.StringWriter;
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
import org.openprovenance.model.OPMSerialiser;
import org.openprovenance.model.Overlaps;
import org.openprovenance.model.Process;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Role;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasGeneratedBy;

public class GenOPM extends ProtoProv {

	private static OPMFactory gOPMFactory = new OPMFactory();
	private static Map<String, Artifact> artifactMap = new HashMap <String, Artifact>();
	private static Map<String, Process> processMap = new HashMap <String, Process>();
	
	public static void printOPMGraph() throws Exception {
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
	
	private static Accounts generateOPMAccounts() {
		List<Account> accountList = getAccountList(getGroups());
		List<Overlaps> overlapsList = new ArrayList<Overlaps>();

		Iterator <List<String>> intersectIter = getIntersects().iterator();
		while(intersectIter.hasNext()) {
			List<String> thisIntersect = intersectIter.next();
			Iterator <String> ctlIter = thisIntersect.iterator();
			List <Account> overlaps = new ArrayList<Account>();
			while(ctlIter.hasNext()) {
				String thisCtl = ctlIter.next();
				Account thisAccount = gOPMFactory.newAccount(thisCtl);
				overlaps.add(thisAccount);
			}
			Overlaps newOverlaps = gOPMFactory.newOverlaps(overlaps);
			overlapsList.add(newOverlaps);
		}
	    
	    
		Accounts accounts = gOPMFactory.newAccounts(accountList, overlapsList);
	    return accounts;
	}
	
	
	private static List<Account> getAccountList(List<String> accountNames) {
		List<Account> accountList = new ArrayList<Account>();
		Iterator <String> accountIdIter = accountNames.iterator();
		while(accountIdIter.hasNext()) {
			String thisAccountId = accountIdIter.next();
			Account curAccount = gOPMFactory.newAccount(thisAccountId);
			accountList.add(curAccount);
		}
		return accountList;
	}
	
	private static Processes generateOPMProcesses() {
		Processes processes = new Processes();

		Iterator <String> fxnIter = fxnStore.keySet().iterator();
		while(fxnIter.hasNext()) {
			String thisFxnKey = fxnIter.next();		
			Function thisFxn = fxnStore.get(thisFxnKey);
			String thisFxnValue = thisFxn.getScope() + "_" + thisFxn.getValue().toString();
			String thisFxnScope = thisFxn.getScope();
			String finalId = thisFxnKey + "_" + thisFxnScope; 
			
			Iterator<String> thisFxnAccountIds = thisFxn.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisFxnAccountIds.hasNext()) {
				String thisAccountStr = thisFxnAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			Process curProcess = gOPMFactory.newProcess(finalId, accounts, thisFxnValue);
			processMap.put(thisFxnKey, curProcess);
			processes.getProcess().add(curProcess);
		}
		return processes;
	}

	private static Artifacts generateOPMArtifacts() {
		Artifacts artifacts = new Artifacts();

		Iterator <String> varIter = varStore.keySet().iterator();
		while(varIter.hasNext()) {
			String thisVarKey = varIter.next();		
			Variable thisVar = varStore.get(thisVarKey);
			String thisVarValue = thisVar.getValue().toString();
			String thisVarType = thisVar.getType();
			String thisVarScope = thisVar.getScope();
			String finalId = thisVarKey + "_" + thisVarScope; 
			String finalValue = thisVarScope + "_" + thisVarType + "_" + thisVarValue;
			
			Iterator<String> thisFxnAccountIds = thisVar.getAccounts().iterator();
			List<Account> accounts = new ArrayList<Account>();
			while(thisFxnAccountIds.hasNext()) {
				String thisAccountStr = thisFxnAccountIds.next();
				Account thisAccount = gOPMFactory.newAccount(thisAccountStr);
				accounts.add(thisAccount);
			}
			Artifact curArtifact = gOPMFactory.newArtifact(finalId, accounts, finalValue);
			artifactMap.put(thisVarKey, curArtifact);
			artifacts.getArtifact().add(curArtifact);
		}

		return artifacts;
	}
	
	
	private static CausalDependencies generateOPMCausalDependencies() {
		CausalDependencies causalDependencies = new CausalDependencies();

		Iterator <String> wgbIter = wgbStore.keySet().iterator();
		while(wgbIter.hasNext()) {
			String thisWgbKey = wgbIter.next();
			WGB thisWGB = wgbStore.get(thisWgbKey);
			Artifact thisArtifact = artifactMap.get(thisWGB.getVariable());
			Process thisProcess = processMap.get(thisWGB.getFunction());
			Role thisWgbRole = gOPMFactory.newRole(thisWGB.getRole());

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

		Iterator <String> usdIter = usdStore.keySet().iterator();
		while(usdIter.hasNext()) {
			String thisUsdKey = usdIter.next();
			Usd thisUsed = usdStore.get(thisUsdKey);
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
