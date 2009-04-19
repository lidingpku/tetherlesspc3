package edu.rpi.tw.provenance;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.openprovenance.model.Account;
import org.openprovenance.model.Accounts;
import org.openprovenance.model.Agents;
import org.openprovenance.model.Artifact;
import org.openprovenance.model.Artifacts;
import org.openprovenance.model.CausalDependencies;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.OPMSerialiser;
import org.openprovenance.model.Overlaps;
import org.openprovenance.model.Process;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Role;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasGeneratedBy;

public class OPMGen extends ProtoProv {

	
	public static void printOPMGraph() throws Exception {
		Accounts accounts = generateOPMAccounts();
		Processes processes = generateOPMProcesses();
		Artifacts artifacts = generateOPMArtifacts();
		Agents agents = gObjectFactory.createAgents();
//		Agents agents = generateOPMAgents();		
		CausalDependencies causalDependencies = generateOPMCausalDependencies();
		
		OPMGraph thisOPMGraph = gOPMFactory.newOPMGraph(accounts, processes, artifacts, agents, causalDependencies);
		
		
		thisOPMGraph.setAccounts(accounts);
		//thisOPMGraph.setAgents(agents);
		thisOPMGraph.setArtifacts(artifacts);
		thisOPMGraph.setCausalDependencies(causalDependencies);
		thisOPMGraph.setProcesses(processes);
		
		StringWriter sw = new StringWriter();
		
		System.out.println(OPMSerialiser.getThreadOPMSerialiser().serialiseOPMGraph(sw, thisOPMGraph, true));
	}
	
	private static Accounts generateOPMAccounts() {
		List<Account> accountList = getAccountList(GrpIDs);
		List<Overlaps> overlapsList = new ArrayList<Overlaps>();

		Iterator <Collection<String>> intersectIter = intersects.iterator();
		while(intersectIter.hasNext()) {
			Collection<String> thisIntersect = intersectIter.next();
			Iterator <String> ctlIter = thisIntersect.iterator();
			Collection <Account> overlaps = new ArrayList<Account>();
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
		Iterator <String>iterGroups = FxnIDs.iterator();
		while(iterGroups.hasNext()) {
			String curIter = iterGroups.next();
			String value = FxnIDtoValueMap.get(curIter).toString();
			List<Account> accounts = getAccountList((List<String>) FxnIDtoAccountsMap.get(curIter));
			Process curProcess = gOPMFactory.newProcess(curIter, accounts, value);
			processes.getProcess().add(curProcess);
		}
		return processes;
	}
	
	private static Artifacts generateOPMArtifacts() {
		Artifacts artifacts = new Artifacts();
		Iterator <String>iterGroups = VarIDs.iterator();
		while(iterGroups.hasNext()) {
			String curIter = iterGroups.next();
			String value = VarIDtoValueMap.get(curIter).toString();
			List<Account> accounts = getAccountList((List<String>) VarIDtoAccountsMap.get(curIter));
			Artifact curArtifact = gOPMFactory.newArtifact(curIter, accounts, value);
			artifacts.getArtifact().add(curArtifact);
		}
		return artifacts;
	}
	
	private static CausalDependencies generateOPMCausalDependencies() {
		CausalDependencies causalDependencies = new CausalDependencies();

		Iterator <String>VarToFxnIter = VarToFxnUUIDs.keySet().iterator();
		while(VarToFxnIter.hasNext()) {
			String curArtifactId = VarToFxnIter.next();
			String curArtifactValue = VarIDtoValueMap.get(curArtifactId).toString();
			List<Account> artAccounts = getAccountList((List<String>) VarIDtoAccountsMap.get(curArtifactId));
			Artifact artifact = gOPMFactory.newArtifact(curArtifactId, artAccounts, curArtifactValue);

			Collection<UUID> curVarToFxnUUIDs = VarToFxnUUIDs.get(curArtifactId);
			Iterator <UUID> UUIDIter = curVarToFxnUUIDs.iterator();
			while(UUIDIter.hasNext()) {
				UUID curUUID = UUIDIter.next();
				Role role = gOPMFactory.newRole(VarToFxnRoles.get(curUUID));
				String curProcessId = VarToFxnIds.get(curUUID);
				String curProcessValue = FxnIDtoValueMap.get(curProcessId).toString();
				List<Account> prcAccounts = getAccountList((List<String>) FxnIDtoAccountsMap.get(curProcessId));
				Process process = gOPMFactory.newProcess(curProcessId, prcAccounts, curProcessValue);
				
				List<Account> wgbAccounts = getAccountList((List<String>) VarToFxnAccountIds.get(curUUID));
				WasGeneratedBy curWasGeneratedBy = gOPMFactory.newWasGeneratedBy(artifact, role, process, wgbAccounts);
				causalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(curWasGeneratedBy);		
			}
		}
	Iterator <String>FxnToVarIter = FxnToVarUUIDs.keySet().iterator();
	while(FxnToVarIter.hasNext()) {
		String curProcessId = FxnToVarIter.next();
		String curProcessValue = FxnIDtoValueMap.get(curProcessId).toString();
		List<Account> prcAccounts = getAccountList((List<String>) FxnIDtoAccountsMap.get(curProcessId));
		Process process = gOPMFactory.newProcess(curProcessId, prcAccounts, curProcessValue);

		Collection<UUID> curFxnToVarUUIDs = FxnToVarUUIDs.get(curProcessId);
		Iterator <UUID> UUIDIter = curFxnToVarUUIDs.iterator();
		while(UUIDIter.hasNext()) {
			UUID curUUID = UUIDIter.next();
			Role role = gOPMFactory.newRole(FxnToVarRoles.get(curUUID));
			
			String curArtifactId = FxnToVarIds.get(curUUID);
			String curArtifactValue = VarIDtoValueMap.get(curArtifactId).toString();
			List<Account> artAccounts = getAccountList((List<String>) VarIDtoAccountsMap.get(curArtifactId));
			Artifact artifact = gOPMFactory.newArtifact(curArtifactId, artAccounts, curArtifactValue);
			
			List<Account> usedAccounts = getAccountList((List<String>) FxnToVarAccountIds.get(curUUID));
			Used curUsed = gOPMFactory.newUsed(process, role, artifact, usedAccounts);
			causalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(curUsed);		
		}
	}
	return causalDependencies;
}

}
