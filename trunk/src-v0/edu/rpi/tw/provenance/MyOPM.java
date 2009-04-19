package edu.rpi.tw.provenance;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;


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
import org.openprovenance.model.ObjectFactory;
import org.openprovenance.model.Process;
import org.openprovenance.model.ProcessId;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Role;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasControlledBy;
import org.openprovenance.model.WasDerivedFrom;
import org.openprovenance.model.WasGeneratedBy;
import org.openprovenance.model.WasTriggeredBy;

public class MyOPM {
	static  MyOPM  gOPM = new MyOPM();

	static ObjectFactory gObjectFactory = new ObjectFactory();
	static OPMSerialiser gOPMSerialiser;


	static Artifacts gArtifacts = new Artifacts();
	static Processes gProcesses = new Processes();
	static Agents gAgents = new Agents();
	static Accounts gAccounts = new Accounts();
	static CausalDependencies gCausalDependencies =  new CausalDependencies();
	
	public static MyOPM get(){
		return gOPM;
	}
	
	public static Artifact addArtifact(String name, Collection <Account> accounts, Object value) {
		Artifact newArtifact = gObjectFactory.createArtifact();
	
		newArtifact.setId(name);
		newArtifact.setValue(value);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newArtifact.getAccount().add(thisAccountId);
		}
		
		gArtifacts.getArtifact().add(newArtifact);
		return newArtifact;
	}

	public static Process addProcess(String name, Collection <Account> accounts, Object value) {
		Process newProcess = gObjectFactory.createProcess();
		
		newProcess.setId(name);
		newProcess.setValue(value);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newProcess.getAccount().add(thisAccountId);
		}
		
		gProcesses.getProcess().add(newProcess);
		return newProcess;
	}

	public static Agent addAgent(String name, Collection <Account> accounts, Object value) {
		Agent newAgent = gObjectFactory.createAgent();
		
		newAgent.setId(name);
		newAgent.setValue(name);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newAgent.getAccount().add(thisAccountId);
		}
		
		gAgents.getAgent().add(newAgent);
		return newAgent;
	}
	
	public static Account addAccount(String name) {
		Account newAccount = OPMFactory.getFactory().newAccount(name);
		gAccounts.getAccount().add(newAccount);
		return newAccount;
	}
	

	
	public static Used addUsed(Process process,
			String role, Artifact artifact, Collection <Account> accounts) {
		Used newUsed = gObjectFactory.createUsed();

		Role roleObj = gObjectFactory.createRole();
		roleObj.setValue(role);
		ProcessId processId = gObjectFactory.createProcessId();
		processId.setId(process.getValue());
		ArtifactId artifactId = gObjectFactory.createArtifactId();
				
		newUsed.setCause(artifactId);
		newUsed.setEffect(processId);
		newUsed.setRole(roleObj);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newUsed.getAccount().add(thisAccountId);
		}
		
		gCausalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(newUsed);
		return newUsed;
	}
	
	public static WasControlledBy addWasControlledBy(Agent agent,
			String role, Process process, Collection <Account> accounts) {
		WasControlledBy newWasControlledBy = gObjectFactory.createWasControlledBy();

		Role roleObj = gObjectFactory.createRole();
		roleObj.setValue(role);
		AgentId agentId = gObjectFactory.createAgentId();
		agentId.setId(agent.getValue());
		ProcessId processId = gObjectFactory.createProcessId();
		processId.setId(process.getValue());		
		
		newWasControlledBy.setCause(agentId);
		newWasControlledBy.setEffect(processId);
		newWasControlledBy.setRole(roleObj);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newWasControlledBy.getAccount().add(thisAccountId);
		}
		
		gCausalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(newWasControlledBy);
		return newWasControlledBy;
	}

	public static WasDerivedFrom addWasDerivedFrom(Artifact artifact,
			Artifact artifact2, Collection <Account> accounts) {
		WasDerivedFrom newWasDerivedFrom = gObjectFactory.createWasDerivedFrom();

		ArtifactId artifactId = gObjectFactory.createArtifactId();
		artifactId.setId(artifact.getValue());
		ArtifactId artifactId2 = gObjectFactory.createArtifactId();
		artifactId2.setId(artifact2.getValue());
		
		newWasDerivedFrom.setCause(artifactId);
		newWasDerivedFrom.setEffect(artifactId2);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newWasDerivedFrom.getAccount().add(thisAccountId);
		}
		
		gCausalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(newWasDerivedFrom);
		return newWasDerivedFrom;
	}

	public static WasGeneratedBy addWasGeneratedBy(Process process,
			String role, Process artifact, Collection <Account> accounts) {
		WasGeneratedBy newWasGeneratedBy = gObjectFactory.createWasGeneratedBy();

		Role roleObj = gObjectFactory.createRole();
		roleObj.setValue(role);
		ProcessId processId = gObjectFactory.createProcessId();
		processId.setId(process.getValue());
		ArtifactId artifactId = gObjectFactory.createArtifactId();
		artifactId.setId(artifact.getValue());
		
		newWasGeneratedBy.setCause(processId);
		newWasGeneratedBy.setEffect(artifactId);
		newWasGeneratedBy.setRole(roleObj);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newWasGeneratedBy.getAccount().add(thisAccountId);
		}
		
		gCausalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(newWasGeneratedBy);
		return newWasGeneratedBy;
	}

	public static WasTriggeredBy addWasTriggeredBy(Process process,
			Process process2, Collection <Account> accounts) {
		WasTriggeredBy newWasTriggeredBy = gObjectFactory.createWasTriggeredBy();

		ProcessId processId = gObjectFactory.createProcessId();
		processId.setId(process.getValue());
		ProcessId processId2 = gObjectFactory.createProcessId();
		processId2.setId(process2.getValue());
		
		newWasTriggeredBy.setCause(processId);
		newWasTriggeredBy.setEffect(processId2);
		
		Iterator <Account>accountIter = accounts.iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			AccountId thisAccountId = gObjectFactory.createAccountId();
			thisAccountId.setId(thisAccount.getId());
			newWasTriggeredBy.getAccount().add(thisAccountId);
		}
		
		gCausalDependencies.getUsedOrWasGeneratedByOrWasTriggeredBy().add(newWasTriggeredBy);
		return newWasTriggeredBy;
	}
	
	public static OPMGraph generateOPMGraph() {
		OPMGraph newOPMGraph = gObjectFactory.createOPMGraph();
		newOPMGraph.setAccounts(gAccounts);
		newOPMGraph.setAgents(gAgents);
		newOPMGraph.setArtifacts(gArtifacts);
		newOPMGraph.setCausalDependencies(gCausalDependencies);
		newOPMGraph.setProcesses(gProcesses);
		
		return newOPMGraph;
	}
	
	public static void printOPMGraph(OPMGraph thisOPMGraph) throws Exception {
		StringWriter sw = new StringWriter();
		gOPMSerialiser.serialiseOPMGraph(sw, thisOPMGraph, true);
	}
}
