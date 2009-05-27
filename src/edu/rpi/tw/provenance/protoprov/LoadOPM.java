package edu.rpi.tw.provenance.protoprov;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.openprovenance.model.Account;
import org.openprovenance.model.AccountId;
import org.openprovenance.model.Accounts;
import org.openprovenance.model.Agent;
import org.openprovenance.model.Agents;
import org.openprovenance.model.Artifact;
import org.openprovenance.model.Artifacts;
import org.openprovenance.model.CausalDependencies;
import org.openprovenance.model.OPMDeserialiser;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.Process;
import org.openprovenance.model.Processes;
import org.openprovenance.model.Used;
import org.openprovenance.model.WasControlledBy;
import org.openprovenance.model.WasDerivedFrom;
import org.openprovenance.model.WasGeneratedBy;
import org.openprovenance.model.WasTriggeredBy;

import edu.rpi.tw.provenance.pc3.util.IdGenerator;

public class LoadOPM {

	public static ProtoProv loadOPMGraph(File f) throws Exception {
		OPMDeserialiser d = new OPMDeserialiser();
		OPMGraph g2 = d.deserialiseOPMGraph(f);

		ProtoProv p = new ProtoProv();
		
		
		if(g2.getAccounts() != null) p = readOPMAccounts(g2.getAccounts(), p);
		if(g2.getAgents() != null) p = readOPMAgents(g2.getAgents(), p);
		if(g2.getProcesses() != null) p = readOPMProcesses(g2.getProcesses(), p);
		if(g2.getArtifacts() != null) p = readOPMArtifacts(g2.getArtifacts(), p);
		if(g2.getCausalDependencies() != null) p = readOPMCausalDependencies(g2.getCausalDependencies(), p);

		/*
		
		Accounts accounts = generateOPMAccounts(p);
		Processes processes = generateOPMProcesses(p);
		Artifacts artifacts =  generateOPMArtifacts(p);
		Agents agents = new Agents();		
		CausalDependencies causalDependencies =  generateOPMCausalDependencies(p);
*/
		return p;
		
	}

	
	public static ProtoProv readOPMCausalDependencies(CausalDependencies cds, ProtoProv p) {
		Iterator<Object> agentIter = cds.getUsedOrWasGeneratedByOrWasTriggeredBy().iterator();
		IdGenerator idGen = new IdGenerator();
		
		while(agentIter.hasNext()) {
			Object thisCdObj = agentIter.next();
			String objClass = thisCdObj.getClass().getSimpleName();
			
			if(objClass.equals("Used")) {
				Used thisCd = (Used) thisCdObj;
				
				String roleValue = "";
				
				if(thisCd.getRole() == null)
					roleValue = "none defined";
				else
					roleValue = thisCd.getRole().getValue();
				
				Process thisProcess = (Process)thisCd.getEffect().getId();
				Artifact thisArtifact = (Artifact)thisCd.getCause().getId();

				p.AddUsed(idGen.newRelationId("Usd"), 
						uriLocalName(thisProcess.getId()),
						roleValue,
						uriLocalName(thisArtifact.getId()), 
						convertAccountListFormat(thisCd.getAccount()));				
			}
			else if(objClass.equals("WasGeneratedBy")) {
				WasGeneratedBy thisCd = (WasGeneratedBy) thisCdObj;
				String roleValue = "";
				
				if(thisCd.getRole() == null)
					roleValue = "none defined";
				else
					roleValue = thisCd.getRole().getValue();
				
				System.out.println(convertAccountListFormat(thisCd.getAccount()));
				Artifact thisArtifact2 = (Artifact)thisCd.getEffect().getId();
				Process thisProcess2 = (Process)thisCd.getCause().getId();
				
				
				
				p.AddWGB(idGen.newRelationId("WGB"),
						uriLocalName(thisArtifact2.getId()),
						roleValue,
						uriLocalName(thisProcess2.getId()), 
						convertAccountListFormat(thisCd.getAccount()));				
			}
			else if(objClass.equals("WasTriggeredBy")) {
				WasTriggeredBy thisCd = (WasTriggeredBy) thisCdObj;
				
				
				Process process1 = (Process)thisCd.getEffect().getId();
				Process process2 = (Process)thisCd.getCause().getId();
				
				p.AddWTB(idGen.newRelationId("WTB"), 
						uriLocalName(process1.getId()), 
						"Undefined Role",
						uriLocalName(process2.getId()),
						convertAccountListFormat(thisCd.getAccount()));				
			}
			else if(objClass.equals("WasDerivedFrom")) {
				WasDerivedFrom thisCd = (WasDerivedFrom) thisCdObj;
				Artifact wdfArtifact1 = (Artifact)thisCd.getEffect().getId();
				Artifact wdfArtifact2 = (Artifact)thisCd.getCause().getId();
				
				p.AddWDF(idGen.newRelationId("WDF"), 
						uriLocalName(wdfArtifact1.getId()), 
						"Undefined Role", 
						uriLocalName(wdfArtifact2.getId()), 
						convertAccountListFormat(thisCd.getAccount()));								
			}
			else if(objClass.equals("WasControlledBy")) {
				WasControlledBy thisCd = (WasControlledBy) thisCdObj;
				
				String roleValue = "";
				
				if(thisCd.getRole() == null)
					roleValue = "none defined";
				else
					roleValue = thisCd.getRole().getValue();
				
				Artifact wcbArtifact = (Artifact)thisCd.getEffect().getId();
				Agent wcbAgent = (Agent)thisCd.getCause().getId();
				
				p.AddWCB(idGen.newRelationId("WCB"), 
						uriLocalName(wcbArtifact.getId()),
						roleValue, 
						uriLocalName(wcbAgent.getId()),
						convertAccountListFormat(thisCd.getAccount()));				
			}
			else { }
		}
		return p;
	}

	
	public static ProtoProv readOPMAccounts(Accounts accts, ProtoProv p) {
		Iterator <Account> accountIter = accts.getAccount().iterator();
		while(accountIter.hasNext()) {
			Account thisAccount = accountIter.next();
			p.AddGroup(uriLocalName(thisAccount.getId()));
		}
		return p;
	}

	
	public static ProtoProv readOPMAgents(Agents agents, ProtoProv p) {
		Iterator<Agent> agentIter = agents.getAgent().iterator();
		while(agentIter.hasNext()) {
			Agent thisAgent = agentIter.next();
			List <String> accountNames = convertAccountListFormat(thisAgent.getAccount());
			
			String artifactId = uriLocalName(thisAgent.getId());		
			String artifactValue = thisAgent.getValue().toString();
			p.AddFunction(artifactId, artifactValue, accountNames);
		}
		return p;
	}
	
	public static ProtoProv readOPMArtifacts(Artifacts arts, ProtoProv p) {
		Iterator<Artifact> artifactIter = arts.getArtifact().iterator();
		while(artifactIter.hasNext()) {
			Artifact thisArtifact = artifactIter.next();
			List <String> accountNames = convertAccountListFormat(thisArtifact.getAccount());
			
			String artifactId = uriLocalName(thisArtifact.getId());		
			String artifactValue = thisArtifact.getValue().toString();
			p.AddFunction(artifactId, artifactValue, accountNames);
		}
		return p;
	}
	
	public static ProtoProv readOPMProcesses(Processes prcs, ProtoProv p) {
		Iterator<Process> processIter = prcs.getProcess().iterator();
		while(processIter.hasNext()) {
			Process thisProcess = processIter.next();
			List <String> accountNames = convertAccountListFormat(thisProcess.getAccount());
			String processId = uriLocalName(thisProcess.getId());	
			String processValue = thisProcess.getValue().toString();
			p.AddFunction(processId, processValue, accountNames);
		}
		return p;
	}

	public static String uriLocalName(String oldURI) {
		String [] splitURI = oldURI.split("#");
		if(splitURI.length > 1) {
			return splitURI[1];
		}
		return splitURI[0];
	}
	public static List<String> convertAccountListFormat(List<AccountId> oldList) {
		List<String> newList = new ArrayList<String>();
		Iterator <AccountId> oldListIter = oldList.iterator();
		while(oldListIter.hasNext()) {
			Account thisAccount = (Account)oldListIter.next().getId();
			newList.add(thisAccount.getId());
		}
		return newList;
	}
}
