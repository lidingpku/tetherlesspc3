package info.ipaw.pc3.PSLoadWorkflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;


import org.inference_web.pml.v2.pmlj.IWInferenceStep;
import org.inference_web.pml.v2.pmlj.IWNodeSet;
import org.inference_web.pml.v2.pmlp.IWInferenceRule;
import org.inference_web.pml.v2.pmlp.IWInformation;
import org.inference_web.pml.v2.pmlp.IWSourceUsage;
import org.inference_web.pml.v2.util.PMLObjectManager;
import org.inference_web.pml.v2.vocabulary.PMLJ;
import org.inference_web.pml.v2.vocabulary.PMLP;
import org.openprovenance.model.CausalDependencies;
import org.openprovenance.model.OPMFactory;
import org.openprovenance.model.ObjectFactory;

public class ProtoPML {
	public static String Namespace = "http://www.cs.rpi.edu/~michaj6/PC3-3.owl#";
	
	public static Map<UUID, ProtoInfStep> ProtoInfStepStore = new HashMap<UUID, ProtoInfStep>();
	public static Map<UUID, ProtoNS> ProtoNSStore = new HashMap<UUID, ProtoNS>();

	
	public static Map<String, Integer> ProcessIDCounter = new HashMap<String, Integer>();
	
	public static class ProtoInfStep {
		public String Process;
		public String Agent;
		public List<UUID> Antecedents;
		
		public ProtoInfStep (String Process, String Agent, List<UUID> Antecedents) {
			this.Process = Process;
			this.Agent = Agent;
			this.Antecedents = Antecedents;
		}
	}
	
	public static class ProtoNS {
		public String conclusion;
		public String identifier;
		public UUID linkToInfStep;
		
		public ProtoNS (String conclusion, String identifier, UUID linkToInfStep) {
			this.conclusion = conclusion;
			this.identifier = identifier;
			this.linkToInfStep = linkToInfStep;
		}
	}
	
	public static String getIdentifierTicket(String identifier) {
		Integer counter = 0;
		String ticket = identifier;
		if(ProcessIDCounter.containsKey(identifier)) {
			counter = ProcessIDCounter.get(identifier);
			counter++;
			ProcessIDCounter.put(identifier, counter);
			ticket += counter.toString();			
		}
		else {
			ProcessIDCounter.put(identifier, counter);
			ticket += counter.toString();			
		}
		return ticket;
	}
	
	public static UUID addNewProtoInfStep(String Process, String Agent, List<UUID> Antecedents) {
		ProtoInfStep newProtoInfStep = new ProtoInfStep(Process, Agent, Antecedents);
		UUID newUUID = UUID.randomUUID();
		ProtoInfStepStore.put(newUUID, newProtoInfStep);
		return newUUID;
	}

	public static UUID addNewProtoNS(String conclusion, String identifier, UUID linkToInfStep) {
		String URI_ID = Namespace + getIdentifierTicket(identifier);
		ProtoNS newProtoNS = new ProtoNS(conclusion, URI_ID, linkToInfStep);
		UUID newUUID = UUID.randomUUID();
		ProtoNSStore.put(newUUID, newProtoNS);
		return newUUID;
	}
	
	public static String getConclusionFromProtoNS(UUID target) {
		ProtoNS targetProtoNS = ProtoNSStore.get(target);
		String retStr = targetProtoNS.conclusion;
		return retStr;
	}
	
	public static void generateOPMGraph(UUID concludingNS_UUID) {
		ObjectFactory objectFactory = new ObjectFactory();
		 CausalDependencies x = objectFactory.createCausalDependencies();
		 
	}
	
	
	public static void generatePMLProof(UUID concludingNS_UUID) {
		IWNodeSet concludingNode = createNodeSet(concludingNS_UUID, 0);
		
		// get NodeSet's content on screen or save it to a file
  		System.out.println(PMLObjectManager.printPMLObjectToString(concludingNode));

	}
	
	private static IWNodeSet createNodeSet(UUID NS_UUID, int tabIndex) {
		ProtoNS NS = ProtoNSStore.get(NS_UUID);
		String conclusionRawString = NS.conclusion;
		String nsURI = NS.identifier;
		UUID linkToInfStep = NS.linkToInfStep;
		
		ProtoInfStep InfStep = ProtoInfStepStore.get(linkToInfStep);
		String ruleURI = InfStep.Process;
		String engineURI = InfStep.Agent;
		
		String tabs = "";
		for(int i = 0; i < tabIndex; i++)
			tabs += "\t";
		
		//System.out.println(tabs+ "CONCLUSION: " + conclusionRawString);
		//System.out.println(tabs+ "NS URI: " + nsURI);
		//System.out.println(tabs+ "PROCESS: " + ruleURI);
		//System.out.println(tabs+ "AGENT: " + engineURI);
		
		// create Information instance and assign property values
		IWInformation conclusion = null;
		if (conclusionRawString != null){
			conclusion = (IWInformation)PMLObjectManager.createPMLObject(PMLP.Information_lname);
			if (conclusionRawString != null ) {
				conclusion.setHasRawString(conclusionRawString);
			}
		}

		// create NodeSet with specified ontology and class
		IWNodeSet ns = (IWNodeSet)PMLObjectManager.createPMLObject(PMLJ.NodeSet_lname);
		// assign NodeSet name
		ns.setIdentifier(PMLObjectManager.getObjectID(nsURI));
		// assign NodeSet conclusion
		ns.setHasConclusion(conclusion);  	
		
		// create InferenceStep instance and assign property values
		IWInferenceStep infStep = (IWInferenceStep)PMLObjectManager.createPMLObject( PMLJ.InferenceStep_lname);
		infStep.setHasIndex(0);
		infStep.setHasInferenceRule(ruleURI); 
		IWInferenceRule rule= (IWInferenceRule)PMLObjectManager.createPMLObject(PMLP.DeclarativeRule_lname);
		rule.setHasName(ruleURI);
		rule.setIdentifier(PMLObjectManager.getObjectID(Namespace+ruleURI));
		infStep.setHasInferenceRule(rule);
		infStep.setHasInferenceEngine(Namespace+engineURI);

		List <IWNodeSet>antecedents = new ArrayList<IWNodeSet>();
		List<UUID> Antecedents = InfStep.Antecedents;
		if(Antecedents != null) {
			Iterator<UUID> antecedentIter = Antecedents.iterator();
			while(antecedentIter.hasNext()) {
				UUID thisUUID = antecedentIter.next();
				IWNodeSet thisNodeSet = createNodeSet(thisUUID, tabIndex+1);
				antecedents.add(thisNodeSet);
			}
			infStep.setHasAntecedentList(antecedents);
		}
		ArrayList <IWInferenceStep>infSteps = new ArrayList<IWInferenceStep>();
		infSteps.add(infStep); 	
		ns.setIsConsequentOf(infSteps);
		
		return ns;
	}
	
}
