package edu.rpi.tw.provenance;

import info.ipaw.pc3.PSLoadWorkflow.ProtoPML.ProtoInfStep;
import info.ipaw.pc3.PSLoadWorkflow.ProtoPML.ProtoNS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.inference_web.pml.v2.pmlj.IWInferenceStep;
import org.inference_web.pml.v2.pmlj.IWNodeSet;
import org.inference_web.pml.v2.pmlp.IWInferenceRule;
import org.inference_web.pml.v2.pmlp.IWInformation;
import org.inference_web.pml.v2.util.PMLObjectManager;
import org.inference_web.pml.v2.vocabulary.PMLJ;
import org.inference_web.pml.v2.vocabulary.PMLP;

public class PMLGen extends ProtoProv {
	
	public static String Namespace = "http://www.cs.rpi.edu/~michaj6/PC3-7.owl#";
	
	public static void generatePMLProof(String concludingNS) {
		IWNodeSet concludingNode = createNodeSet(concludingNS, 0);
		
		// get NodeSet's content on screen or save it to a file
  		System.out.println(PMLObjectManager.printPMLObjectToString(concludingNode));

	}
	
	private static IWNodeSet createNodeSet(String idNS, int tabIndex) {
		
		String conclusionRawString = VarIDtoValueMap.get(idNS).toString();
		String nsURI = Namespace + idNS;
		
		Collection <UUID> linksToInfStep = new ArrayList<UUID> ();
		if(VarToFxnUUIDs.containsKey(idNS))
			linksToInfStep = VarToFxnUUIDs.get(idNS);
		Iterator <UUID> infStepUUIDs = linksToInfStep.iterator();

		// create Information instance and assign property values
		IWInformation conclusion = null;
		if (conclusionRawString != null){
			conclusion = (IWInformation)PMLObjectManager.createPMLObject(PMLP.Information_lname);
			if (conclusionRawString != null ) {
				conclusion.setHasRawString(conclusionRawString);
			}
		}

		String tabs = "";
		for(int i = 0; i < tabIndex; i++)
			tabs += "\t";
		
		// create NodeSet with specified ontology and class
		IWNodeSet ns = (IWNodeSet)PMLObjectManager.createPMLObject(PMLJ.NodeSet_lname);
		// assign NodeSet name
		ns.setIdentifier(PMLObjectManager.getObjectID(nsURI));
		// assign NodeSet conclusion
		ns.setHasConclusion(conclusion);  	
		
		int infStepCounter = 0;

		ArrayList <IWInferenceStep>infSteps = new ArrayList<IWInferenceStep>();
		while(infStepUUIDs.hasNext()) {
			UUID thisKey = infStepUUIDs.next();
			String fxnId = VarToFxnIds.get(thisKey);
			Collection <UUID> ctlUUIDs = FxnToCtlUUIDs.get(fxnId);
			UUID selectedIter = ctlUUIDs.iterator().next();
			String engineURI = FxnToCtlIds.get(selectedIter);			
			String ruleURI = FxnIDtoValueMap.get(fxnId).toString();
		
			//System.out.println(tabs+ "CONCLUSION: " + conclusionRawString);
			//System.out.println(tabs+ "NS URI: " + nsURI);
			//System.out.println(tabs+ "PROCESS: " + ruleURI);
			//System.out.println(tabs+ "AGENT: " + engineURI);
			
			// create InferenceStep instance and assign property values
			IWInferenceStep infStep = (IWInferenceStep)PMLObjectManager.createPMLObject( PMLJ.InferenceStep_lname);
			infStep.setHasIndex(infStepCounter++);
			IWInferenceRule rule= (IWInferenceRule)PMLObjectManager.createPMLObject(PMLP.DeclarativeRule_lname);
			rule.setHasName(ruleURI);
			rule.setIdentifier(PMLObjectManager.getObjectID(Namespace+ruleURI));
			infStep.setHasInferenceRule(rule);
			infStep.setHasInferenceEngine(Namespace+engineURI);

			Collection <UUID> thisFxnToVarUUIDs = new ArrayList<UUID>();
			if(FxnToVarUUIDs.containsKey(fxnId))
				thisFxnToVarUUIDs = FxnToVarUUIDs.get(fxnId);
			
			Iterator <UUID> FxnToVarIter = thisFxnToVarUUIDs.iterator();
			List <IWNodeSet>antecedents = new ArrayList<IWNodeSet>();
			while(FxnToVarIter.hasNext()) {
				UUID thisIter = FxnToVarIter.next();
				String thisVarId = FxnToVarIds.get(thisIter);
				IWNodeSet thisNodeSet = createNodeSet(thisVarId, tabIndex+1);
				antecedents.add(thisNodeSet);
			}
			infStep.setHasAntecedentList(antecedents);
			infSteps.add(infStep); 	
		}
		ns.setIsConsequentOf(infSteps);	
		return ns;	
	}
}
