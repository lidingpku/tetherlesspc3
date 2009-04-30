package edu.rpi.tw.provenance.protoprov;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.inference_web.pml.v2.pmlj.IWInferenceStep;
import org.inference_web.pml.v2.pmlj.IWNodeSet;
import org.inference_web.pml.v2.pmlp.IWInferenceRule;
import org.inference_web.pml.v2.pmlp.IWInformation;
import org.inference_web.pml.v2.util.PMLObjectManager;
import org.inference_web.pml.v2.vocabulary.PMLJ;
import org.inference_web.pml.v2.vocabulary.PMLP;

public class GenPML {
	
	public String Namespace = "http://www.cs.rpi.edu/~michaj6/PC3/PML-A.owl#";
	
	public IWNodeSet generatePMLProof(ProtoProv p, String concludingNS) {
		IWNodeSet concludingNode = createNodeSet(p, concludingNS);
		
		// get NodeSet's content on screen or save it to a file
  		System.out.println(PMLObjectManager.printPMLObjectToString(concludingNode));

  		return concludingNode;
	}
	
	private IWNodeSet createNodeSet(ProtoProv p, String idNS) {
		ProtoProv.Variable curVariable = p.varStore.get(idNS);
		
		String conclusionRawString = curVariable.getValue().toString();
		String nsURI = Namespace + idNS;
		
		Collection <String> linksToInfStep = new ArrayList<String> ();
		
		if(p.wgbLink.containsKey(idNS))
			linksToInfStep = p.wgbLink.get(idNS);
		Iterator <String> infStepIDs = linksToInfStep.iterator();

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
		
		int infStepCounter = 0;

		ArrayList <IWInferenceStep>infSteps = new ArrayList<IWInferenceStep>();
		while(infStepIDs.hasNext()) {
			String thisKey = infStepIDs.next();
			ProtoProv.WGB thisWGB = p.wgbStore.get(thisKey);
			String fxnID = thisWGB.getFunction();
			
			ProtoProv.Function thisFxn = p.fxnStore.get(fxnID);
			ProtoProv.WCB thisCtl = p.wcbStore.get(p.wcbLink.get(fxnID).iterator().next());
			
			String engineURI = thisCtl.getController();
			String ruleURI = thisFxn.getName();		
			
		
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

			Collection <String> linksToNS = new ArrayList<String> ();		
			if(p.usdLink.containsKey(fxnID)) {
				linksToNS = p.usdLink.get(fxnID);
			//	System.out.println(tabs + fxnID + ": " + usdLink.get(fxnID));
			//	System.out.println(linksToNS.toString());
				Iterator <String> nsIDs = linksToNS.iterator();
				
				List <IWNodeSet>antecedents = new ArrayList<IWNodeSet>();
				while(nsIDs.hasNext()) {
					String thisIter = nsIDs.next();
					ProtoProv.Usd thisUsd = p.usdStore.get(thisIter);
					String variable = thisUsd.getVariable();
					IWNodeSet thisNodeSet = createNodeSet(p, variable);
					antecedents.add(thisNodeSet);
				}
				infStep.setHasAntecedentList(antecedents);
				infSteps.add(infStep); 	
			}
		}
		ns.setIsConsequentOf(infSteps);	
		return ns;	
	}
}
