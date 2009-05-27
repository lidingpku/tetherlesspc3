package edu.rpi.tw.provenance.protoprov;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntResource;
import com.hp.hpl.jena.ontology.Ontology;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.query.ResultSet;




public class GenRDFModel extends ProtoProv {
	
    /** <p>The namespace of the vocabalary as a string ({@value})</p> */
    public static final String ProtoProvNS = "http://www.cs.rpi.edu/~michaj6/PPV2.owl#";
    public static final String OutputNS = "http://www.cs.rpi.edu/~michaj6/PC3.owl#";
    
	public OntModel genRDFStore(ProtoProv protoprov) {
        // create an empty model
		OntModel model = ModelFactory.createOntologyModel();
		Ontology ont = model.createOntology( OutputNS );
		ont.addImport(model.createResource(ProtoProvNS));
		
		//model.read("http://www.cs.rpi.edu/~michaj6/ProtoProvV2.owl#");
       // Model model = ModelFactory.createDefaultModel();
        // Load Vars in First
        model.setNsPrefix("PPV2", ProtoProvNS);
        model.setNsPrefix("PC3", OutputNS);
        //model = loadInGroups()
        model = loadInControllers(model, protoprov);
        model = loadInVars(model, protoprov);
        model = loadInFxns(model, protoprov);
        model = loadInUsds(model, protoprov);
        model = loadInWGBs(model, protoprov);
        model = loadInWCBs(model, protoprov);
        
        
        return model;
	}
	
	public static OntModel loadInContexts(OntModel model, ProtoProv p) {
		// TODO: insert code for loading in groups
		return model;
	}
	
	public static OntModel loadInContextOverlaps(OntModel model, ProtoProv p) {
		// TODO: insert code for loading in intersects
		return model;
	}
	
	public static OntModel loadInControllers(OntModel model, ProtoProv p) {
		Iterator <String> ctlIter = p.ctlStore.keySet().iterator();
		
		Property hasName = model.getProperty(ProtoProvNS + "hasName");
		Property hasAccount = model.getProperty(ProtoProvNS + "hasContext");
		//RDFNode x = model.getRDFNode(model.getResource(ProtoProvNS + "Context").asNode());
		while(ctlIter.hasNext()) {
			String thisCtlKey = ctlIter.next();
			Resource thisCtlRes = model.createResource(OutputNS + thisCtlKey);

			Controller thisCtl = p.ctlStore.get(thisCtlKey);
			String thisCtlValue = thisCtlKey;	
			Statement rdfTypeStmt = model.createStatement(thisCtlRes, RDF.type, model.createResource(ProtoProvNS + "Controller"));
			Statement valueStmt = model.createStatement(thisCtlRes, hasName,  thisCtlValue);
			model.add(rdfTypeStmt);
			model.add(valueStmt);
			
			Iterator<String> thisCtlAccounts = thisCtl.getAccounts().iterator();
			while(thisCtlAccounts.hasNext()) {
				String thisAccountStr = OutputNS + thisCtlAccounts.next();
				Statement accountStmt = model.createStatement(thisCtlRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
 		}
		return model;
	}


	public static OntModel loadInVars(OntModel model, ProtoProv p) {
		Iterator <String> varIter = p.varStore.keySet().iterator();
		Property hasValue = model.createProperty(ProtoProvNS + "hasValue");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasContext");
		Ontology ont = model.getOntology(ProtoProvNS);
		
		while(varIter.hasNext()) {
			String thisVarKey = varIter.next();
			OntResource thisVarRes  = model.createOntResource(OutputNS + thisVarKey);

			thisVarRes.setRDFType(model.getResource(ProtoProvNS + "Variable"));
			
			Variable thisVar = p.varStore.get(thisVarKey);
			String thisVarValue = thisVar.getValue().toString();

			Statement valueStmt = model.createLiteralStatement(thisVarRes, hasValue, thisVarValue);
			/*ont.setRDFType(thisVarRes);
			ont.setr
			Statement rdfTypeStmt = model.createStatement(thisVarRes, RDF.type, ont.setR
					
					model.createResource(ProtoProvNS + "Variable"));
			model.add(rdfTypeStmt);*/
			model.add(valueStmt);
			
			Iterator<String> thisVarAccounts = thisVar.getAccounts().iterator();
			while(thisVarAccounts.hasNext()) {
				String thisAccountStr = OutputNS + thisVarAccounts.next();
				Statement accountStmt = model.createStatement(thisVarRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}


	public static OntModel loadInFxns(OntModel model, ProtoProv p) {
		Iterator <String> fxnIter = p.fxnStore.keySet().iterator();
		Property hasName = model.createProperty(ProtoProvNS + "hasName");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasContext");
	
		while(fxnIter.hasNext()) {
			String thisFxnKey = fxnIter.next();
			Resource thisFxnRes  = model.createResource(OutputNS + thisFxnKey);
			//System.out.println("FXN KEY:" + thisFxnKey);
			Function thisFxn = p.fxnStore.get(thisFxnKey);
			String thisFxnValue = thisFxn.getName();
			
			Statement valueStmt = model.createLiteralStatement(thisFxnRes, hasName, thisFxnValue);

			Statement rdfTypeStmt = model.createStatement(thisFxnRes, RDF.type, model.createResource(ProtoProvNS + "Function"));
			model.add(rdfTypeStmt);
			model.add(valueStmt);
			
			Iterator<String> thisFxnAccounts = thisFxn.getAccounts().iterator();
			while(thisFxnAccounts.hasNext()) {
				String thisAccountStr =OutputNS + thisFxnAccounts.next();
				Statement accountStmt = model.createStatement(thisFxnRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	public static OntModel loadInUsds(OntModel model, ProtoProv p) {
		Iterator <String> usdIter = p.usdStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "usdTarget");
		Property hasFunction = model.createProperty(ProtoProvNS + "usdSource");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasContext");
		
		while(usdIter.hasNext()) {
			String thisUsdKey = usdIter.next();
			Resource thisUsdRes  = model.createResource(OutputNS+ thisUsdKey);
			
			Usd thisUsd = p.usdStore.get(thisUsdKey);
			RDFNode thisUsdFunction = model.createResource(OutputNS+ thisUsd.getSource());
//			String thisUsdFunction = thisUsd.getFunction();
			RDFNode thisUsdVariable = model.createResource(OutputNS+ thisUsd.getTarget());
//			String thisUsdVariable = thisUsd.getVariable();
			String thisUsdRole = thisUsd.getRole();
			
			Statement variableStmt = model.createStatement(thisUsdRes, hasVariable, thisUsdVariable);
			Statement functionStmt = model.createStatement(thisUsdRes, hasFunction, thisUsdFunction);
			Statement roleStmt = model.createStatement(thisUsdRes, hasRole, thisUsdRole);
			
			Statement rdfTypeStmt = model.createStatement(thisUsdRes, RDF.type, model.createResource(ProtoProvNS + "Usd"));
			model.add(rdfTypeStmt);
			model.add(variableStmt);
			model.add(functionStmt);
			model.add(roleStmt);
			
			Iterator<String> thisUsdAccounts = thisUsd.getContexts().iterator();
			while(thisUsdAccounts.hasNext()) {
				String thisAccountStr =OutputNS + thisUsdAccounts.next();
				Statement accountStmt = model.createStatement(thisUsdRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	
	public static OntModel loadInWGBs(OntModel model, ProtoProv p) {
		Iterator <String> wgbIter = p.wgbStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "wgbSource");
		Property hasFunction = model.createProperty(ProtoProvNS + "wgbTarget");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasContext");
		
		while(wgbIter.hasNext()) {
			String thisWgbKey = wgbIter.next();
			Resource thisWgbRes  = model.createResource(OutputNS+ thisWgbKey);
			
			WGB thisWgb = p.wgbStore.get(thisWgbKey);
			System.out.println(thisWgb.getTarget());
			RDFNode thisWgbFunction = model.createResource(OutputNS+ thisWgb.getTarget());
			//String thisWgbFunction = thisWgb.getFunction();
			RDFNode thisWgbVariable = model.createResource(OutputNS+ thisWgb.getSource());
			//String thisWgbVariable = thisWgb.getVariable();
			String thisWgbRole = thisWgb.getRole();
			
			Statement variableStmt = model.createStatement(thisWgbRes, hasVariable, thisWgbVariable);
			Statement functionStmt = model.createStatement(thisWgbRes, hasFunction, thisWgbFunction);
			Statement roleStmt = model.createStatement(thisWgbRes, hasRole, thisWgbRole);
			
			Statement rdfTypeStmt = model.createStatement(thisWgbRes, RDF.type, model.createResource(ProtoProvNS + "WGB"));
			model.add(rdfTypeStmt);
			model.add(variableStmt);
			model.add(functionStmt);
			model.add(roleStmt);
			
			Iterator<String> thisWgbAccounts = thisWgb.getContexts().iterator();
			while(thisWgbAccounts.hasNext()) {
				String thisAccountStr =OutputNS + thisWgbAccounts.next();
				Statement accountStmt = model.createStatement(thisWgbRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	public static OntModel loadInWCBs(OntModel model, ProtoProv p) {
		Iterator <String> wcbIter = p.wcbStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "wcbTarget");
		Property hasFunction = model.createProperty(ProtoProvNS + "wcbSource");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasContext");
		
		while(wcbIter.hasNext()) {
			String thisWcbKey = wcbIter.next();
			Resource thisWcbRes  = model.createResource(OutputNS+ thisWcbKey);
			WCB thisWcb = p.wcbStore.get(thisWcbKey);
			RDFNode thisWcbFunction = model.createResource(OutputNS+ thisWcb.getSource());
//			String thisWcbFunction = thisWcb.getFunction();
			RDFNode thisWcbController = model.createResource(OutputNS+ thisWcb.getTarget());
//			String thisWcbController = thisWcb.getController();
			String thisWcbRole = thisWcb.getRole();
			

			Statement variableStmt = model.createStatement(thisWcbRes, hasVariable, thisWcbController);
			Statement functionStmt = model.createStatement(thisWcbRes, hasFunction, thisWcbFunction);
			Statement roleStmt = model.createStatement(thisWcbRes, hasRole, thisWcbRole);
			
			Statement rdfTypeStmt = model.createStatement(thisWcbRes, RDF.type, model.createResource(ProtoProvNS + "WCB"));
			model.add(rdfTypeStmt);
			model.add(variableStmt);
			model.add(functionStmt);
			model.add(roleStmt);

			Iterator<String> thisWcbAccounts = thisWcb.getContexts().iterator();
			while(thisWcbAccounts.hasNext()) {
				String thisAccountStr =OutputNS + thisWcbAccounts.next();
				Statement accountStmt = model.createStatement(thisWcbRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}
}