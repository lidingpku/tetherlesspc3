package edu.rpi.tw.provenance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.query.ResultSet;




public class RDFGen extends ProtoProv {

	static final String modQueryPrefix =	"PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
	"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\r\n"+    
	"PREFIX owl: <http://www.w3.org/2002/07/owl#>\r\n" +
	"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\r\n" +
	"PREFIX ds: <http://inference-web.org/2.0/ds.owl#>\r\n" +
	"PREFIX pc: <http://www.cs.rpi.edu/#PC3>\r\n";
	
	
    /** <p>The namespace of the vocabalary as a string ({@value})</p> */
    public static final String NS = "http://www.cs.rpi.edu/PC3#";
    
	public static void genRDFStore() {
        // create an empty model
        Model model = ModelFactory.createDefaultModel();
        // Load Vars in First
        model = loadInCtl(model);
        model = loadInVars(model);
        model = loadInFxns(model);
        
        queryModel(model);
        // Then Load in Fxns
	}
	
	
	
	public static Model loadInCtl(Model model) {
		Iterator <String> CtlIter = CtlIDs.iterator();
		Property hasAccount = model.createProperty(NS + "hasAccount");
		
		while(CtlIter.hasNext()) {
			String thisCtlStr = CtlIter.next();
			Resource thisCtlRes = model.createResource(thisCtlStr);
			
			Iterator<String> thisCtlAccounts = CtlIDtoAccountsMap.get(thisCtlStr).iterator();
			while(thisCtlAccounts.hasNext()) {
				String thisAccountStr = thisCtlAccounts.next();
				Statement accountStmt = model.createStatement(thisCtlRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
 		}
		return model;
	}

	/*
	 * 
	 * 	// OPM Artifacts
	static Collection<String>				VarIDs = new ArrayList<String>();	
	static Map<String,Object> 				VarIDtoValueMap = new HashMap<String,Object>();
	static Map<String, Collection<String>> VarIDtoAccountsMap = new HashMap<String,Collection<String>>();

	 * 
	 * 
	 */
	public static Model loadInVars(Model model) {
		Iterator <String> VarIter = VarIDs.iterator();
		Property hasValue = model.createProperty(NS + "hasValue");
		Property hasAccount = model.createProperty(NS + "hasAccount");
			
		while(VarIter.hasNext()) {
			String thisVarStr = VarIter.next();
			String thisVarValue = VarIDtoValueMap.get(thisVarStr).toString();
			Resource varId = model.createResource(NS + thisVarStr);
			Statement varType = model.createStatement(varId, RDF.type, "variable");
			model.add(varType);
			Statement varValue = model.createStatement(varId, hasValue, thisVarValue);
			model.add(varValue);
			
			Iterator<String> thisVarAccounts = VarIDtoAccountsMap.get(thisVarStr).iterator();
			while(thisVarAccounts.hasNext()) {
				String thisAccountStr = thisVarAccounts.next();
				Statement accountStmt = model.createStatement(varId, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	
	public static Model loadInFxns(Model model) {
		Iterator <String> VarIter = FxnIDs.iterator();
		Property hasValue = model.createProperty(NS + "hasValue");
		Property hasAccount = model.createProperty(NS + "hasAccount");
		
		
		
		while(VarIter.hasNext()) {
			String thisFxnStr = VarIter.next();
			String thisFxnValue = FxnIDtoValueMap.get(thisFxnStr).toString();
			Resource fxnId = model.createResource(NS + thisFxnStr);
			Statement fxnType = model.createStatement(fxnId, RDF.type, "function");
			model.add(fxnType);
			Statement fxnValue = model.createStatement(fxnId, hasValue, thisFxnValue);
			model.add(fxnValue);
			
			Iterator<String> thisFxnAccounts = FxnIDtoAccountsMap.get(thisFxnStr).iterator();
			while(thisFxnAccounts.hasNext()) {
				String thisAccountStr = thisFxnAccounts.next();
				Statement accountStmt = model.createStatement(fxnId, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}
	
/*	// VarToFxn:  (OPM: wasGeneratedBy, PML: isConsequentOf)
	static Map<String, Collection<UUID>> VarToFxnUUIDs = new HashMap<String, Collection<UUID>>();
	static Map<UUID, String> VarToFxnRoles = new HashMap<UUID, String>();
	static Map<UUID, String> VarToFxnIds = new HashMap<UUID, String>();
	static Map<UUID, Collection<String>> VarToFxnAccountIds = new HashMap<UUID, Collection<String>>();
*/
	public static Model loadInWasGeneratedBy(Model model) {
		Iterator <String> VarIter = VarIDs.iterator();
		
		Property hasRole = model.createProperty(NS + "hasRole");
		
		while(VarIter.hasNext()) {
			String VarIdStr = VarIter.next();
			
			Iterator <UUID> wgbUUIDs = VarToFxnUUIDs.get(VarIdStr).iterator();
			while(wgbUUIDs.hasNext()) {
				UUID curUUID = wgbUUIDs.next();
				String role = VarToFxnRoles.get(curUUID);
				
			}
			
		}
		
		
		
		return model;
	}
	
	
	
	
	
	/**
	 * 
	 * 
	 * 			Iterator<UUID> thisUUIDCollection = ((Collection<UUID>)VarToFxnUUIDs.get(thisVarStr)).iterator();
			while(thisUUIDCollection.hasNext()) {
				UUID thisUUID = thisUUIDCollection.next();
				/*
				 * 
				 * 	static Map<UUID, String> VarToFxnRoles = new HashMap<UUID, String>();
	static Map<UUID, String> VarToFxnIds = new HashMap<UUID, String>();
	static Map<UUID, Collection<String>> VarToFxnAccountIds = new HashMap<UUID, Collection<String>>();

				 * 
				 *

				String role = VarToFxnRoles.get(thisUUID);
				String fxnId = VarToFxnIds.get(thisUUID);
				Iterator<String> thisVarToFxnAccounts = VarToFxnAccountIds.get(thisUUID).iterator();
				while(thisVarToFxnAccounts.hasNext()) {
					String thisAccountStr = thisVarAccounts.next();
					Statement accountStmt = model.createStatement(varId, hasAccount, thisAccountStr);
					model.add(accountStmt);
				}
				
			}

	 * 
	 * 
	 * @param model
	 */
	public static void queryModel(Model model) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?s ?p ?o " +
		" WHERE { " +
		" ?s ?p ?o" +
		"}";
		//System.out.println(queryStr1);
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
			System.out.println(thisSolution.toString());
		}
	}
}
