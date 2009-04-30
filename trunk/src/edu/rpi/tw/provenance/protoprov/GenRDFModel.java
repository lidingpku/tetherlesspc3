package edu.rpi.tw.provenance.protoprov;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.query.ResultSet;




public class GenRDFModel extends ProtoProv {

	static final String modQueryPrefix =	"PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
	"PREFIX ProtoProv: <http://www.cs.rpi.edu/~michaj6/ProtoProv.owl#>\r\n" +
	"PREFIX pc: <http://www.cs.rpi.edu/~michaj6/PC3/PC3.owl#>";
	
	
    /** <p>The namespace of the vocabalary as a string ({@value})</p> */
    public static final String NS = "http://www.cs.rpi.edu/~michaj6/PC3/PC3.owl#";
    public static final String ProtoProvNS = "http://www.cs.rpi.edu/~michaj6/ProtoProv.owl#";
    
	public Model genRDFStore(ProtoProv protoprov) {
        // create an empty model
        Model model = ModelFactory.createDefaultModel();
        // Load Vars in First
        model.setNsPrefix("pc", NS);
        model.setNsPrefix("ProtoProv", ProtoProvNS);
        //model = loadInGroups()
        model = loadInControllers(model, protoprov);
        model = loadInVars(model, protoprov);
        model = loadInFxns(model, protoprov);
        model = loadInUsds(model, protoprov);
        model = loadInWGBs(model, protoprov);
        model = loadInWCBs(model, protoprov);
        
        model.write(System.out);
        
        coreQuery3(model);
        //System.out.println("CQ 1: " + coreQuery1(model));
      //  System.out.println("CQ 2: " + coreQuery2(model));
     //   System.out.println("CQ 3: " + coreQuery3(model));
        //System.out.println("OQ 8: " + optionalQuery8(model));
        //System.out.println("OQ 10: " + optionalQuery10(model));
        //System.out.println("OQ 11: " + optionalQuery11(model));
        
 //       model.get
//        queryModel(model);
        // Then Load in Fxns
        return model;
	}
	
	
	
	public static Model loadInControllers(Model model, ProtoProv p) {
		Iterator <String> ctlIter = p.ctlStore.keySet().iterator();
		Property hasValue = model.createProperty(ProtoProvNS + "hasValue");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(ctlIter.hasNext()) {
			String thisCtlKey = ctlIter.next();
			Resource thisCtlRes = model.createResource(NS + thisCtlKey);

			Controller thisCtl = p.ctlStore.get(thisCtlKey);
			String thisCtlValue = thisCtlKey;	
			
			Statement rdfTypeStmt = model.createStatement(thisCtlRes, RDF.type, model.createResource(ProtoProvNS + "Controller"));
			Statement valueStmt = model.createStatement(thisCtlRes, hasValue, NS + thisCtlValue);
			model.add(rdfTypeStmt);
			model.add(valueStmt);
			
			Iterator<String> thisCtlAccounts = thisCtl.getAccounts().iterator();
			while(thisCtlAccounts.hasNext()) {
				String thisAccountStr = NS + thisCtlAccounts.next();
				Statement accountStmt = model.createStatement(thisCtlRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
 		}
		return model;
	}


	public static Model loadInVars(Model model, ProtoProv p) {
		Iterator <String> varIter = p.varStore.keySet().iterator();
		Property hasValue = model.createProperty(ProtoProvNS + "hasValue");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(varIter.hasNext()) {
			String thisVarKey = varIter.next();
			Resource thisVarRes  = model.createResource(NS + thisVarKey);
			
			Variable thisVar = p.varStore.get(thisVarKey);
			String thisVarValue = thisVar.getValue().toString();

			Statement valueStmt = model.createLiteralStatement(thisVarRes, hasValue, thisVarValue);
			
			Statement rdfTypeStmt = model.createStatement(thisVarRes, RDF.type, model.createResource(ProtoProvNS + "Variable"));
			model.add(rdfTypeStmt);
			model.add(valueStmt);
			
			Iterator<String> thisVarAccounts = thisVar.getAccounts().iterator();
			while(thisVarAccounts.hasNext()) {
				String thisAccountStr = NS + thisVarAccounts.next();
				Statement accountStmt = model.createStatement(thisVarRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}


	public static Model loadInFxns(Model model, ProtoProv p) {
		Iterator <String> fxnIter = p.fxnStore.keySet().iterator();
		Property hasName = model.createProperty(ProtoProvNS + "hasName");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(fxnIter.hasNext()) {
			String thisFxnKey = fxnIter.next();
			Resource thisFxnRes  = model.createResource(NS + thisFxnKey);
			
			Function thisFxn = p.fxnStore.get(thisFxnKey);
			String thisFxnValue = thisFxn.getName();
			
			Statement valueStmt = model.createLiteralStatement(thisFxnRes, hasName, thisFxnValue);

			Statement rdfTypeStmt = model.createStatement(thisFxnRes, RDF.type, model.createResource(ProtoProvNS + "Function"));
			model.add(rdfTypeStmt);
			model.add(valueStmt);
			
			Iterator<String> thisFxnAccounts = thisFxn.getAccounts().iterator();
			while(thisFxnAccounts.hasNext()) {
				String thisAccountStr = NS + thisFxnAccounts.next();
				Statement accountStmt = model.createStatement(thisFxnRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	public static Model loadInUsds(Model model, ProtoProv p) {
		Iterator <String> usdIter = p.usdStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "usdTarget");
		Property hasFunction = model.createProperty(ProtoProvNS + "usdSource");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(usdIter.hasNext()) {
			String thisUsdKey = usdIter.next();
			Resource thisUsdRes  = model.createResource(NS + thisUsdKey);
			
			Usd thisUsd = p.usdStore.get(thisUsdKey);
			RDFNode thisUsdFunction = model.createResource(NS + thisUsd.getFunction());
//			String thisUsdFunction = thisUsd.getFunction();
			RDFNode thisUsdVariable = model.createResource(NS + thisUsd.getVariable());
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
			
			Iterator<String> thisUsdAccounts = thisUsd.getAccounts().iterator();
			while(thisUsdAccounts.hasNext()) {
				String thisAccountStr = NS + thisUsdAccounts.next();
				Statement accountStmt = model.createStatement(thisUsdRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	
	public static Model loadInWGBs(Model model, ProtoProv p) {
		Iterator <String> wgbIter = p.wgbStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "wgbSource");
		Property hasFunction = model.createProperty(ProtoProvNS + "wgbTarget");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(wgbIter.hasNext()) {
			String thisWgbKey = wgbIter.next();
			Resource thisWgbRes  = model.createResource(NS + thisWgbKey);
			
			WGB thisWgb = p.wgbStore.get(thisWgbKey);
			RDFNode thisWgbFunction = model.createResource(NS + thisWgb.getFunction());
			//String thisWgbFunction = thisWgb.getFunction();
			RDFNode thisWgbVariable = model.createResource(NS + thisWgb.getVariable());
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
			
			Iterator<String> thisWgbAccounts = thisWgb.getAccounts().iterator();
			while(thisWgbAccounts.hasNext()) {
				String thisAccountStr = NS + thisWgbAccounts.next();
				Statement accountStmt = model.createStatement(thisWgbRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}

	public static Model loadInWCBs(Model model, ProtoProv p) {
		Iterator <String> wcbIter = p.wcbStore.keySet().iterator();
		Property hasVariable = model.createProperty(ProtoProvNS + "wcbTarget");
		Property hasFunction = model.createProperty(ProtoProvNS + "wcbSource");
		Property hasRole = model.createProperty(ProtoProvNS + "hasRole");
		Property hasAccount = model.createProperty(ProtoProvNS + "hasAccount");
		
		while(wcbIter.hasNext()) {
			String thisWcbKey = wcbIter.next();
			Resource thisWcbRes  = model.createResource(NS + thisWcbKey);
			WCB thisWcb = p.wcbStore.get(thisWcbKey);
			RDFNode thisWcbFunction = model.createResource(NS + thisWcb.getFunction());
//			String thisWcbFunction = thisWcb.getFunction();
			RDFNode thisWcbController = model.createResource(NS + thisWcb.getController());
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

			Iterator<String> thisWcbAccounts = thisWcb.getAccounts().iterator();
			while(thisWcbAccounts.hasNext()) {
				String thisAccountStr = NS + thisWcbAccounts.next();
				Statement accountStmt = model.createStatement(thisWcbRes, hasAccount, thisAccountStr);
				model.add(accountStmt);
			}
		
			
 		}
		return model;
	}


	public static Set <String> coreQuery1(Model model) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?value " +
		" WHERE { " +
		" ?wgb ProtoProv:wgbSource pc:DBEntryP2Detection_0_ForIter3 . " +
		" ?wgb ProtoProv:wgbTarget ?fxn ." +
		" ?usd ProtoProv:usdSource ?fxn ." +
		" ?usd ProtoProv:usdTarget ?var ." +
		" ?var ProtoProv:hasType ?type ." +
		" FILTER(?type = \"CSVFileEntry\") " +
		" ?var ProtoProv:hasValue ?value" +
		"}";	
		
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
			System.out.println(thisSolution);
			functionRet.add(thisSolution.getLiteral("value").getLexicalForm());
		}
		return functionRet;
	}
	
	
	public static Set <String> coreQuery2(Model model) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?fxn " +
		" WHERE { " +
		" ?usd ProtoProv:usdTarget pc:ReadCSVFileColumnNamesOutput_0_ForIter1 . " +
		" ?usd ProtoProv:usdSource ?fxn ." +
		" ?fxn ProtoProv:hasValue ?val ." +
		" FILTER(?val=\"IsMatchTableColumnRanges\")" +
		"}";
		
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		if(results.hasNext()) {
			functionRet.add("YES");
		} else {
			functionRet.add("NO");
		}
		return functionRet;
	}	
	
	
	public static Set <String> coreQuery3(Model model) {
		RDFNode imageEntry = model.getResource("http://www.cs.rpi.edu/~michaj6/PC3/PC3.owl#DBEntryP2ImageMeta_0_ForIter2");
		Set <String> functions = rdfRecurse(model, imageEntry);
		return functions;
	}
		
	public static Set <String> rdfRecurse(Model model, RDFNode r) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?fxn ?value ?var " +
		" WHERE { " +
		" ?wgb ProtoProv:wgbSource <" + r + "> . " +
		" ?wgb ProtoProv:wgbTarget ?fxn ." +
		" ?fxn ProtoProv:hasValue ?value ." +
		" ?usd ProtoProv:usdSource ?fxn ." +
		" ?usd ProtoProv:usdTarget ?var ." +
		" ?var ProtoProv:hasType ?type ." +
		" FILTER(?type != \"boolean\") ." +	
		"}";	
		
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
///			RDFNode var = thisSolution.get("var");
			System.out.println(thisSolution);
/*			
			String fxn = thisSolution.getResource("fxn").getLocalName();
			if(!functionRet.contains(fxn) && !thisSolution.getLiteral("value").getLexicalForm().equals("ForEach"))
				functionRet.add(fxn);
			functionRet.addAll(rdfRecurse(model, var));*/
		}
		return functionRet;
	}
	

	public static Set<String> optionalQuery8(Model model) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?fxn " +
		" WHERE { " +
		" ?fxn rdf:type ProtoProv:Function . " +
		" }";
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
			String fxn = thisSolution.getResource("fxn").getLocalName();
			functionRet.add(fxn);
		}
		return functionRet;			
		
	}
	
	public static Set<String> optionalQuery10(Model model) {
		String queryStr1 = modQueryPrefix + 
		" SELECT ?var " +
		" WHERE { " +
		" ?var rdf:type ProtoProv:Variable ." +
		" ?wgb ProtoProv:wgbSource ?var ." +
		" ?wgb ProtoProv:wgbTarget ?fxn ." +
		" ?fxn ProtoProv:hasValue ?value ." +
		" FILTER(?value = \"DirectAssertion\")" +	
		"}";	
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
			functionRet.add(thisSolution.getResource("var").getLocalName());
		}
		return functionRet;	
	}
	
	public static Set<String> optionalQuery11(Model model) {	
		String queryStr1 = modQueryPrefix + 
		" SELECT ?fxn " +
		" WHERE { " +
		" ?fxn rdf:type ProtoProv:Function . " +
		" ?usd ProtoProv:usdSource ?fxn ." +
		" ?usd ProtoProv:usdTarget ?var ." +
		" ?wgb ProtoProv:wgbSource ?var ." +
		" ?wgb ProtoProv:wgbTarget ?fxn2 ." +
		" ?fxn2 ProtoProv:hasValue ?value ." +
		" FILTER(?value = \"DirectAssertion\")" +	
		"}";
		Query query = QueryFactory.create(queryStr1);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet results = qexec.execSelect();
		Set <String> functionRet = new HashSet<String>();
		while(results.hasNext()) {
			QuerySolution thisSolution = results.nextSolution();
			String fxn = thisSolution.getResource("fxn").getLocalName();
			
			functionRet.add(fxn);
		}
		return functionRet;	
	}	
}
