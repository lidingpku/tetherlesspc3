package edu.rpi.tw.provenance.pc3.queries;

import java.util.HashSet;
import java.util.Set;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class RunQueries {

	static final String modQueryPrefix =	"PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\r\n" +
	"PREFIX ProtoProv: <http://www.cs.rpi.edu/~michaj6/ProtoProv.owl#>\r\n" +
	"PREFIX pc: <http://www.cs.rpi.edu/~michaj6/PC3/PC3.owl#>";

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
