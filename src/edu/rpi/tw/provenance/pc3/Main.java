package edu.rpi.tw.provenance.pc3;

import com.hp.hpl.jena.rdf.model.Model;

import edu.rpi.tw.provenance.pc3.emulators.LoadWorkflow;
import edu.rpi.tw.provenance.protoprov.*;

public class Main {
	public static void main(String[] args) throws Exception {
		LoadWorkflow x = new LoadWorkflow();
		GenRDFModel genRDFModel = new GenRDFModel();
		ProtoProv p = x.RunPANSTAARS("J062941", "../../Data/J062941/");
		Model m = genRDFModel.genRDFStore(p);
		
		m.write(System.out);
		
	}
}
