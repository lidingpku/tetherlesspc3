package edu.rpi.tw.provenance.pc3;

import java.io.File;
import java.io.StringWriter;

import org.openprovenance.model.OPMDeserialiser;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.OPMSerialiser;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.Model;

import edu.rpi.tw.provenance.pc3.converters.KCL;
import edu.rpi.tw.provenance.pc3.converters.Ncsa;
import edu.rpi.tw.provenance.pc3.converters.PASS3;
import edu.rpi.tw.provenance.pc3.converters.SDSC;
import edu.rpi.tw.provenance.pc3.converters.SotonUSCISI;
import edu.rpi.tw.provenance.pc3.converters.Swift;
import edu.rpi.tw.provenance.pc3.converters.UCDGC;
import edu.rpi.tw.provenance.pc3.converters.UoM;
import edu.rpi.tw.provenance.pc3.converters.VisTrails;
import edu.rpi.tw.provenance.pc3.emulators.LoadWorkflow;
import edu.rpi.tw.provenance.protoprov.*;

public class Main {
	public static void main(String[] args) throws Exception {
	/*	LoadWorkflow x = new LoadWorkflow();
		GenRDFModel genRDFModel = new GenRDFModel();
		ProtoProv p = x.RunPANSTAARS("J062941", "/Data/J062941/");
		Model m = genRDFModel.genRDFStore(p);
		OPMGraph g = genOPM.genOPMGraph(p);
	
		*/
		
//		Ncsa iNcsa = new Ncsa();
//		ProtoProv p = iNcsa.loadNcsa();
		
//		Swift iSwift = new Swift();
//		ProtoProv p = iSwift.loadSwift();
		
//		UCDGC iUCDGC = new UCDGC();
//		ProtoProv p = iUCDGC.loadUCDGC();
		
		
//		SotonUSCISI iSotonUSCISI = new SotonUSCISI();
//		ProtoProv p = iSotonUSCISI.loadSotonUSCISI();

		
//		UoM iUoM = new UoM();
//		ProtoProv p = iUoM.loadUoM();


//		SDSC iSDSC = new SDSC();
//		ProtoProv p = iSDSC.loadSDSC();

//		VisTrails iVisTrails = new VisTrails();
//		ProtoProv p = iVisTrails.loadVisTrails();
		
//		KCL iKCL = new KCL();
//		ProtoProv p = iKCL.loadKCL();
		
		PASS3 iPASS3 = new PASS3();
		ProtoProv p = iPASS3.loadPASS3();
		
		
		//GenOPM genOPM = new GenOPM();

		//OPMGraph g = genOPM.genOPMGraph(p);
		
		//StringWriter sw = new StringWriter();
		//LoadOPM l = new LoadOPM();
		
		//System.out.println(OPMSerialiser.getThreadOPMSerialiser().serialiseOPMGraph(sw, g, true));

		
		GenRDFModel gRdf = new GenRDFModel();
		OntModel m = gRdf.genRDFStore(p);
		
		m.write(System.out);
		
	}
}
