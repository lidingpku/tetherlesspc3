package edu.rpi.tw.provenance.pc3.converters;

import java.io.File;
import java.io.StringWriter;

import org.openprovenance.model.OPMDeserialiser;
import org.openprovenance.model.OPMGraph;

import edu.rpi.tw.provenance.protoprov.LoadOPM;
import edu.rpi.tw.provenance.protoprov.ProtoProv;

public class VisTrails {
	public static ProtoProv loadVisTrails() throws Exception {
		
		File f = new File("/PC3/otherTeams/VisTrails3/workflow_opm2.xml");
		OPMDeserialiser d = new OPMDeserialiser();
		OPMGraph g2 = d.deserialiseOPMGraph(f);
		StringWriter sw = new StringWriter();
		LoadOPM l = new LoadOPM();
		
		ProtoProv p = l.loadOPMGraph(f);
		return p;
	}
}
