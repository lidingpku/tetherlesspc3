package edu.rpi.tw.provenance.pc3.converters;

import java.io.File;
import java.io.StringWriter;

import org.openprovenance.model.OPMDeserialiser;
import org.openprovenance.model.OPMGraph;

import edu.rpi.tw.provenance.protoprov.LoadOPM;
import edu.rpi.tw.provenance.protoprov.ProtoProv;

public class PASS3 {
	public static ProtoProv loadPASS3() throws Exception {
		
		File f = new File("/PC3/otherTeams/PASS3/J062941.opm.xml");
		OPMDeserialiser d = new OPMDeserialiser();
		OPMGraph g2 = d.deserialiseOPMGraph(f);
		StringWriter sw = new StringWriter();
		LoadOPM l = new LoadOPM();
		
		ProtoProv p = l.loadOPMGraph(f);
		return p;
	}
}
