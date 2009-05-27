package edu.rpi.tw.provenance.pc3.converters;

import java.io.*;
import java.util.HashMap;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.openprovenance.model.OPMDeserialiser;
import org.openprovenance.model.OPMGraph;
import org.openprovenance.model.OPMSerialiser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.xml.sax.helpers.DefaultHandler;

import edu.rpi.tw.provenance.protoprov.LoadOPM;
import edu.rpi.tw.provenance.protoprov.ProtoProv;

public class Ncsa extends DefaultHandler {
	static private BufferedWriter out;

	private String tempVal = "";

	private HashMap<String, String> hp = new HashMap<String, String>();

	public Ncsa() {

		// initialize the mapping into a hashmap
		hp.put("b:Account", "Context");
		hp.put("b:Artifact", "Variable");
		hp.put("b:CSV_file", "Variable");
		hp.put("b:Process", "Function");
		hp.put("a:Loop_Iteration", "Function");
		hp.put("b:Role", "ProvRole");
		hp.put("b:Generated", "WGB");
		hp.put("b:Triggered", "WTB");
		hp.put("b:Used", "Usd");
		hp.put("c:timeInterval", "TimeSlice");

		hp.put("b:eventAccount", "hasContext");
		hp.put("b:generatedArtifact", "wgbTarget");
		hp.put("b:generatedByProcess", "wgbSource");
		hp.put("b:triggeredProcess", "wtbTarget");
		hp.put("b:triggeredByProcess", "wtbSource");
		hp.put("b:usedByProcess", "usdSource");
		hp.put("b:usedArtifact", "usdTarget");
		hp.put("b:usedRole", "hasRole");
		hp.put("b:generatedRole", "hasRole");
		hp.put("c:usedTime", "hasTimeSlice");
		hp.put("c:noEarlier", "hasStartTime");
		hp.put("c:noLater", "hasStopTime");
		hp.put("a:PathToFile", "hasValue");

		hp.put("rdf:type", "hasType");
		hp.put("rdfs:label", "rdfs:label");
		hp.put("rdf:RDF", "rdf:RDF");

	}

	/**
	 * Parse the RDF/XML document
	 */
	public void parseDocument() {

		// get a factory
		SAXParserFactory spf = SAXParserFactory.newInstance();

		try {

			// get a new instance of parser
			SAXParser sp = spf.newSAXParser();

			// parse the file and also register this class for call backs
			sp.parse("E:\\Research-RPI\\tw\\provenance\\2009-04_2009-05_pc3\\team\\NcsaPc3\\output.rdf", this);

		} catch (SAXException se) {
			se.printStackTrace();
		} catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		} catch (IOException ie) {
			ie.printStackTrace();
		}
	}

	/**
	 * Write the parsed string into ProvProto ontology file
	 */
	private void printData(String s) {
		try {
			out.write(s);
			out.flush();
		} catch (IOException e) {
			System.out.println("IO Error!");
		}
	}

	private void newLine() {
		String lineEnd = System.getProperty("line.separator");
		try {
			out.write(lineEnd);
		} catch (IOException e) {
			System.out.println("IO Error!");
		}
	}

	// Event Handlers
	public void startDocument() throws SAXException {
		printData("<?xml version='1.0' encoding='UTF-8'?>");
		newLine();
	}

	public void endDocument() throws SAXException {
		try {
			newLine();
			out.flush();
		} catch (IOException e) {
			throw new SAXException("I/O error", e);
		}
	}

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (hp.containsKey(qName)) {
			printData("<" + hp.get(qName));
			if (attributes != null) {
				for (int i = 0; i < attributes.getLength(); i++) {
					printData(" ");
					printData(attributes.getQName(i) + "=\""
							+ attributes.getValue(i) + "\"");
				}
			}
			printData(">");
		} else {
			System.out.println("No matching defined.");
		}

	}

	public void characters(char[] ch, int start, int length)
			throws SAXException {
		tempVal = new String(ch, start, length);
		printData(tempVal);
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {

		if (hp.containsKey(qName)) {
			printData("</" + hp.get(qName) + ">");
		} else {
			System.out.println("No matching defined.");
		}

	}

	public static ProtoProv loadNcsa() throws Exception {
		
		File f = new File("/PC3/otherTeams/NcsaPc3/J609241_output.xml");
		OPMDeserialiser d = new OPMDeserialiser();
		OPMGraph g2 = d.deserialiseOPMGraph(f);
		StringWriter sw = new StringWriter();
		LoadOPM l = new LoadOPM();
		
		ProtoProv p = l.loadOPMGraph(f);
		return p;
//		System.out.println(OPMSerialiser.getThreadOPMSerialiser().serialiseOPMGraph(sw, g2, true));
/*
		
		// Set up output stream
		try {
			FileWriter fstream = new FileWriter(
					"E:\\Research-RPI\\tw\\provenance\\2009-04_2009-05_pc3\\team\\NcsaPc3\\converted1.rdf");
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.out.println("Failed to open file writer!");
		}
		Ncsa nc3 = new Ncsa();
		nc3.parseDocument();*/
	}
}
