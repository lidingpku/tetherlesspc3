package edu.rpi.tw.provenance.pc3.converters;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.xml.sax.helpers.DefaultHandler;

public class UCDGC extends DefaultHandler {

	static private BufferedWriter out;

	private String tempVal = "";
	
	private String lastElement = "";

	private HashMap<String, String> HM = new HashMap<String, String>();
	private HashMap<String, String> HM1 = new HashMap<String, String>();
	private HashMap<String, String> HM1Assist = new HashMap<String, String>();
	
	private HashMap<String, String> HM2 = new HashMap<String, String>();

	public UCDGCMapping() {

		// initialize the mapping into a hashmap
		//Mapping first level elements
		HM.put("opmGraph", "rdf:RDF");
		HM.put("process", "Function");
		HM.put("artifact", "Variable");
		HM.put("used", "Usd");
		HM.put("wasGeneratedBy", "WGB");
		HM.put("wasDerivedFrom", "WDF");
		HM.put("wasTriggeredBy", "WTB");
		//HM.put("processes", "Functions");
		
		//Mapping second level elements
		HM1.put("effect", "Source");
		HM1.put("cause", "Target");
		HM1.put("role", "hasRole");
		HM1.put("value", "");
		
		HM1Assist.put("used", "usd");
		HM1Assist.put("wasGeneratedBy", "wgb");
		HM1Assist.put("wasDerivedFrom", "wdf");
		HM1Assist.put("wasTriggeredBy", "wtb");
		HM1Assist.put("process", "");
		HM1Assist.put("artifact", "");
		//Mapping attributes
		HM2.put("id", "rdf:resource");
		HM2.put("value", "rdf:about");
		

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
			sp.parse(

					"C:\\J062941.xml",
					this);

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
		String tempAttQName = "";
//convert first level
		if (qName == "processes"  || qName == ("artifacts")|| qName == ("causalDependencies")) {
			
		} else {
			if (HM.containsKey(qName)) {
				lastElement = qName;
				printData("<" + HM.get(qName));
				if (attributes != null) {
					for (int i = 0; i < attributes.getLength(); i++) {
						if (qName == "artifact" || qName == "process") {
							tempAttQName = "rdf:about";
						} else {
							tempAttQName = attributes.getQName(i);
						}
						printData(" ");
						if (qName == "opmGraph") {
							printData("xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" ");
						}
						printData(tempAttQName + "=\""
								+ attributes.getValue(i) + "\"");
						if (qName == "opmGraph") {
							printData(" xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"");
						}
					
					}
				}
				printData(">");
//convert second level
			} else if (HM1.containsKey(qName)){
				if (qName == "value") {
					if (lastElement == "process") {
						printData("<" + "hasType");
					} else {
						printData("<" + "hasValue");
					}
				} else {
					if (qName == "role"){
						printData("<" + HM1.get(qName) + ">");
						printData("\n\t\t\t\t");
						printData("<ProvRole>");					
						printData("\n\t\t\t\t\t");
						printData("<rdf:labels>");
						printData(attributes.getValue(0));
						printData("</rdf:labels>");
						printData("\n\t\t\t\t");
						printData("</ProvRole>");
						printData("\n\t\t\t");
						printData("</hasRole");
					} else {
						printData("<" + HM1Assist.get(lastElement) + HM1.get(qName));
						if (attributes != null) {
							if (attributes.getQName(0) == "xsi:type") {
								printData("");
							}	else {
									for (int i = 0; i < attributes.getLength(); i++) {
										if (HM2.containsKey(attributes.getQName(i))) {
											tempAttQName = HM2.get(attributes.getQName(i));
										} else {
											tempAttQName = attributes.getQName(i);
										}
										printData(" ");
										printData(tempAttQName + "=\"" + attributes.getValue(i) + "\"");
									}
									printData("/");
								}
							}
					}
				}
				printData(">");
			} else {
				System.out.println(qName);
				System.out.println("No matching defined.");
			}
		}
	}

	public void characters(char[] ch, int start, int length)
			throws SAXException {
		tempVal = new String(ch, start, length);
		printData(tempVal);
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
//convert first level
		if (qName == "processes" || qName == ("artifacts") || qName == ("causalDependencies")){
			
		} else {
			
		if (HM.containsKey(qName)) {
			if (qName == "opmGraph") {
				printData("</" + HM.get(qName) + ">");
			} else {
				printData("\t");
				printData("<rdfs:label>" + qName +  "</rdfs:label>");
				printData("\n");
				printData("\t");
				printData("\t");
				printData("</" + HM.get(qName) + ">");
			}
//convert second level
		} else if (qName == "value") {              
			if (lastElement == "process") {
				printData("</" + "hasType" + ">");
			} else {
				printData("</" + "hasValue" + ">");
			}
		} else if (HM1.containsKey(qName)) {
			printData("");
		} else {
			System.out.println("No matching defined.");
		}
		
		}
	}

	public static void main(String[] args) {
		// Set up output stream
		try {
			FileWriter fstream = new FileWriter(
					"C:\\eclipse\\JAVA code\\xmlParse\\UDCGC_converted.txt");
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.out.println("Failed to open file writer!");
		}
		UCDGCMapping map = new UCDGCMapping();
		map.parseDocument();
	}

}
