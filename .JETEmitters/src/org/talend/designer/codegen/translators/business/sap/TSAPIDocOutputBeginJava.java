package org.talend.designer.codegen.translators.business.sap;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.Map;
import java.util.List;

public class TSAPIDocOutputBeginJava
{
  protected static String nl;
  public static synchronized TSAPIDocOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPIDocOutputBeginJava result = new TSAPIDocOutputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = " " + NL + "\t\t\tfinal String decryptedPassword_";
  protected final String TEXT_3 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_4 = ");" + NL + "\t\t";
  protected final String TEXT_5 = NL + "\t\t\tfinal String decryptedPassword_";
  protected final String TEXT_6 = " = ";
  protected final String TEXT_7 = "; " + NL + "\t\t";
  protected final String TEXT_8 = NL + NL + "\t\tjava.util.Properties properties_";
  protected final String TEXT_9 = " = new java.util.Properties();" + NL + "\t";
  protected final String TEXT_10 = "\t" + NL + "\t\t\tproperties_";
  protected final String TEXT_11 = ".put(";
  protected final String TEXT_12 = " ,";
  protected final String TEXT_13 = ");" + NL + "\t\t\t";
  protected final String TEXT_14 = NL + "\t" + NL + "\t";
  protected final String TEXT_15 = NL + "\t\t\tcom.sap.conn.jco.JCoDestination destination_";
  protected final String TEXT_16 = " = (com.sap.conn.jco.JCoDestination)globalMap.get(\"conn_";
  protected final String TEXT_17 = "\");";
  protected final String TEXT_18 = NL + "\t\t\tif(destination_";
  protected final String TEXT_19 = " == null){" + NL + "\t\t\t\tdestination_";
  protected final String TEXT_20 = " = com.sap.conn.jco.JCoDestinationManager.getDestination(";
  protected final String TEXT_21 = ");" + NL + "\t\t\t}";
  protected final String TEXT_22 = NL + "\t\torg.talend.sap.TSAPDestinationData destinationData_";
  protected final String TEXT_23 = " = new org.talend.sap.TSAPApplicationServerDestData.Builder(";
  protected final String TEXT_24 = "," + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_25 = "," + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tdecryptedPassword_";
  protected final String TEXT_26 = "," + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_27 = "," + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_28 = "," + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_29 = ")" + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t.setCustomProp(properties_";
  protected final String TEXT_30 = ")" + NL + "        \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t.build();" + NL + "\t" + NL + "\t\tcom.sap.conn.jco.JCoDestination destination_";
  protected final String TEXT_31 = " = org.talend.sap.TSAPDestinationFactory.getInstance().getDestination(destinationData_";
  protected final String TEXT_32 = ");";
  protected final String TEXT_33 = NL + "\t" + NL + "\t\tcom.sap.conn.idoc.IDocRepository iDocRepository_";
  protected final String TEXT_34 = " = com.sap.conn.idoc.jco.JCoIDoc.getIDocRepository(destination_";
  protected final String TEXT_35 = ");" + NL + "\t\tString tid_";
  protected final String TEXT_36 = " = destination_";
  protected final String TEXT_37 = ".createTID();" + NL + "\t\tcom.sap.conn.idoc.IDocFactory iDocFactory_";
  protected final String TEXT_38 = " = com.sap.conn.idoc.jco.JCoIDoc.getIDocFactory();" + NL + "\t";
  protected final String TEXT_39 = NL + "\t\t\torg.talend.sap.ISAPConnection connection_";
  protected final String TEXT_40 = " = (org.talend.sap.ISAPConnection)globalMap.get(\"conn_";
  protected final String TEXT_41 = "\");";
  protected final String TEXT_42 = NL + "\t\t\tif(connection_";
  protected final String TEXT_43 = " == null){" + NL + "\t\t\t\tconnection_";
  protected final String TEXT_44 = " = ((org.talend.sap.impl.SAPConnectionFactory)(org.talend.sap.impl.SAPConnectionFactory.getInstance())).createConnection(";
  protected final String TEXT_45 = ");" + NL + "\t\t\t}";
  protected final String TEXT_46 = NL + "\t\tproperties_";
  protected final String TEXT_47 = ".put(org.talend.sap.ISAPConnection.PROP_CLIENT, ";
  protected final String TEXT_48 = ");" + NL + "\t    properties_";
  protected final String TEXT_49 = ".put(org.talend.sap.ISAPConnection.PROP_USER, ";
  protected final String TEXT_50 = ");" + NL + "\t    properties_";
  protected final String TEXT_51 = ".put(org.talend.sap.ISAPConnection.PROP_PASSWORD, decryptedPassword_";
  protected final String TEXT_52 = ");" + NL + "\t    properties_";
  protected final String TEXT_53 = ".put(org.talend.sap.ISAPConnection.PROP_LANGUAGE, ";
  protected final String TEXT_54 = ");" + NL + "\t    " + NL + "\t    properties_";
  protected final String TEXT_55 = ".put(org.talend.sap.ISAPConnection.PROP_APPLICATION_SERVER_HOST, ";
  protected final String TEXT_56 = ");" + NL + "\t    properties_";
  protected final String TEXT_57 = ".put(org.talend.sap.ISAPConnection.PROP_SYSTEM_NUMBER, ";
  protected final String TEXT_58 = ");" + NL + "\t    " + NL + "\t    org.talend.sap.ISAPConnection connection_";
  protected final String TEXT_59 = " = org.talend.sap.impl.SAPConnectionFactory.getInstance().createConnection(properties_";
  protected final String TEXT_60 = ");" + NL + "\t    resourceMap.put(\"connection_";
  protected final String TEXT_61 = "\", connection_";
  protected final String TEXT_62 = ");";
  protected final String TEXT_63 = "    " + NL + "    org.talend.sap.service.ISAPIDocService service_";
  protected final String TEXT_64 = " = connection_";
  protected final String TEXT_65 = ".getIDocService();" + NL + "    org.talend.sap.idoc.ISAPIDocSupport support_";
  protected final String TEXT_66 = " = org.talend.sap.impl.idoc.SAPIDocSupport.getInstance();" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	boolean startIterate = false;
	
	String client = ElementParameterParser.getValue(node, "__CLIENT__");
	String userid = ElementParameterParser.getValue(node, "__USERID__");
	String language = ElementParameterParser.getValue(node, "__LANGUAGE__");
	String hostname = ElementParameterParser.getValue(node, "__HOSTNAME__");
	String systemnumber = ElementParameterParser.getValue(node, "__SYSTEMNUMBER__");
	
	boolean fromXML = "true".equals(ElementParameterParser.getValue(node, "__FROM_XML__"));
	
	String passwordFieldName = "__PASSWORD__";
	
	List<Map<String, String>> sapProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__SAP_PROPERTIES__");    
	
	boolean useExistingConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
	
	INode connectionInfoNode = null;
	boolean useAliasFromConnection = false;
	String alias = null;
	
	if(useExistingConn) {
		for (INode processNode : node.getProcess().getGeneratingNodes()) {
			if(connection.equals(processNode.getUniqueName())) {
				connectionInfoNode = processNode; 
				break;
			}
		}
		
		boolean specify_alias = "true".equals(ElementParameterParser.getValue(connectionInfoNode, "__SPECIFY_DATASOURCE_ALIAS__"));
		alias = ElementParameterParser.getValue(connectionInfoNode, "__SAP_DATASOURCE_ALIAS__");
		
		useAliasFromConnection = specify_alias && null != alias && !("".equals(alias));
	} else {
		if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_4);
    
		} else {
		
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_7);
    
		}
	
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    
	    if(sapProps!=null) {
			for(Map<String, String> item : sapProps){
			
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(item.get("PROPERTY") );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(item.get("VALUE") );
    stringBuffer.append(TEXT_13);
     
			}
	    }
    }
	
    stringBuffer.append(TEXT_14);
    if(fromXML) {
    
		if(useExistingConn) {

    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_17);
    
			if(useAliasFromConnection) {

    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(alias);
    stringBuffer.append(TEXT_21);
    
			}
		} else {

    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(client);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(userid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(language);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(hostname);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(systemnumber);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
		}

    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    } else {
    
		if(useExistingConn) {

    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_41);
    
			if(useAliasFromConnection) {

    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(alias);
    stringBuffer.append(TEXT_45);
    
			}
		} else {

    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(client);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(userid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(language);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(hostname);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(systemnumber);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    
		}

    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    }
    return stringBuffer.toString();
  }
}
