package org.talend.designer.codegen.translators.business.sap;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.utils.TalendTextUtils;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class TSAPDataSourceReceiverEndJava
{
  protected static String nl;
  public static synchronized TSAPDataSourceReceiverEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPDataSourceReceiverEndJava result = new TSAPDataSourceReceiverEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL + "\t\tmessage_";
  protected final String TEXT_4 = ".acknowledge();" + NL + "\t";
  protected final String TEXT_5 = NL + "\t\t//try {" + NL + "\t\t\tif(session_";
  protected final String TEXT_6 = " != null && connection_";
  protected final String TEXT_7 = " != null) {" + NL + "\t\t\t\tsession_";
  protected final String TEXT_8 = ".commit();" + NL + "\t\t\t}" + NL + "\t\t}catch(java.lang.Exception e_";
  protected final String TEXT_9 = "){" + NL + "\t\t\tif (session_";
  protected final String TEXT_10 = " != null  && connection_";
  protected final String TEXT_11 = " != null) {" + NL + "\t\t\t\tsession_";
  protected final String TEXT_12 = ".rollback();" + NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_13 = NL + "\t//{" + NL + "\t}" + NL + "\t" + NL + "\tif (session_";
  protected final String TEXT_14 = " != null  && connection_";
  protected final String TEXT_15 = " != null) {" + NL + " \t\tsession_";
  protected final String TEXT_16 = ".close();" + NL + " \t}" + NL + " \t" + NL + "    if (connection_";
  protected final String TEXT_17 = " != null) {" + NL + "    \tconnection_";
  protected final String TEXT_18 = ".close();" + NL + "    }" + NL + "} finally {" + NL + "\tif (connection_";
  protected final String TEXT_19 = " != null) {" + NL + " \t\tconnection_";
  protected final String TEXT_20 = ".close();" + NL + "\t}" + NL + "}";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	List<? extends IConnection> outputConnections = node.getOutgoingConnections();
	
	if(outputConnections==null || outputConnections.isEmpty()){
		return "";
	}
	
	IConnection outputConnection = outputConnections.get(0);
	
	boolean linktHMap =  "true".equals(ElementParameterParser.getValue(node, "__LINK_THMAP__"));
	String hid = ElementParameterParser.getValue(node, "__MAPPING__");
	if(linktHMap && (hid == null || "".equals(hid))) {
		return "";
	}
	
	boolean transacted = "true".equals(ElementParameterParser.getValue(node, "__IS_TRANSACTED__"));
	String acknowledgmentMode = ElementParameterParser.getValue(node, "__ACKNOWLEDGMENT_MODE__");
	
	if ("CLIENT_ACKNOWLEDGE".equals(acknowledgmentMode)) {
	
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    
	}
	
	if(transacted) {

    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    
	}

    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_20);
    return stringBuffer.toString();
  }
}
