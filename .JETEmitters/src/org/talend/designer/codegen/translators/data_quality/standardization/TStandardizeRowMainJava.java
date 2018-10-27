package org.talend.designer.codegen.translators.data_quality.standardization;

import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;

public class TStandardizeRowMainJava
{
  protected static String nl;
  public static synchronized TStandardizeRowMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TStandardizeRowMainJava result = new TStandardizeRowMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = NL + "engine_";
  protected final String TEXT_2 = ".parseOnly(";
  protected final String TEXT_3 = ".";
  protected final String TEXT_4 = ");" + NL + "" + NL + "//System.err.println(error_";
  protected final String TEXT_5 = ".getStatus());" + NL + "//System.err.println(org.talend.dataquality.parser.util.RecognitionError.getStatus());" + NL + "" + NL + "if (org.talend.dataquality.parser.util.RecognitionError.getStatus()) {";
  protected final String TEXT_6 = NL + "        if (";
  protected final String TEXT_7 = " == null) {";
  protected final String TEXT_8 = NL + "            ";
  protected final String TEXT_9 = " = new ";
  protected final String TEXT_10 = "Struct();" + NL + "        }" + NL;
  protected final String TEXT_11 = NL + "        ";
  protected final String TEXT_12 = ".NORMALIZED_FIELD = engine_";
  protected final String TEXT_13 = ".normalize(";
  protected final String TEXT_14 = ");" + NL;
  protected final String TEXT_15 = NL + "            ";
  protected final String TEXT_16 = ".STANDARDIZED_FIELD = engine_";
  protected final String TEXT_17 = ".standardize(";
  protected final String TEXT_18 = ");";
  protected final String TEXT_19 = NL + "        ";
  protected final String TEXT_20 = NL + "                                ";
  protected final String TEXT_21 = ".";
  protected final String TEXT_22 = " = ";
  protected final String TEXT_23 = ".";
  protected final String TEXT_24 = ";";
  protected final String TEXT_25 = NL + "        ";
  protected final String TEXT_26 = NL + "        ";
  protected final String TEXT_27 = " = null;";
  protected final String TEXT_28 = NL + "} else {";
  protected final String TEXT_29 = NL + "        if (";
  protected final String TEXT_30 = " == null) {";
  protected final String TEXT_31 = NL + "            ";
  protected final String TEXT_32 = " = new ";
  protected final String TEXT_33 = "Struct();" + NL + "        }" + NL;
  protected final String TEXT_34 = NL + "        ";
  protected final String TEXT_35 = ".error_message = org.talend.dataquality.parser.util.RecognitionError.getMessage();";
  protected final String TEXT_36 = NL + "                                ";
  protected final String TEXT_37 = ".";
  protected final String TEXT_38 = " = ";
  protected final String TEXT_39 = ".";
  protected final String TEXT_40 = ";";
  protected final String TEXT_41 = NL + "        ";
  protected final String TEXT_42 = " = null;";
  protected final String TEXT_43 = NL + "}" + NL + "org.talend.dataquality.parser.util.RecognitionError.reset();";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

boolean useJsonOutput = Boolean.valueOf(ElementParameterParser.getValue(node, "__USE_JSON_OUTPUT__"));

List<? extends IConnection> inConns = node.getIncomingConnections(EConnectionType.FLOW_MAIN);
IConnection inConn = null; 
IMetadataTable preMetadata; 

// get incoming connection
if (inConns == null || inConns.isEmpty()) {
    return "";
} else {
    inConn = inConns.get(0);
}
String sInConnName = null;

// get incoming connection name
if (inConn == null || !inConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
    return "";
} else {
    sInConnName = inConn.getName();
}
// get outgoing connection
List<? extends IConnection> outConns = node.getOutgoingSortedConnections();
String sOutMainConnName = null, sOutRejectConnName = null;

//for (IConnection outConn : outConns) {
//  if (outConn.isActivate() && outConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
//      sOutMainConnName = outConn.getName();
//      break;
//  }
//}
for (IConnection outConn : outConns) {
  if (outConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) { 
    if (outConn.isActivate()){
        String connOutCntorName = outConn.getConnectorName();
        String connOutFlowName = outConn.getName();

        if ("REJECT".equalsIgnoreCase(connOutCntorName)){
            sOutRejectConnName = connOutFlowName;
        } else {
            sOutMainConnName = connOutFlowName;
        }
    }
  }
}

if (sOutMainConnName == null) return "";
boolean bStandardized = "true".equals(ElementParameterParser.getValue(node, "__STANDARDIZED__"));
String columnName =  ElementParameterParser.getValue(node, "__COLUMN_TO_PARSE__");

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(sInConnName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    if (sOutMainConnName != null) {
    stringBuffer.append(TEXT_6);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(useJsonOutput);
    stringBuffer.append(TEXT_14);
    if (bStandardized) {
    stringBuffer.append(TEXT_15);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(useJsonOutput);
    stringBuffer.append(TEXT_18);
    }
    stringBuffer.append(TEXT_19);
    
        for (IConnection outConn : outConns) {
          if (outConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) { 
            if (outConn.isActivate() && outConn.getName().equals(sOutMainConnName)){
                List<IMetadataTable> metadatas = node.getMetadataList();
                if ((metadatas!=null)&&(metadatas.size()>0)) {
                    IMetadataTable metadata = metadatas.get(0);
                    if (metadata!=null) {
                        //IConnection inConnection = node.getIncomingConnections().get(0);
                        List<IMetadataColumn> columns = metadata.getListColumns();
                        int sizeColumns = columns.size();
                        for(int i=0; i<sizeColumns; i++) {
                            if(!("NORMALIZED_FIELD").equals(columns.get(i).getLabel())&&!("STANDARDIZED_FIELD").equals(columns.get(i).getLabel())) {

    stringBuffer.append(TEXT_20);
    stringBuffer.append(outConn.getName());
    stringBuffer.append(TEXT_21);
    stringBuffer.append(columns.get(i).getLabel());
    stringBuffer.append(TEXT_22);
    stringBuffer.append(inConn.getName());
    stringBuffer.append(TEXT_23);
    stringBuffer.append(columns.get(i).getLabel());
    stringBuffer.append(TEXT_24);
    
                            }                  
                        }
                    }
                }
            }
          }
        }

    stringBuffer.append(TEXT_25);
    
    }
    
    if (sOutRejectConnName != null) {
    
    stringBuffer.append(TEXT_26);
    stringBuffer.append(sOutRejectConnName);
    stringBuffer.append(TEXT_27);
    }
    stringBuffer.append(TEXT_28);
    if (sOutRejectConnName != null) {
    stringBuffer.append(TEXT_29);
    stringBuffer.append(sOutRejectConnName);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(sOutRejectConnName);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(sOutRejectConnName);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(sOutRejectConnName);
    stringBuffer.append(TEXT_35);
    
        for (IConnection outConn : outConns) {
          if (outConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) { 
            if (outConn.isActivate() && outConn.getName().equals(sOutRejectConnName)){
                List<IMetadataTable> metadatas = node.getMetadataList();
                if ((metadatas!=null)&&(metadatas.size()>0)) {
                    IMetadataTable metadata = metadatas.get(0);
                    if (metadata!=null) {
                        //IConnection inConnection = node.getIncomingConnections().get(0);
                        List<IMetadataColumn> columns = metadata.getListColumns();
                        int sizeColumns = columns.size();
                        for(int i=0; i<sizeColumns; i++) {
                            if(!("NORMALIZED_FIELD").equals(columns.get(i).getLabel())&&!("STANDARDIZED_FIELD").equals(columns.get(i).getLabel())) {

    stringBuffer.append(TEXT_36);
    stringBuffer.append(outConn.getName());
    stringBuffer.append(TEXT_37);
    stringBuffer.append(columns.get(i).getLabel());
    stringBuffer.append(TEXT_38);
    stringBuffer.append(inConn.getName());
    stringBuffer.append(TEXT_39);
    stringBuffer.append(columns.get(i).getLabel());
    stringBuffer.append(TEXT_40);
    
                            }                    
                        }
                    }
                }
            }
          }
        }
    }
    
    if (sOutMainConnName != null) {
    
    stringBuffer.append(TEXT_41);
    stringBuffer.append(sOutMainConnName);
    stringBuffer.append(TEXT_42);
    }
    stringBuffer.append(TEXT_43);
    return stringBuffer.toString();
  }
}
