package org.talend.designer.codegen.translators.business.sap;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TSAPIDocOutputMainJava
{
  protected static String nl;
  public static synchronized TSAPIDocOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPIDocOutputMainJava result = new TSAPIDocOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t\tif(";
  protected final String TEXT_3 = ".";
  protected final String TEXT_4 = "!=null) {" + NL + "\t\t\ttry {" + NL + "\t\t\t\tservice_";
  protected final String TEXT_5 = ".send(support_";
  protected final String TEXT_6 = ".from(";
  protected final String TEXT_7 = ".";
  protected final String TEXT_8 = ".toCharArray()));" + NL + "\t\t\t} catch(java.lang.Exception e_";
  protected final String TEXT_9 = ") {" + NL + "\t\t\t\t";
  protected final String TEXT_10 = NL + "\t\t\t\t\tthrow(e_";
  protected final String TEXT_11 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\t\tSystem.err.println(e_";
  protected final String TEXT_13 = ".getMessage());" + NL + "\t\t\t\t";
  protected final String TEXT_14 = NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_15 = NL + "\t" + NL + "\tString iDocXML_";
  protected final String TEXT_16 = " = null;" + NL + "\tjava.io.FileReader fileReader_";
  protected final String TEXT_17 = ";" + NL + "" + NL + "\ttry {" + NL + "\t\tfileReader_";
  protected final String TEXT_18 = " = new java.io.FileReader(";
  protected final String TEXT_19 = ");" + NL + "\t\tjava.io.BufferedReader br_";
  protected final String TEXT_20 = " = new java.io.BufferedReader(fileReader_";
  protected final String TEXT_21 = ");" + NL + "\t\tjava.lang.StringBuffer sb_";
  protected final String TEXT_22 = " = new java.lang.StringBuffer();" + NL + "\t\tString line_";
  protected final String TEXT_23 = ";" + NL + "\t\twhile ((line_";
  protected final String TEXT_24 = " = br_";
  protected final String TEXT_25 = ".readLine()) != null)" + NL + "\t\t{" + NL + "\t\t\tsb_";
  protected final String TEXT_26 = ".append(line_";
  protected final String TEXT_27 = ");" + NL + "\t\t}" + NL + "\t\tiDocXML_";
  protected final String TEXT_28 = " = sb_";
  protected final String TEXT_29 = ".toString();" + NL + "\t\t" + NL + "\t\t" + NL + "\t\tbr_";
  protected final String TEXT_30 = ".close();" + NL + "\t\tfileReader_";
  protected final String TEXT_31 = ".close();" + NL + "\t\t\t\t}" + NL + "\tcatch(Exception ex)" + NL + "\t{" + NL + "\t\tex.printStackTrace();" + NL + "\t}" + NL + "\t" + NL + "\tcom.sap.conn.idoc.IDocDocumentList iDocList_";
  protected final String TEXT_32 = "=null;" + NL + "\tcom.sap.conn.idoc.IDocXMLProcessor processor_";
  protected final String TEXT_33 = "=iDocFactory_";
  protected final String TEXT_34 = ".getIDocXMLProcessor();" + NL + "\ttry {" + NL + "\t\tiDocList_";
  protected final String TEXT_35 = "=processor_";
  protected final String TEXT_36 = ".parse(iDocRepository_";
  protected final String TEXT_37 = ", iDocXML_";
  protected final String TEXT_38 = ");" + NL + "\t} catch (com.sap.conn.idoc.IDocParseException e){" + NL + "\t\t";
  protected final String TEXT_39 = NL + "\t\t\tthrow(e);" + NL + "\t\t";
  protected final String TEXT_40 = NL + "\t\t\tSystem.err.println(e.getMessage());" + NL + "\t\t";
  protected final String TEXT_41 = NL + "\t}" + NL + "\t" + NL + "\ttry {" + NL + "\t\tcom.sap.conn.idoc.jco.JCoIDoc.send(iDocList_";
  protected final String TEXT_42 = ", com.sap.conn.idoc.IDocFactory.IDOC_VERSION_DEFAULT, destination_";
  protected final String TEXT_43 = ", tid_";
  protected final String TEXT_44 = ");" + NL + "\t} catch(com.sap.conn.jco.JCoException e) {" + NL + "\t\t";
  protected final String TEXT_45 = NL + "\t\t\tthrow(e);" + NL + "\t\t";
  protected final String TEXT_46 = NL + "\t\t\tSystem.err.println(e.getMessage());" + NL + "\t\t";
  protected final String TEXT_47 = NL + "\t}" + NL + "\tdestination_";
  protected final String TEXT_48 = ".confirmTID(tid_";
  protected final String TEXT_49 = ");";
  protected final String TEXT_50 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	String dieOnError = ElementParameterParser.getValue(node, "__DIE_ON_ERROR__");
	boolean fromXML = "true".equals(ElementParameterParser.getValue(node, "__FROM_XML__"));
	
	if(!fromXML) {
		java.util.List<IMetadataColumn> columnList = null;
		java.util.List<IMetadataTable> metadatas = node.getMetadataList();
		if(metadatas != null && metadatas.size() > 0) {
			IMetadataTable metadata = metadatas.get(0);
			if(metadata != null) {
			    columnList = metadata.getListColumns();
			}
		}
		
		java.util.List<? extends IConnection> incomingConns = node.getIncomingConnections();
		if(incomingConns==null || incomingConns.isEmpty() || columnList==null || columnList.isEmpty()) {
			return "";
		}
		
		IConnection inputConn = null;
		for(IConnection incomingConn : incomingConns) {
			if(incomingConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
				inputConn = incomingConn;
				break;
			}
		}
		
		if(inputConn == null) {
			return "";
		}
		
		String idocColumn = ElementParameterParser.getValue(node, "__IDOC_COLUMN__");
		
		boolean findColumn = false;
		for(IMetadataColumn column : columnList) {
			if(column.getLabel().equals(idocColumn)) {
				findColumn = true;
				break;
			}
		}
		
		if(!findColumn) {
			return "";
		}

    stringBuffer.append(TEXT_2);
    stringBuffer.append(inputConn.getName());
    stringBuffer.append(TEXT_3);
    stringBuffer.append(idocColumn);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(inputConn.getName());
    stringBuffer.append(TEXT_7);
    stringBuffer.append(idocColumn);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    if(("true").equals(dieOnError)) {
				
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
				}else {
				
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
				}
    stringBuffer.append(TEXT_14);
    
		return stringBuffer.toString();
	}
	
	String file_idoc_xml= ElementParameterParser.getValue(node, "__FILE_IDOC_XML__");

    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(file_idoc_xml);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
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
    if(("true").equals(dieOnError)) {
		
    stringBuffer.append(TEXT_39);
    
		}else {
		
    stringBuffer.append(TEXT_40);
    
		}
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    if(("true").equals(dieOnError)) {
		
    stringBuffer.append(TEXT_45);
    
		}else {
		
    stringBuffer.append(TEXT_46);
    
		}
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(TEXT_50);
    return stringBuffer.toString();
  }
}
