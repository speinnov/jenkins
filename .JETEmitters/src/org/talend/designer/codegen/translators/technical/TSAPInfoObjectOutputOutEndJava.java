package org.talend.designer.codegen.translators.technical;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TSAPInfoObjectOutputOutEndJava
{
  protected static String nl;
  public static synchronized TSAPInfoObjectOutputOutEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPInfoObjectOutputOutEndJava result = new TSAPInfoObjectOutputOutEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\tdata_";
  protected final String TEXT_3 = ".submit();\t";
  protected final String TEXT_4 = NL + "    if(connection_";
  protected final String TEXT_5 = "!=null && connection_";
  protected final String TEXT_6 = ".isAlive()) {" + NL + "    \tconnection_";
  protected final String TEXT_7 = ".close();" + NL + "    \tresourceMap.put(\"finish_";
  protected final String TEXT_8 = "\", true);" + NL + "    }";
  protected final String TEXT_9 = NL + "\tglobalMap.put(\"protocol_";
  protected final String TEXT_10 = "\", data_";
  protected final String TEXT_11 = ".getProtocol());";
  protected final String TEXT_12 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	List<? extends IConnection> inputConnections = node.getIncomingConnections();
	if((inputConnections == null) || (inputConnections.size() == 0)) {
		return "";
	}
	
	IConnection inputConnection = null;
	for(IConnection inputConn : inputConnections) {
		if(inputConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			inputConnection = inputConn;
			break;
		}
	}
	
	if(inputConnection == null) {
		return "";
	}
	
	List<IMetadataTable> metadatas = node.getMetadataList();
	if ((metadatas == null) && (metadatas.size() == 0) || (metadatas.get(0) == null)) {
		return "";
	}
	IMetadataTable metadata = metadatas.get(0);
	
	List<IMetadataColumn> columnList = metadata.getListColumns();
	if((columnList == null) || (columnList.size() == 0)) {
		return "";
	}

    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
	boolean useExistingConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	if(!useExistingConn) {

    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
	}
	
	boolean on_submit_protocol_work = "true".equals(ElementParameterParser.getValue(node, "__ON_SUBMIT_PROTOCOL_WORK__"));
	if(!on_submit_protocol_work) {
		return stringBuffer.toString();
	}
	
	String graphicalUniqueName = cid.replace("_Out","");

    stringBuffer.append(TEXT_9);
    stringBuffer.append(graphicalUniqueName);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(TEXT_12);
    return stringBuffer.toString();
  }
}
