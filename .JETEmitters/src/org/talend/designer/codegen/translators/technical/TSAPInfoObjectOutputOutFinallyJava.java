package org.talend.designer.codegen.translators.technical;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TSAPInfoObjectOutputOutFinallyJava
{
  protected static String nl;
  public static synchronized TSAPInfoObjectOutputOutFinallyJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPInfoObjectOutputOutFinallyJava result = new TSAPInfoObjectOutputOutFinallyJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t\tif(resourceMap.get(\"finish_";
  protected final String TEXT_3 = "\")==null){" + NL + "\t\t\tif(resourceMap.get(\"conn_";
  protected final String TEXT_4 = "\")!=null){" + NL + "\t\t\t\t((org.talend.sap.ISAPConnection)resourceMap.get(\"conn_";
  protected final String TEXT_5 = "\")).close();" + NL + "\t\t\t}" + NL + "\t\t}" + NL + "\t";
  protected final String TEXT_6 = NL;

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
	
	boolean useExistingConn = ("true").equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	
	if(!useExistingConn) {
	
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
	}
    
    stringBuffer.append(TEXT_6);
    return stringBuffer.toString();
  }
}
