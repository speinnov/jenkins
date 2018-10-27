package org.talend.designer.codegen.translators.databases.azure.cosmosdb;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;

public class TCosmosDBRowEndJava
{
  protected static String nl;
  public static synchronized TCosmosDBRowEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCosmosDBRowEndJava result = new TCosmosDBRowEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\tif(mongo_";
  protected final String TEXT_2 = " != null){" + NL + "\t\t\t";
  protected final String TEXT_3 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_4 = " - Closing the connection \" + mongo_";
  protected final String TEXT_5 = ".getServerAddressList() + \".\");" + NL + "\t\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\tmongo_";
  protected final String TEXT_7 = ".close();" + NL + "\t\t\t";
  protected final String TEXT_8 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_9 = " - The connection was closed successfully.\");" + NL + "\t\t\t";
  protected final String TEXT_10 = NL + "\t\t}" + NL + "\t\tresourceMap.put(\"finish_";
  protected final String TEXT_11 = "\", true); " + NL + "\t";
  protected final String TEXT_12 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
	String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
	if(!"true".equals(useExistingConn)){
	
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
			}
			
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    
			}
			
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
	}
	
    stringBuffer.append(TEXT_12);
    return stringBuffer.toString();
  }
}
