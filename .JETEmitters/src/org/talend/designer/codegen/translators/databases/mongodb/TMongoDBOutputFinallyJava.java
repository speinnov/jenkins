package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;

public class TMongoDBOutputFinallyJava
{
  protected static String nl;
  public static synchronized TMongoDBOutputFinallyJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBOutputFinallyJava result = new TMongoDBOutputFinallyJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\tif(resourceMap.get(\"finish_";
  protected final String TEXT_2 = "\") == null){" + NL + "\t\t\tif(resourceMap.get(\"mongo_";
  protected final String TEXT_3 = "\") != null){" + NL + "\t\t\t\t";
  protected final String TEXT_4 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_5 = " - Closing the connection \" + ((com.mongodb.Mongo)resourceMap.get(\"mongo_";
  protected final String TEXT_6 = "\")).getServerAddressList() + \".\");" + NL + "\t\t\t";
  protected final String TEXT_7 = NL + "\t\t\t\t((com.mongodb.Mongo)resourceMap.get(\"mongo_";
  protected final String TEXT_8 = "\")).close();" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_10 = " - The connection was closed successfully.\");" + NL + "\t\t\t";
  protected final String TEXT_11 = NL + "\t\t\t}" + NL + "\t\t}";
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    
			}
			
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    
			}
			
    stringBuffer.append(TEXT_11);
    
	}

    stringBuffer.append(TEXT_12);
    return stringBuffer.toString();
  }
}
