package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;

public class TMongoDBOutputEndJava
{
  protected static String nl;
  public static synchronized TMongoDBOutputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBOutputEndJava result = new TMongoDBOutputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t// if bulkWriteOperationCounter_";
  protected final String TEXT_2 = " == 1 the ulkWriteOperation_";
  protected final String TEXT_3 = " is empty, do not execute." + NL + "\t\tif (bulkWriteOperationCounter_";
  protected final String TEXT_4 = " != 1) {" + NL + "\t\t    bulkWriteOperation_";
  protected final String TEXT_5 = ".execute();" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_6 = NL + "\t\tlog.info(\"";
  protected final String TEXT_7 = " - Has been ";
  protected final String TEXT_8 = " \" + nb_line_";
  protected final String TEXT_9 = " + \" records.\");" + NL + "\t";
  protected final String TEXT_10 = NL + "\t\tif(mongo_";
  protected final String TEXT_11 = " != null){" + NL + "\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_13 = " - Closing the connection \" + mongo_";
  protected final String TEXT_14 = ".getServerAddressList() + \".\");" + NL + "\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\tmongo_";
  protected final String TEXT_16 = ".close();" + NL + "\t\t\t";
  protected final String TEXT_17 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_18 = " - The connection was closed successfully.\");" + NL + "\t\t\t";
  protected final String TEXT_19 = NL + "\t\t}" + NL + "\t\tresourceMap.put(\"finish_";
  protected final String TEXT_20 = "\", true); " + NL + "\t";
  protected final String TEXT_21 = NL + "\tglobalMap.put(\"";
  protected final String TEXT_22 = "_NB_LINE\", nb_line_";
  protected final String TEXT_23 = ");";
  protected final String TEXT_24 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
	String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
	String dataAction = ElementParameterParser.getValue(node,"__DATA_ACTION__");
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

	// BulkWrite is only supportted on Mongo 2.6+ drivers
	String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
	boolean setBulkWrite = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_BULK_WRITE__"));
	final boolean bulkWrite = setBulkWrite & (!"MONGODB_2_5_X".equalsIgnoreCase(dbversion));

	// BulkWrite 
	// Last write if number of rows added is less than bulksize
	if(bulkWrite){
		
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
	}

	if(isLog4jEnabled){
	
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(dataAction.toLowerCase()+(dataAction.endsWith("E")?"d":"ed"));
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    
	}
	if(!"true".equals(useExistingConn)){
	
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
			}
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    
			}
			
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    
	}
	
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(TEXT_24);
    return stringBuffer.toString();
  }
}
