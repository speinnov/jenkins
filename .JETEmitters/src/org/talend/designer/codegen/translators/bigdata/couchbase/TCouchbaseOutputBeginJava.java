package org.talend.designer.codegen.translators.bigdata.couchbase;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;

public class TCouchbaseOutputBeginJava
{
  protected static String nl;
  public static synchronized TCouchbaseOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCouchbaseOutputBeginJava result = new TCouchbaseOutputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "\tcom.couchbase.client.CouchbaseClient client_";
  protected final String TEXT_3 = " = null;" + NL + "\t";
  protected final String TEXT_4 = NL + "\t    client_";
  protected final String TEXT_5 = " = (com.couchbase.client.CouchbaseClient)globalMap.get(\"conn_";
  protected final String TEXT_6 = "\");" + NL + "\t    ";
  protected final String TEXT_7 = "\t" + NL + "\t\t\tif(client_";
  protected final String TEXT_8 = " != null) {" + NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_9 = " - Get an existing client from \" + \"";
  protected final String TEXT_10 = "\" + \".\");" + NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_11 = NL + "\t\t\tSystem.setProperty(\"net.spy.log.LoggerImpl\", \"net.spy.memcached.compat.log.Log4JLogger\");" + NL + "\t\t";
  protected final String TEXT_12 = NL + "\t    java.util.ArrayList<java.net.URI> uris_";
  protected final String TEXT_13 = " = new java.util.ArrayList<java.net.URI>();" + NL + "   \t\t";
  protected final String TEXT_14 = NL + "    \t\turis_";
  protected final String TEXT_15 = ".add(java.net.URI.create(";
  protected final String TEXT_16 = "));" + NL + "    \t";
  protected final String TEXT_17 = NL + "    \t";
  protected final String TEXT_18 = NL + "        ";
  protected final String TEXT_19 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_20 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_21 = ");";
  protected final String TEXT_22 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_23 = " = ";
  protected final String TEXT_24 = "; ";
  protected final String TEXT_25 = NL + "\t    \t" + NL + "    \t" + NL + "        client_";
  protected final String TEXT_26 = " = new com.couchbase.client.CouchbaseClient(uris_";
  protected final String TEXT_27 = ", ";
  protected final String TEXT_28 = ",";
  protected final String TEXT_29 = ",";
  protected final String TEXT_30 = " decryptedPassword_";
  protected final String TEXT_31 = ");" + NL + "\t";
  protected final String TEXT_32 = NL + "\tint nb_line_";
  protected final String TEXT_33 = " = 0;" + NL + "\tint nb_line_update_";
  protected final String TEXT_34 = " = 0;" + NL + "\tint nb_line_inserted_";
  protected final String TEXT_35 = " = 0;" + NL + "\tint nb_line_inserted_updated_";
  protected final String TEXT_36 = " = 0;" + NL + "\tint nb_line_deleted_";
  protected final String TEXT_37 = " = 0;" + NL + "\tint nb_line_rejected_";
  protected final String TEXT_38 = " = 0;" + NL + "\tboolean whetherReject_";
  protected final String TEXT_39 = " = false;";
  protected final String TEXT_40 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	
	INode node = (INode)codeGenArgument.getArgument();
	
	String cid = node.getUniqueName();
	boolean useExistingConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
	if(useExistingConn){
	    String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
		
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_6);
    
		if(isLog4jEnabled){
		
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_10);
    
		}
	}else{
	    String dbname = ElementParameterParser.getValue(node, "__DBNAME__");
	    String username = ElementParameterParser.getValue(node, "__USER__");

	    List<Map<String, String>> URIList = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__URIS__");
	    if(isLog4jEnabled){
		
    stringBuffer.append(TEXT_11);
    
		}
		
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
        for(java.util.Map<String,String> map:URIList){
    	
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(map.get("URI"));
    stringBuffer.append(TEXT_16);
    
        }
    	
    stringBuffer.append(TEXT_17);
    
        String passwordFieldName = "__PASS__";
        
    stringBuffer.append(TEXT_18);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_21);
    } else {
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_24);
    }
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_28);
    if(username!=null && !"".equals(username)){
    stringBuffer.append(username);
    stringBuffer.append(TEXT_29);
    }
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
	}
	
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(TEXT_40);
    return stringBuffer.toString();
  }
}
