package org.talend.designer.codegen.translators.databases.cassandra;

import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;

public class TCassandraBulkExecMainJava
{
  protected static String nl;
  public static synchronized TCassandraBulkExecMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCassandraBulkExecMainJava result = new TCassandraBulkExecMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = " \t" + NL + "java.io.File file_";
  protected final String TEXT_2 = " = new java.io.File(";
  protected final String TEXT_3 = ", ";
  protected final String TEXT_4 = "+\"/\"+";
  protected final String TEXT_5 = ");" + NL + "" + NL + "java.util.List<String> args_";
  protected final String TEXT_6 = " = new java.util.ArrayList<String>();" + NL + "args_";
  protected final String TEXT_7 = ".add(\"-d\"+";
  protected final String TEXT_8 = ");" + NL + "args_";
  protected final String TEXT_9 = ".add(\"-p\"+";
  protected final String TEXT_10 = ");";
  protected final String TEXT_11 = NL;
  protected final String TEXT_12 = NL;
  protected final String TEXT_13 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_14 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_15 = ");";
  protected final String TEXT_16 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_17 = " = ";
  protected final String TEXT_18 = "; ";
  protected final String TEXT_19 = NL + NL + "args_";
  protected final String TEXT_20 = ".add(\"-u\"+";
  protected final String TEXT_21 = ");" + NL + "" + NL + "args_";
  protected final String TEXT_22 = ".add(\"-pw\");" + NL + "args_";
  protected final String TEXT_23 = ".add(decryptedPassword_";
  protected final String TEXT_24 = ");" + NL;
  protected final String TEXT_25 = NL;
  protected final String TEXT_26 = NL + "\t";
  protected final String TEXT_27 = NL + "\t\targs_";
  protected final String TEXT_28 = ".add(\"-ks\");" + NL + "\t\targs_";
  protected final String TEXT_29 = ".add(";
  protected final String TEXT_30 = ");" + NL + "\t\targs_";
  protected final String TEXT_31 = ".add(\"-kspw\");" + NL + "\t\targs_";
  protected final String TEXT_32 = ".add(";
  protected final String TEXT_33 = ");" + NL + "\t";
  protected final String TEXT_34 = NL + "\targs_";
  protected final String TEXT_35 = ".add(\"-prtcl\");" + NL + "\targs_";
  protected final String TEXT_36 = ".add(\"TLS\");" + NL + "\targs_";
  protected final String TEXT_37 = ".add(\"-ts\");" + NL + "\targs_";
  protected final String TEXT_38 = ".add(";
  protected final String TEXT_39 = ");" + NL + "\targs_";
  protected final String TEXT_40 = ".add(\"-tspw\");" + NL + "\targs_";
  protected final String TEXT_41 = ".add(";
  protected final String TEXT_42 = ");";
  protected final String TEXT_43 = NL + "\tSystem.setProperty(\"cassandra.config\", \"file://\" + ";
  protected final String TEXT_44 = ");\t" + NL + "\targs_";
  protected final String TEXT_45 = ".add(\"-f\");" + NL + "\targs_";
  protected final String TEXT_46 = ".add(";
  protected final String TEXT_47 = ");";
  protected final String TEXT_48 = NL + NL + NL + "args_";
  protected final String TEXT_49 = ".add(file_";
  protected final String TEXT_50 = ".getAbsolutePath());";
  protected final String TEXT_51 = NL + "\tlog.info(\"";
  protected final String TEXT_52 = " - Loading the data from SSTable \"+file_";
  protected final String TEXT_53 = ".getAbsolutePath()+\"to the server.\");";
  protected final String TEXT_54 = NL + "org.apache.cassandra.tools.BulkLoader.main(args_";
  protected final String TEXT_55 = ".toArray(new String[args_";
  protected final String TEXT_56 = ".size()]));";
  protected final String TEXT_57 = NL + "\tlog.info(\"";
  protected final String TEXT_58 = " - Loaded the data to the server successfully.\");";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

String sstablePath = ElementParameterParser.getValue(node,"__SSTABLE_STORE_PATH__");
String keySpace = ElementParameterParser.getValue(node,"__KEY_SPACE__");
String columnFamily = ElementParameterParser.getValue(node,"__COLUMN_FAMILY__");
String host = ElementParameterParser.getValue(node,"__HOST__");
String port = ElementParameterParser.getValue(node,"__PORT__");
String version = ElementParameterParser.getValue(node,"__DB_VERSION__");
boolean requireAuth = "true".equals(ElementParameterParser.getValue(node,"__REQUIRED_AUTHENTICATION__"));
boolean needAuth = !"CASSANDRA_1_1_2".equals(version) && requireAuth;
String username = ElementParameterParser.getValue(node,"__USERNAME__");
String password = ElementParameterParser.getValue(node,"__PASSWORD__"); 
boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(sstablePath);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(keySpace);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(columnFamily);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(host);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(port);
    stringBuffer.append(TEXT_10);
    if(needAuth){
    stringBuffer.append(TEXT_11);
    
String passwordFieldName = "__PASSWORD__";

    stringBuffer.append(TEXT_12);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_15);
    } else {
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_18);
    }
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    }
    stringBuffer.append(TEXT_25);
    
class SSL_HELPER{
	INode node;
	
	public SSL_HELPER(INode node){
		this.node = node;
	}
	
	public boolean useHTTPS() {
        return node != null;
    }
	
	public String getTSType(){
		return ElementParameterParser.getValue(node, "__TRUSTSTORE_TYPE__");
	}
	
	public String getTSPath(){
		return ElementParameterParser.getValue(node,"__SSL_TRUSTSERVER_TRUSTSTORE__");
	}
	
	public String getTSPwd(){
		return ElementParameterParser.getPasswordValue(node,"__SSL_TRUSTSERVER_PASSWORD__");
	}
	
	public boolean needKS(){
		return ElementParameterParser.getBooleanValue(node,"__NEED_CLIENT_AUTH__");
	}
	
	public String getKSType(){
		return ElementParameterParser.getValue(node, "__KEYSTORE_TYPE__");
	}
	
	public String getKSPath(){
		return ElementParameterParser.getValue(node,"__SSL_KEYSTORE__");
	}
	
	public String getKSPwd(){
		return ElementParameterParser.getPasswordValue(node,"__SSL_KEYSTORE_PASSWORD__");
	}
	
	public boolean needVerifyHostname(){
		return ElementParameterParser.getBooleanValue(node,"__VERIFY_NAME__");
	}
}	

    
INode sslNode = ElementParameterParser.getLinkedNodeValue(node, "__HTTPS_SETTING__");
SSL_HELPER sslHelper = new SSL_HELPER(sslNode);
boolean useSSL = ElementParameterParser.getBooleanValue(node, "__USE_HTTPS__") && sslHelper.useHTTPS();
if(useSSL){

    stringBuffer.append(TEXT_26);
    if(sslHelper.needKS()){
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(sslHelper.getKSPath());
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(sslHelper.getKSPwd());
    stringBuffer.append(TEXT_33);
    }
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(sslHelper.getTSPath());
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(sslHelper.getTSPwd());
    stringBuffer.append(TEXT_42);
    
}
boolean useConfFile = ElementParameterParser.getBooleanValue(node, "__USE_CONF_FILE__");
if(useConfFile){
	String confFile = ElementParameterParser.getValue(node, "__CONF_FILE__");
	
    stringBuffer.append(TEXT_43);
    stringBuffer.append(confFile);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(confFile);
    stringBuffer.append(TEXT_47);
    
}

    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    
if (isLog4jEnabled) {

    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
}

    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    
if (isLog4jEnabled) {

    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    
}

    return stringBuffer.toString();
  }
}
