package org.talend.designer.codegen.translators.technical;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.utils.TalendTextUtils;

public class TSAPInfoObjectOutputOutBeginJava
{
  protected static String nl;
  public static synchronized TSAPInfoObjectOutputOutBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPInfoObjectOutputOutBeginJava result = new TSAPInfoObjectOutputOutBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + NL + "\t";
  protected final String TEXT_3 = "\t    " + NL + "\t\torg.talend.sap.ISAPConnection connection_";
  protected final String TEXT_4 = " = (org.talend.sap.ISAPConnection)globalMap.get(\"conn_";
  protected final String TEXT_5 = "\");\t";
  protected final String TEXT_6 = NL + "\t\t\t\tif(connection_";
  protected final String TEXT_7 = " == null){" + NL + "\t\t\t\t\tconnection_";
  protected final String TEXT_8 = " = ((org.talend.sap.impl.SAPConnectionFactory)(org.talend.sap.impl.SAPConnectionFactory.getInstance())).createConnection(";
  protected final String TEXT_9 = ");" + NL + "\t\t\t\t}";
  protected final String TEXT_10 = NL + "\t";
  protected final String TEXT_11 = NL + "\t\torg.talend.sap.ISAPConnection connection_";
  protected final String TEXT_12 = " = null;";
  protected final String TEXT_13 = NL + "\t\t\t\tconnection_";
  protected final String TEXT_14 = " = ((org.talend.sap.impl.SAPConnectionFactory)(org.talend.sap.impl.SAPConnectionFactory.getInstance())).createConnection(";
  protected final String TEXT_15 = ");";
  protected final String TEXT_16 = NL + "\t\t\tif (connection_";
  protected final String TEXT_17 = " == null) {//}";
  protected final String TEXT_18 = NL + "\t\t";
  protected final String TEXT_19 = NL + NL + "\tclass PropertyUil_";
  protected final String TEXT_20 = " {" + NL + "\t\t" + NL + "        void validateAndSet(java.util.Properties p, String key, Object value) {" + NL + "        \tif(value==null) {" + NL + "        \t\tSystem.err.println(\"WARN : will ignore the property : \" + key + \" as setting the null value.\"); " + NL + "        \t\treturn;" + NL + "        \t}" + NL + "        \t" + NL + "        \tp.setProperty(key, String.valueOf(value));" + NL + "        }" + NL + "        " + NL + "\t}" + NL + "\t" + NL + "\tPropertyUil_";
  protected final String TEXT_21 = " pu_";
  protected final String TEXT_22 = " = new PropertyUil_";
  protected final String TEXT_23 = "();" + NL + "" + NL + "\t";
  protected final String TEXT_24 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_25 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_26 = ");";
  protected final String TEXT_27 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_28 = " = ";
  protected final String TEXT_29 = "; ";
  protected final String TEXT_30 = NL + "\tjava.util.Properties properties_";
  protected final String TEXT_31 = " = new java.util.Properties();" + NL + "    pu_";
  protected final String TEXT_32 = ".validateAndSet(properties_";
  protected final String TEXT_33 = ", org.talend.sap.ISAPConnection.PROP_CLIENT, ";
  protected final String TEXT_34 = ");" + NL + "    pu_";
  protected final String TEXT_35 = ".validateAndSet(properties_";
  protected final String TEXT_36 = ", org.talend.sap.ISAPConnection.PROP_USER, ";
  protected final String TEXT_37 = ");" + NL + "    pu_";
  protected final String TEXT_38 = ".validateAndSet(properties_";
  protected final String TEXT_39 = ", org.talend.sap.ISAPConnection.PROP_PASSWORD, decryptedPassword_";
  protected final String TEXT_40 = ");" + NL + "    pu_";
  protected final String TEXT_41 = ".validateAndSet(properties_";
  protected final String TEXT_42 = ", org.talend.sap.ISAPConnection.PROP_LANGUAGE, ";
  protected final String TEXT_43 = ");" + NL + "    ";
  protected final String TEXT_44 = NL + "    pu_";
  protected final String TEXT_45 = ".validateAndSet(properties_";
  protected final String TEXT_46 = ", org.talend.sap.ISAPConnection.PROP_APPLICATION_SERVER_HOST, ";
  protected final String TEXT_47 = ");" + NL + "    pu_";
  protected final String TEXT_48 = ".validateAndSet(properties_";
  protected final String TEXT_49 = ", org.talend.sap.ISAPConnection.PROP_SYSTEM_NUMBER, ";
  protected final String TEXT_50 = ");";
  protected final String TEXT_51 = NL + "    pu_";
  protected final String TEXT_52 = ".validateAndSet(properties_";
  protected final String TEXT_53 = ", \"jco.client.mshost\", ";
  protected final String TEXT_54 = ");" + NL + "    pu_";
  protected final String TEXT_55 = ".validateAndSet(properties_";
  protected final String TEXT_56 = ", \"jco.client.r3name\", ";
  protected final String TEXT_57 = ");" + NL + "    pu_";
  protected final String TEXT_58 = ".validateAndSet(properties_";
  protected final String TEXT_59 = ", \"jco.client.group\", ";
  protected final String TEXT_60 = ");";
  protected final String TEXT_61 = NL + "    " + NL + "\t";
  protected final String TEXT_62 = NL + "\t\tpu_";
  protected final String TEXT_63 = ".validateAndSet(properties_";
  protected final String TEXT_64 = ", ";
  protected final String TEXT_65 = " ,";
  protected final String TEXT_66 = ");" + NL + "\t\t";
  protected final String TEXT_67 = NL + "        " + NL + "    \tconnection_";
  protected final String TEXT_68 = " = org.talend.sap.impl.SAPConnectionFactory.getInstance().createConnection(properties_";
  protected final String TEXT_69 = ");";
  protected final String TEXT_70 = NL + "\t\t\t//{" + NL + "\t\t\t}";
  protected final String TEXT_71 = NL + "\tresourceMap.put(\"conn_";
  protected final String TEXT_72 = "\", connection_";
  protected final String TEXT_73 = ");" + NL + "\t" + NL + "\torg.talend.sap.service.ISAPBWService dataService_";
  protected final String TEXT_74 = " = connection_";
  protected final String TEXT_75 = ".getBWService();" + NL + "\t";
  protected final String TEXT_76 = NL + NL + "\torg.talend.sap.bw.ISAPBWTableDataWritable data_";
  protected final String TEXT_77 = " = dataService_";
  protected final String TEXT_78 = NL + "\t";
  protected final String TEXT_79 = NL + "\t\t.createInfoObjectAttributeRequest(";
  protected final String TEXT_80 = ")" + NL + "\t";
  protected final String TEXT_81 = NL + "\t\t.createInfoObjectTextRequest(";
  protected final String TEXT_82 = ")" + NL + "\t";
  protected final String TEXT_83 = NL + "\t\t.addField(\"";
  protected final String TEXT_84 = "\")";
  protected final String TEXT_85 = NL + "      \t.protocolWork(";
  protected final String TEXT_86 = ")" + NL + "      \t";
  protected final String TEXT_87 = NL + " \t\t.updateAllAttributes(";
  protected final String TEXT_88 = ")" + NL + " \t\t";
  protected final String TEXT_89 = NL + " \t\t" + NL + "\t\t.writable();";

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
	
	String client = ElementParameterParser.getValue(node, "__CLIENT__");
	String userid = ElementParameterParser.getValue(node, "__USERID__");
	String password = ElementParameterParser.getValue(node, "__PASSWORD__");
	String language = ElementParameterParser.getValue(node, "__LANGUAGE__");
	String hostname = ElementParameterParser.getValue(node, "__HOSTNAME__");
	String systemnumber = ElementParameterParser.getValue(node, "__SYSTEMNUMBER__");
	
	String systemId = ElementParameterParser.getValue(node,"__SYSTEMID__");
	String groupName = ElementParameterParser.getValue(node,"__GROUPNAME__");
	
	String serverType = ElementParameterParser.getValue(node,"__SERVERTYPE__");
	
	String tableName = ElementParameterParser.getValue(node, "__TABLE__");
	
	List<Map<String, String>> sapProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__SAP_PROPERTIES__");
	
	String passwordFieldName = "__PASSWORD__";
	
	String data_action = ElementParameterParser.getValue(node, "__DATA_ACTION__");
	
    boolean useExistingConn = ("true").equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");

    stringBuffer.append(TEXT_2);
    if(useExistingConn){
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_5);
    	
		INode connectionNode = null; 
		for (INode processNode : node.getProcess().getGeneratingNodes()) { 
			if(connection.equals(processNode.getUniqueName())) { 
				connectionNode = processNode; 
				break; 
			} 
		} 
		boolean specify_alias = "true".equals(ElementParameterParser.getValue(connectionNode, "__SPECIFY_DATASOURCE_ALIAS__"));
		if(specify_alias){
			String alias = ElementParameterParser.getValue(connectionNode, "__SAP_DATASOURCE_ALIAS__");
			if(null != alias && !("".equals(alias))){

    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(alias);
    stringBuffer.append(TEXT_9);
    
			}
		}

    stringBuffer.append(TEXT_10);
    }else{
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    
		boolean specify_alias = "true".equals(ElementParameterParser.getValue(node, "__SPECIFY_DATASOURCE_ALIAS__"));
		if(specify_alias){
			String alias = ElementParameterParser.getValue(node, "__SAP_DATASOURCE_ALIAS__");
			if(null != alias && !("".equals(alias))){

    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(alias);
    stringBuffer.append(TEXT_15);
    
			}

    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    
		}

    stringBuffer.append(TEXT_18);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_26);
    } else {
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_29);
    }
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(client);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(userid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(language);
    stringBuffer.append(TEXT_43);
    if("ApplicationServer".equals(serverType)){
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(hostname);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(systemnumber);
    stringBuffer.append(TEXT_50);
    }else{
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(hostname);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(systemId);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(groupName);
    stringBuffer.append(TEXT_60);
    }
    stringBuffer.append(TEXT_61);
    
    if(sapProps!=null) {
		for(Map<String, String> item : sapProps){
		
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(item.get("PROPERTY") );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(item.get("VALUE") );
    stringBuffer.append(TEXT_66);
     
		}
    }
	
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    
		if(specify_alias){

    stringBuffer.append(TEXT_70);
    
		}
	}

    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    
	String info_object_type = ElementParameterParser.getValue(node, "__INFO_OBJECT_TYPE__");

    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    if("ATTRIBUTE".equals(info_object_type)) {
    stringBuffer.append(TEXT_79);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_80);
    } else {
    stringBuffer.append(TEXT_81);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_82);
    }
    
	for(int i=0;i<columnList.size();i++) {
		IMetadataColumn column = columnList.get(i);
		String tableField = column.getOriginalDbColumnName();

    stringBuffer.append(TEXT_83);
    stringBuffer.append(tableField);
    stringBuffer.append(TEXT_84);
    
	}
	
	boolean on_submit_protocol_work = "true".equals(ElementParameterParser.getValue(node, "__ON_SUBMIT_PROTOCOL_WORK__"));
	boolean on_submit_update_all_attributes = "true".equals(ElementParameterParser.getValue(node, "__ON_SUBMIT_UPDATE_ALL_ATTRIBUTES__"));

    stringBuffer.append(TEXT_85);
    stringBuffer.append(on_submit_protocol_work);
    stringBuffer.append(TEXT_86);
    if("ATTRIBUTE".equals(info_object_type)) {
    stringBuffer.append(TEXT_87);
    stringBuffer.append(on_submit_update_all_attributes);
    stringBuffer.append(TEXT_88);
    }
    stringBuffer.append(TEXT_89);
    return stringBuffer.toString();
  }
}
