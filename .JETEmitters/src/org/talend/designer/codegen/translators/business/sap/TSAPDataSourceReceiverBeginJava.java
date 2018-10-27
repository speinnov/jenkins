package org.talend.designer.codegen.translators.business.sap;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.utils.TalendTextUtils;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class TSAPDataSourceReceiverBeginJava
{
  protected static String nl;
  public static synchronized TSAPDataSourceReceiverBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPDataSourceReceiverBeginJava result = new TSAPDataSourceReceiverBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL + "\tjavax.jms.Connection connection_";
  protected final String TEXT_4 = " = null;" + NL + "\ttry {" + NL + "\t//}" + NL + "\t";
  protected final String TEXT_5 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_6 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_7 = ");";
  protected final String TEXT_8 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_9 = " = ";
  protected final String TEXT_10 = "; ";
  protected final String TEXT_11 = NL + "\t" + NL + "\tString url_";
  protected final String TEXT_12 = " =\"\";" + NL + "\t";
  protected final String TEXT_13 = NL + "\t\turl_";
  protected final String TEXT_14 = " = \"failover:(\";" + NL + "\t\t";
  protected final String TEXT_15 = NL + "\t\t\turl_";
  protected final String TEXT_16 = " = url_";
  protected final String TEXT_17 = " +\",\";" + NL + "\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\turl_";
  protected final String TEXT_19 = " = url_";
  protected final String TEXT_20 = " +\"";
  protected final String TEXT_21 = "://\";" + NL + "\t\t\turl_";
  protected final String TEXT_22 = " = url_";
  protected final String TEXT_23 = " +";
  protected final String TEXT_24 = ";" + NL + "\t\t\turl_";
  protected final String TEXT_25 = " = url_";
  protected final String TEXT_26 = " +\":\";" + NL + "\t\t\turl_";
  protected final String TEXT_27 = " = url_";
  protected final String TEXT_28 = " +";
  protected final String TEXT_29 = ";" + NL + "\t\t\t";
  protected final String TEXT_30 = NL + "\t\turl_";
  protected final String TEXT_31 = " = url_";
  protected final String TEXT_32 = " +\")?randomize=false\";" + NL + "\t\t";
  protected final String TEXT_33 = NL + "\t\t\turl_";
  protected final String TEXT_34 = " = url_";
  protected final String TEXT_35 = " +\"&\"+";
  protected final String TEXT_36 = "+\"=\"+";
  protected final String TEXT_37 = ";" + NL + "\t\t";
  protected final String TEXT_38 = NL + "\t\turl_";
  protected final String TEXT_39 = " = \"";
  protected final String TEXT_40 = "://\"+";
  protected final String TEXT_41 = "+\":\"+";
  protected final String TEXT_42 = ";" + NL + "\t";
  protected final String TEXT_43 = NL + "\t" + NL + "\torg.apache.activemq.ActiveMQConnectionFactory factory_";
  protected final String TEXT_44 = " = new org.apache.activemq.ActiveMQConnectionFactory(url_";
  protected final String TEXT_45 = ");" + NL + "\t" + NL + "\t";
  protected final String TEXT_46 = NL + "\t\tconnection_";
  protected final String TEXT_47 = " = factory_";
  protected final String TEXT_48 = ".createConnection();" + NL + "\t";
  protected final String TEXT_49 = NL + "\t\tconnection_";
  protected final String TEXT_50 = " = factory_";
  protected final String TEXT_51 = ".createConnection(";
  protected final String TEXT_52 = ",decryptedPassword_";
  protected final String TEXT_53 = ");" + NL + "\t";
  protected final String TEXT_54 = NL + "\t" + NL + "\tconnection_";
  protected final String TEXT_55 = ".start();" + NL + "\t" + NL + "\tjavax.jms.Session session_";
  protected final String TEXT_56 = " = connection_";
  protected final String TEXT_57 = ".createSession(";
  protected final String TEXT_58 = ", javax.jms.Session.";
  protected final String TEXT_59 = ");" + NL + "\t" + NL + "\tjavax.jms.Destination des_";
  protected final String TEXT_60 = " = null;" + NL + "\t" + NL + "\t";
  protected final String TEXT_61 = NL + "\tdes_";
  protected final String TEXT_62 = " = session_";
  protected final String TEXT_63 = ".";
  protected final String TEXT_64 = "createTopic";
  protected final String TEXT_65 = "createQueue";
  protected final String TEXT_66 = "(\"";
  protected final String TEXT_67 = "\" + " + NL + "\t\t";
  protected final String TEXT_68 = NL + "\t\tglobalMap.get(\"";
  protected final String TEXT_69 = "_source_struct_name\")" + NL + "\t\t";
  protected final String TEXT_70 = NL + "\t\t";
  protected final String TEXT_71 = NL + "\t\t";
  protected final String TEXT_72 = NL + "\t);" + NL + "\t\t" + NL + "\tjavax.jms.MessageProducer replyProducer_";
  protected final String TEXT_73 = " = session_";
  protected final String TEXT_74 = ".createProducer(null);" + NL + "\treplyProducer_";
  protected final String TEXT_75 = ".setDeliveryMode(javax.jms.DeliveryMode.NON_PERSISTENT);" + NL + "\t\t\t" + NL + "\tjavax.jms.MessageConsumer consumer_";
  protected final String TEXT_76 = " = session_";
  protected final String TEXT_77 = ".createConsumer(des_";
  protected final String TEXT_78 = ");" + NL + "\t" + NL + "\tjavax.jms.Message message_";
  protected final String TEXT_79 = ";" + NL + "\t" + NL + "\t";
  protected final String TEXT_80 = NL + "\tint count_";
  protected final String TEXT_81 = " = 0;" + NL + "\tint max_count_";
  protected final String TEXT_82 = " = ";
  protected final String TEXT_83 = ";" + NL + "\t";
  protected final String TEXT_84 = NL + "\t" + NL + "\t";
  protected final String TEXT_85 = NL + "\tlong max_interval_";
  protected final String TEXT_86 = " = ";
  protected final String TEXT_87 = "*1000;" + NL + "\tlong start_point_";
  protected final String TEXT_88 = " = java.lang.System.currentTimeMillis();" + NL + "\t";
  protected final String TEXT_89 = NL + "\t" + NL + "\twhile (true) {" + NL + "\t\t";
  protected final String TEXT_90 = NL + "\t\tif(count_";
  protected final String TEXT_91 = " == max_count_";
  protected final String TEXT_92 = ") {" + NL + "\t\t\tbreak;" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_93 = NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_94 = NL + "\t\tif((java.lang.System.currentTimeMillis() - start_point_";
  protected final String TEXT_95 = ") > max_interval_";
  protected final String TEXT_96 = ") {" + NL + "\t\t\tbreak;" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_97 = NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_98 = " " + NL + "\t\ttry {" + NL + "\t\t//}" + NL + "\t\t";
  protected final String TEXT_99 = NL + "\t\t" + NL + "\t\t\tmessage_";
  protected final String TEXT_100 = " = consumer_";
  protected final String TEXT_101 = ".receive(" + NL + "\t\t\t\t";
  protected final String TEXT_102 = " ";
  protected final String TEXT_103 = "*1000 ";
  protected final String TEXT_104 = NL + "\t\t\t\t";
  protected final String TEXT_105 = " max_interval_";
  protected final String TEXT_106 = " ";
  protected final String TEXT_107 = NL + "\t\t\t);" + NL + "\t\t\t" + NL + "\t\t\tif(message_";
  protected final String TEXT_108 = " == null) {" + NL + "\t\t\t\tbreak;" + NL + "\t\t\t}" + NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_109 = NL + "\t\t\tcount_";
  protected final String TEXT_110 = "++;" + NL + "\t\t";
  protected final String TEXT_111 = NL + "\t\t\t" + NL + "    //}" + NL;
  protected final String TEXT_112 = NL + NL + "String messageContent_";
  protected final String TEXT_113 = " = ((javax.jms.TextMessage) message_";
  protected final String TEXT_114 = ").getText();" + NL + "if(messageContent_";
  protected final String TEXT_115 = "!=null) {" + NL + "\torg.talend.sap.bw.ISAPBWDataRequest dataRequest_";
  protected final String TEXT_116 = " = new com.google.gson.Gson().fromJson(messageContent_";
  protected final String TEXT_117 = ", org.talend.sap.impl.bw.SAPBWDataRequest.class);" + NL + "\t";
  protected final String TEXT_118 = ".dataSourceName = dataRequest_";
  protected final String TEXT_119 = ".getDataSourceName();" + NL + "\t";
  protected final String TEXT_120 = ".dataSourceType = dataRequest_";
  protected final String TEXT_121 = ".getDataSourceType().toString();" + NL + "\t";
  protected final String TEXT_122 = ".id = dataRequest_";
  protected final String TEXT_123 = ".getId();" + NL + "\t";
  protected final String TEXT_124 = ".packageSize = dataRequest_";
  protected final String TEXT_125 = ".getPackageSize();" + NL + "\t";
  protected final String TEXT_126 = ".sourceSystemName = dataRequest_";
  protected final String TEXT_127 = ".getSourceSystemName();" + NL + "\t";
  protected final String TEXT_128 = ".time = dataRequest_";
  protected final String TEXT_129 = ".getTime();" + NL + "\t";
  protected final String TEXT_130 = ".type = dataRequest_";
  protected final String TEXT_131 = ".getType().toString();" + NL + "\t";
  protected final String TEXT_132 = ".username = dataRequest_";
  protected final String TEXT_133 = ".getUsername();" + NL + "\t";
  protected final String TEXT_134 = ".tid = dataRequest_";
  protected final String TEXT_135 = ".getTID();" + NL + "}" + NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	List<? extends IConnection> outputConnections = node.getOutgoingConnections();
	
	if(outputConnections==null || outputConnections.isEmpty()){
		return "";
	}
	
	boolean linktHMap =  "true".equals(ElementParameterParser.getValue(node, "__LINK_THMAP__"));
	String hid = ElementParameterParser.getValue(node, "__MAPPING__");
	if(linktHMap && (hid == null || "".equals(hid))) {
		return "";
	}
	
	String keyName = null;
	if(node.getUniqueName().startsWith("tSAPDataSource")) {
		keyName = ElementParameterParser.getValue(node, "__DATASOURCE__");
	} else {
		keyName = ElementParameterParser.getValue(node, "__IDOC_TYPE__");
	}
	
	//begin to get mode parameters
	String mode = ElementParameterParser.getValue(node, "__MODE__");
	boolean realTimeMode = "REAL_TIME".equals(mode);
	boolean batchMode = "BATCH".equals(mode);
	
	boolean listenInterval = "true".equals(ElementParameterParser.getValue(node, "__LISTEN_INTERVAL__"));
	boolean listenCount = "true".equals(ElementParameterParser.getValue(node, "__LISTEN_COUNT__"));
	
	String interval = ElementParameterParser.getValue(node, "__INTERVAL__");
	String count = ElementParameterParser.getValue(node, "__COUNT__");
	
	String timeout = ElementParameterParser.getValue(node, "__TIMEOUT__");
	//end to get mode parameters
	
	//begin to get connection parameters
	boolean failover =  "true".equals(ElementParameterParser.getValue(node, "__FAILOVER__"));
	List<Map<String,String>> servers = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__SERVERS__");
	List<Map<String,String>> additionalOpts = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__ADDITIONAL_OPTIONS__");
	
	String host = ElementParameterParser.getValue(node, "__SERVERADDRESS__");
	String port = ElementParameterParser.getValue(node, "__SERVERPORT__");
	String dbuser = ElementParameterParser.getValue(node, "__USER__");
	
	boolean transacted = "true".equals(ElementParameterParser.getValue(node, "__IS_TRANSACTED__"));
	String acknowledgmentMode = ElementParameterParser.getValue(node, "__ACKNOWLEDGMENT_MODE__");
	
	boolean isUseSSL = ("true").equals(ElementParameterParser.getValue(node, "__USE_SSL__"));
	String transProtocol="tcp";
	if(isUseSSL){
		transProtocol = "ssl";
	}
	//end to get connection parameters
	
	String passwordFieldName = "__PASS__";

    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_7);
    } else {
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_10);
    }
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_12);
    if(failover){
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    
		int i=0;
		for(Map<String, String> serversMap : servers) {
			String strHost = serversMap.get("SERVERS_HOST");
			String strPort = serversMap.get("SERVERS_PORT");

			if(i>0) {
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    
			}
			
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(transProtocol);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_23);
    stringBuffer.append(strHost);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(strPort);
    stringBuffer.append(TEXT_29);
    
			i++;
		}
		
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_32);
    
		for(Map<String, String> optrion : additionalOpts) {
		
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(optrion.get("OPTION_NAME"));
    stringBuffer.append(TEXT_36);
    stringBuffer.append(optrion.get("OPTION_VALUE"));
    stringBuffer.append(TEXT_37);
    
		}
	}else{
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(transProtocol);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(host);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(port);
    stringBuffer.append(TEXT_42);
    }
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_45);
    
	if(dbuser == null || ("\"\"").equals(dbuser) || ("").equals(dbuser)) {
	
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_48);
    
	} else {
	
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
	}
	
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(transacted);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(acknowledgmentMode);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_60);
    
	String keyPrefix = ElementParameterParser.getValue(node, "__KEY_PREFIX__");
	
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_63);
    if(realTimeMode) {
    stringBuffer.append(TEXT_64);
    } else {
    stringBuffer.append(TEXT_65);
    }
    stringBuffer.append(TEXT_66);
    stringBuffer.append(keyPrefix);
    stringBuffer.append(TEXT_67);
    if(linktHMap) {
    stringBuffer.append(TEXT_68);
    stringBuffer.append(hid );
    stringBuffer.append(TEXT_69);
    } else {
    stringBuffer.append(TEXT_70);
    stringBuffer.append(keyName);
    stringBuffer.append(TEXT_71);
    }
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    if(realTimeMode && listenCount) {
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(count);
    stringBuffer.append(TEXT_83);
    }
    stringBuffer.append(TEXT_84);
    if(realTimeMode && listenInterval) {
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(interval);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    }
    stringBuffer.append(TEXT_89);
    if(realTimeMode && listenCount) {
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    }
    stringBuffer.append(TEXT_93);
    if(realTimeMode && listenInterval) {
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    }
    stringBuffer.append(TEXT_97);
    if(transacted) {
    stringBuffer.append(TEXT_98);
    }
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_101);
    if(batchMode) {
    stringBuffer.append(TEXT_102);
    stringBuffer.append(timeout);
    stringBuffer.append(TEXT_103);
    }
    stringBuffer.append(TEXT_104);
    if(realTimeMode && listenInterval) {
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    }
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    if(realTimeMode && listenCount) {
		
    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    }
    stringBuffer.append(TEXT_111);
    
	for(IConnection outputConnection : outputConnections) {
    	if(outputConnection.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(outputConnection.getName());
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    
		}
	}

    return stringBuffer.toString();
  }
}
