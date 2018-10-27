package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;
import java.util.Map;

public class TMongoDBGridFSListBeginJava
{
  protected static String nl;
  public static synchronized TMongoDBGridFSListBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBGridFSListBeginJava result = new TMongoDBGridFSListBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t\tlog.debug(\"";
  protected final String TEXT_2 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_3 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_4 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_5 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_7 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_8 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_10 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_11 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_13 = " - Written records count: \" + nb_line_";
  protected final String TEXT_14 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_16 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_17 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_18 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_19 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_20 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_21 = " - Writing the record \" + nb_line_";
  protected final String TEXT_22 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_24 = " - Processing the record \" + nb_line_";
  protected final String TEXT_25 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_26 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_27 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_28 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_29 = NL + "            int nb_line_";
  protected final String TEXT_30 = " = 0;" + NL + "\t\t\tcom.mongodb.Mongo mongo_";
  protected final String TEXT_31 = "=null;" + NL + "\t\t\tcom.mongodb.DB db_";
  protected final String TEXT_32 = "=null;" + NL + "\t\t\t";
  protected final String TEXT_33 = NL + "\t\t\t\tmongo_";
  protected final String TEXT_34 = "=(com.mongodb.Mongo)globalMap.get(\"mongo_";
  protected final String TEXT_35 = "\");" + NL + "\t\t\t\tdb_";
  protected final String TEXT_36 = " = (com.mongodb.DB) globalMap.get(\"db_";
  protected final String TEXT_37 = "\");" + NL + "\t\t\t\t";
  protected final String TEXT_38 = "\t" + NL + "\t\t\t\t\tlog.info(\"";
  protected final String TEXT_39 = " - Get an existing client from \" + \"";
  protected final String TEXT_40 = "\" + \".\");" + NL + "\t\t\t\t\tlog.info(\"";
  protected final String TEXT_41 = " - Get an existing DB from \" + \"";
  protected final String TEXT_42 = "\" + \".\");" + NL + "\t\t\t\t";
  protected final String TEXT_43 = NL + "\t\t\ttry{ // link with the \"finally\" on the \"finally.javajet\"" + NL + "\t\t\t\t";
  protected final String TEXT_44 = NL + NL + "                // Empty client options" + NL + "                com.mongodb.MongoClientOptions clientOptions_";
  protected final String TEXT_45 = " = new com.mongodb.MongoClientOptions.Builder().build();";
  protected final String TEXT_46 = NL + "                    clientOptions_";
  protected final String TEXT_47 = " = new com.mongodb.MongoClientOptions.Builder()" + NL + "                    .socketFactory(javax.net.ssl.SSLSocketFactory.getDefault())" + NL + "                    .build();";
  protected final String TEXT_48 = NL + "                // Empty client credentials list" + NL + "                java.util.List<com.mongodb.MongoCredential> mongoCredentialList_";
  protected final String TEXT_49 = " = new java.util.ArrayList<com.mongodb.MongoCredential>();" + NL;
  protected final String TEXT_50 = NL + "                    com.mongodb.MongoCredential mongoCredential_";
  protected final String TEXT_51 = ";";
  protected final String TEXT_52 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_53 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_54 = ");";
  protected final String TEXT_55 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_56 = " = ";
  protected final String TEXT_57 = "; ";
  protected final String TEXT_58 = NL + "                    \t\t\tmongoCredential_";
  protected final String TEXT_59 = " = com.mongodb.MongoCredential.createCredential(";
  protected final String TEXT_60 = ", ";
  protected final String TEXT_61 = ", new String(decryptedPassword_";
  protected final String TEXT_62 = ").toCharArray());";
  protected final String TEXT_63 = NL + "                    \t\t\tmongoCredential_";
  protected final String TEXT_64 = " = com.mongodb.MongoCredential.createMongoCRCredential(";
  protected final String TEXT_65 = ", ";
  protected final String TEXT_66 = ", new String(decryptedPassword_";
  protected final String TEXT_67 = ").toCharArray());";
  protected final String TEXT_68 = NL + "                            mongoCredential_";
  protected final String TEXT_69 = " = com.mongodb.MongoCredential.createPlainCredential(";
  protected final String TEXT_70 = ", \"$external\", new String(decryptedPassword_";
  protected final String TEXT_71 = ").toCharArray());";
  protected final String TEXT_72 = NL + "                            mongoCredential_";
  protected final String TEXT_73 = " = com.mongodb.MongoCredential.createScramSha1Credential(";
  protected final String TEXT_74 = ", ";
  protected final String TEXT_75 = ", new String(decryptedPassword_";
  protected final String TEXT_76 = ").toCharArray());";
  protected final String TEXT_77 = NL + "                        System.setProperty(\"java.security.krb5.realm\", ";
  protected final String TEXT_78 = ");" + NL + "                        System.setProperty(\"java.security.krb5.kdc\", ";
  protected final String TEXT_79 = ");" + NL + "                        System.setProperty(\"javax.security.auth.useSubjectCredsOnly\", \"false\");" + NL + "                        mongoCredential_";
  protected final String TEXT_80 = " = com.mongodb.MongoCredential.createGSSAPICredential(";
  protected final String TEXT_81 = ");";
  protected final String TEXT_82 = NL + "                    mongoCredentialList_";
  protected final String TEXT_83 = ".add(mongoCredential_";
  protected final String TEXT_84 = ");";
  protected final String TEXT_85 = NL + "\t\t\t\t\tList<com.mongodb.ServerAddress> addrs_";
  protected final String TEXT_86 = " = new java.util.ArrayList<com.mongodb.ServerAddress>();" + NL + "\t\t\t\t\t";
  protected final String TEXT_87 = NL + "\t\t\t\t\t\taddrs_";
  protected final String TEXT_88 = ".add(new com.mongodb.ServerAddress(";
  protected final String TEXT_89 = ",";
  protected final String TEXT_90 = "));" + NL + "\t\t\t\t\t";
  protected final String TEXT_91 = NL + "                    mongo_";
  protected final String TEXT_92 = " = new com.mongodb.MongoClient(addrs_";
  protected final String TEXT_93 = ", mongoCredentialList_";
  protected final String TEXT_94 = ", clientOptions_";
  protected final String TEXT_95 = ");";
  protected final String TEXT_96 = NL + "                    com.mongodb.ServerAddress serverAddress_";
  protected final String TEXT_97 = " = new com.mongodb.ServerAddress(";
  protected final String TEXT_98 = ", ";
  protected final String TEXT_99 = ");" + NL + "                    mongo_";
  protected final String TEXT_100 = " = new com.mongodb.MongoClient(serverAddress_";
  protected final String TEXT_101 = ", mongoCredentialList_";
  protected final String TEXT_102 = ", clientOptions_";
  protected final String TEXT_103 = ");";
  protected final String TEXT_104 = NL + "                        mongo_";
  protected final String TEXT_105 = ".addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);";
  protected final String TEXT_106 = NL + NL + "\t\t\t\tdb_";
  protected final String TEXT_107 = " = mongo_";
  protected final String TEXT_108 = ".getDB(";
  protected final String TEXT_109 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_110 = NL + "\t\t\t            mongo_";
  protected final String TEXT_111 = ".setReadPreference(com.mongodb.ReadPreference.primary());" + NL + "\t\t\t            ";
  protected final String TEXT_112 = NL + "\t\t\t            mongo_";
  protected final String TEXT_113 = ".setReadPreference(com.mongodb.ReadPreference.primaryPreferred());" + NL + "\t\t\t            ";
  protected final String TEXT_114 = NL + "\t\t\t            mongo_";
  protected final String TEXT_115 = ".setReadPreference(com.mongodb.ReadPreference.secondary());" + NL + "\t\t\t            ";
  protected final String TEXT_116 = NL + "\t\t\t            mongo_";
  protected final String TEXT_117 = ".setReadPreference(com.mongodb.ReadPreference.secondaryPreferred());" + NL + "\t\t\t            ";
  protected final String TEXT_118 = NL + "\t\t\t            mongo_";
  protected final String TEXT_119 = ".setReadPreference(com.mongodb.ReadPreference.nearest());" + NL + "\t\t\t            ";
  protected final String TEXT_120 = NL + "\t\t\t            // Unhandled" + NL + "\t\t\t            ";
  protected final String TEXT_121 = "\t" + NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_122 = " - Connecting to \" + mongo_";
  protected final String TEXT_123 = ".getServerAddressList() + \".\");" + NL + "\t\t\t\t";
  protected final String TEXT_124 = NL + "\t\t\tcom.mongodb.gridfs.GridFS gridfs_";
  protected final String TEXT_125 = " = new com.mongodb.gridfs.GridFS(db_";
  protected final String TEXT_126 = ", ";
  protected final String TEXT_127 = ");" + NL + "\t\t\tcom.mongodb.DBObject gridFSQuery_";
  protected final String TEXT_128 = " = (com.mongodb.DBObject) com.mongodb.util.JSON.parse(";
  protected final String TEXT_129 = ");" + NL + "\t\t\tcom.mongodb.DBCursor filesCursor_";
  protected final String TEXT_130 = " = gridfs_";
  protected final String TEXT_131 = ".getFileList(gridFSQuery_";
  protected final String TEXT_132 = ");" + NL + "" + NL + "\t\t\t";
  protected final String TEXT_133 = NL + "\t\t\tcom.mongodb.DBObject sortField_";
  protected final String TEXT_134 = " = new com.mongodb.BasicDBObject();" + NL + "\t\t\tsortField_";
  protected final String TEXT_135 = ".put(\"";
  protected final String TEXT_136 = "\", ";
  protected final String TEXT_137 = ");" + NL + "\t\t\tfilesCursor_";
  protected final String TEXT_138 = " = filesCursor_";
  protected final String TEXT_139 = ".sort(sortField_";
  protected final String TEXT_140 = ");" + NL + "" + NL + "\t\t\twhile(filesCursor_";
  protected final String TEXT_141 = ".hasNext()){" + NL + "\t\t\t\tcom.mongodb.DBObject file_";
  protected final String TEXT_142 = " = filesCursor_";
  protected final String TEXT_143 = ".next();" + NL + "\t\t\t\tnb_line_";
  protected final String TEXT_144 = "++;";
  protected final String TEXT_145 = NL + "\t\t\t\t\tlog.info(\"";
  protected final String TEXT_146 = " - Current file \" + file_";
  protected final String TEXT_147 = ".get(\"filename\"));";
  protected final String TEXT_148 = NL + "\t\t\t\tglobalMap.put(\"";
  protected final String TEXT_149 = "_CURRENT_FILEID\", (String) file_";
  protected final String TEXT_150 = ".get(\"_id\").toString() );" + NL + "\t\t\t\tglobalMap.put(\"";
  protected final String TEXT_151 = "_CURRENT_FILENAME\", (String) file_";
  protected final String TEXT_152 = ".get(\"filename\"));" + NL + "\t\t\t\tglobalMap.put(\"";
  protected final String TEXT_153 = "_CURRENT_FILEUPLOADDATE\", (java.util.Date) file_";
  protected final String TEXT_154 = ".get(\"uploadDate\"));" + NL + "\t\t\t\tglobalMap.put(\"";
  protected final String TEXT_155 = "_CURRENT_FILELENGTH\", (Long) file_";
  protected final String TEXT_156 = ".get(\"length\"));" + NL + "\t\t\t\tglobalMap.put(\"";
  protected final String TEXT_157 = "_CURRENT_FILEMD5\", (String) file_";
  protected final String TEXT_158 = ".get(\"md5\"));";
  protected final String TEXT_159 = NL;
  protected final String TEXT_160 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
	//this util class use by set log4j debug paramters
	class DefaultLog4jFileUtil {
	
		INode node = null;
	    String cid = null;
 		boolean isLog4jEnabled = false;
 		String label = null;
 		
 		public DefaultLog4jFileUtil(){
 		}
 		public DefaultLog4jFileUtil(INode node) {
 			this.node = node;
 			this.cid = node.getUniqueName();
 			this.label = cid;
			this.isLog4jEnabled = ("true").equals(org.talend.core.model.process.ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
 		}
 		
 		public void setCid(String cid) {
 			this.cid = cid;
 		}
 		
		//for all tFileinput* components 
		public void startRetriveDataInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_1);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_2);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_3);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_6);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_9);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_12);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_17);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_19);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_20);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_23);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_26);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    
			}
		}
	}
	
	final DefaultLog4jFileUtil log4jFileUtil = new DefaultLog4jFileUtil((INode)(((org.talend.designer.codegen.config.CodeGeneratorArgument)argument).getArgument()));
	
    
  	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
  	INode node = (INode)codeGenArgument.getArgument();
  	String cid = node.getUniqueName();

    String dbhost   = ElementParameterParser.getValue(node, "__HOST__");
    String dbname   = ElementParameterParser.getValue(node, "__DATABASE__");
    String dbuser   = ElementParameterParser.getValue(node, "__USERNAME__");
    boolean authentication="true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REQUIRED_AUTHENTICATION__"));
    String authenticationMechanism = ElementParameterParser.getValue(node, "__AUTHENTICATION_MECHANISM__");
    String krbUserPrincipal = ElementParameterParser.getValue(node, "__KRB_USER_PRINCIPAL__");
    String krbRealm = ElementParameterParser.getValue(node, "__KRB_REALM__");
    String krbKdc = ElementParameterParser.getValue(node, "__KRB_KDC__");
    String dbpass   = ElementParameterParser.getValue(node, "__PASSWORD__");
    String mongoPort = ElementParameterParser.getValue(node, "__PORT__");
    String dbport = mongoPort.startsWith("context.") ? "Integer.valueOf(" + mongoPort + ").intValue()" : mongoPort.replace("\"", "");
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
    String collection = ElementParameterParser.getValue(node,"__COLLECTION__");
    
  	String limit = ElementParameterParser.getValue(node,"__LIMIT__");

  	List<Map<String, String>> sorts = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__SORT__");
  	boolean useReplicaSet="true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_REPLICA_SET__"));
  	boolean setReadPreference = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_READ_PREFERENCE__"));
    String readPreference = ElementParameterParser.getValue(node, "__READ_PREFERENCE__");
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
    boolean useAuthDB = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_AUTHENTICATION_DATABASE__"));
    String authDB = ElementParameterParser.getValue(node, "__AUTHENTICATION_DATABASE__");
    String usedAuthenticationDB = useAuthDB ? authDB : dbname;
    boolean queryOptionNoTimeOut = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__QUERYOPTION_NOTIMEOUT__"));
	
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
      		if (useExistingConnection){
      			String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
				
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_37);
    
				if(isLog4jEnabled){
				
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_42);
    
				}
			}else{
			
    stringBuffer.append(TEXT_43);
    
	 			List<Map<String,String>> replicaAddrs= (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__REPLICA_SET__");

                boolean useSSL = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_SSL__"));
                String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
                
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    
                if (useSSL) {
                    
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    
                }
                
                // Client Credentials
                
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    
                // Authentication
                if (authentication){
                    
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    
                    if((authenticationMechanism.equals("NEGOTIATE_MEC"))||(authenticationMechanism.equals("PLAIN_MEC"))||(authenticationMechanism.equals("SCRAMSHA1_MEC"))){
                        String passwordFieldName = "__PASSWORD__";
                        
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_54);
    } else {
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_57);
    }
    
                        if(authenticationMechanism.equals("NEGOTIATE_MEC")){
                			if(dbversion.equals("MONGODB_3_0_X") || dbversion.equals("MONGODB_3_2_X")) {

    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
                        
                			} else {

    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    
                			}
                        } else if(authenticationMechanism.equals("PLAIN_MEC")){
                            
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    
                        } else if(authenticationMechanism.equals("SCRAMSHA1_MEC")){
                            
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    
                        }
                    } else { // GSSAPI SASL (KERBEROS)
                        
    stringBuffer.append(TEXT_77);
    stringBuffer.append(krbRealm);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(krbKdc);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(krbUserPrincipal);
    stringBuffer.append(TEXT_81);
    
                    }
                    
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    
                }

                // the client
				if(useReplicaSet){
				    
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    
					for(Map<String,String> replicaAddr:replicaAddrs){
					
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(replicaAddr.get("REPLICA_HOST"));
    stringBuffer.append(TEXT_89);
    stringBuffer.append(replicaAddr.get("REPLICA_PORT"));
    stringBuffer.append(TEXT_90);
    
					}
                    
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    
				}else{
                    
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    
				}

                if(queryOptionNoTimeOut){
                    
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    
                }

                
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_109);
    
			}

			if (setReadPreference) {
			        if (readPreference.equals("PRIMARY")) {
			            
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    
			        } else if (readPreference.equals("PRIMARY_PREFERRED")) {
			            
    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    
			        } else if (readPreference.equals("SECONDARY")) {
			            
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    
			        } else if (readPreference.equals("SECONDARY_PREFERRED")) {
			            
    stringBuffer.append(TEXT_116);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_117);
    
			        } else if (readPreference.equals("NEAREST")) {
			            
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    
			        } else {
			            
    stringBuffer.append(TEXT_120);
    
			        }
			}

			if(isLog4jEnabled){
			    
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    
			}
			log4jFileUtil.startRetriveDataInfo();

    
    boolean givenStringFileName = "STRING_FILENAME".equalsIgnoreCase(ElementParameterParser.getValue(node, "__QUERY_TYPE__"));
    String stringFileName = ElementParameterParser.getValue(node, "__FILENAME__");
    String query = ElementParameterParser.getValue(node,"__QUERY__");
    query = query.replaceAll("\n","");
    query = query.replaceAll("\r","");
    String bucketName = ElementParameterParser.getValue(node, "__BUCKET__");
    String sortBy = ElementParameterParser.getValue(node, "__SORTBY__");
    String sortOrder = ElementParameterParser.getValue(node, "__SORTORDER__");

    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(bucketName);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    
			// SORT CURSOR
			
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(sortBy);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(sortOrder);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_144);
    
				if(isLog4jEnabled){

    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    
				}

    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    

    stringBuffer.append(TEXT_159);
    stringBuffer.append(TEXT_160);
    return stringBuffer.toString();
  }
}
