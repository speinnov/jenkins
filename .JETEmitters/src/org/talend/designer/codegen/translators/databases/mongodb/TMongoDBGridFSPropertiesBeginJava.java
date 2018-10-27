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

public class TMongoDBGridFSPropertiesBeginJava
{
  protected static String nl;
  public static synchronized TMongoDBGridFSPropertiesBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBGridFSPropertiesBeginJava result = new TMongoDBGridFSPropertiesBeginJava();
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
  protected final String TEXT_29 = NL + "    int nb_line_";
  protected final String TEXT_30 = " = 0;";
  protected final String TEXT_31 = NL + "            com.mongodb.Mongo mongo_";
  protected final String TEXT_32 = "=null;" + NL + "            com.mongodb.DB db_";
  protected final String TEXT_33 = "=null;";
  protected final String TEXT_34 = NL + "                mongo_";
  protected final String TEXT_35 = "=(com.mongodb.Mongo)globalMap.get(\"mongo_";
  protected final String TEXT_36 = "\");" + NL + "                db_";
  protected final String TEXT_37 = " = (com.mongodb.DB) globalMap.get(\"db_";
  protected final String TEXT_38 = "\");";
  protected final String TEXT_39 = "  " + NL + "                    log.info(\"";
  protected final String TEXT_40 = " - Get an existing client from \" + \"";
  protected final String TEXT_41 = "\" + \".\");" + NL + "                    log.info(\"";
  protected final String TEXT_42 = " - Get an existing DB from \" + \"";
  protected final String TEXT_43 = "\" + \".\");";
  protected final String TEXT_44 = NL + "            try{ // link with the \"finally\" on the \"finally.javajet\"";
  protected final String TEXT_45 = NL + NL + "                // Empty client options" + NL + "                com.mongodb.MongoClientOptions clientOptions_";
  protected final String TEXT_46 = " = new com.mongodb.MongoClientOptions.Builder().build();";
  protected final String TEXT_47 = NL + "                    clientOptions_";
  protected final String TEXT_48 = " = new com.mongodb.MongoClientOptions.Builder()" + NL + "                    .socketFactory(javax.net.ssl.SSLSocketFactory.getDefault())" + NL + "                    .build();";
  protected final String TEXT_49 = NL + "                // Empty client credentials list" + NL + "                java.util.List<com.mongodb.MongoCredential> mongoCredentialList_";
  protected final String TEXT_50 = " = new java.util.ArrayList<com.mongodb.MongoCredential>();" + NL;
  protected final String TEXT_51 = NL + "                    com.mongodb.MongoCredential mongoCredential_";
  protected final String TEXT_52 = ";";
  protected final String TEXT_53 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_54 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_55 = ");";
  protected final String TEXT_56 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_57 = " = ";
  protected final String TEXT_58 = "; ";
  protected final String TEXT_59 = NL + "                                mongoCredential_";
  protected final String TEXT_60 = " = com.mongodb.MongoCredential.createCredential(";
  protected final String TEXT_61 = ", ";
  protected final String TEXT_62 = ", new String(decryptedPassword_";
  protected final String TEXT_63 = ").toCharArray());";
  protected final String TEXT_64 = NL + "                                mongoCredential_";
  protected final String TEXT_65 = " = com.mongodb.MongoCredential.createMongoCRCredential(";
  protected final String TEXT_66 = ", ";
  protected final String TEXT_67 = ", new String(decryptedPassword_";
  protected final String TEXT_68 = ").toCharArray());";
  protected final String TEXT_69 = NL + "                            mongoCredential_";
  protected final String TEXT_70 = " = com.mongodb.MongoCredential.createPlainCredential(";
  protected final String TEXT_71 = ", \"$external\", new String(decryptedPassword_";
  protected final String TEXT_72 = ").toCharArray());";
  protected final String TEXT_73 = NL + "                            mongoCredential_";
  protected final String TEXT_74 = " = com.mongodb.MongoCredential.createScramSha1Credential(";
  protected final String TEXT_75 = ", ";
  protected final String TEXT_76 = ", new String(decryptedPassword_";
  protected final String TEXT_77 = ").toCharArray());";
  protected final String TEXT_78 = NL + "                        System.setProperty(\"java.security.krb5.realm\", ";
  protected final String TEXT_79 = ");" + NL + "                        System.setProperty(\"java.security.krb5.kdc\", ";
  protected final String TEXT_80 = ");" + NL + "                        System.setProperty(\"javax.security.auth.useSubjectCredsOnly\", \"false\");" + NL + "                        mongoCredential_";
  protected final String TEXT_81 = " = com.mongodb.MongoCredential.createGSSAPICredential(";
  protected final String TEXT_82 = ");";
  protected final String TEXT_83 = NL + "                    mongoCredentialList_";
  protected final String TEXT_84 = ".add(mongoCredential_";
  protected final String TEXT_85 = ");";
  protected final String TEXT_86 = NL + "                        mongo_";
  protected final String TEXT_87 = ".addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);";
  protected final String TEXT_88 = NL + "                    List<com.mongodb.ServerAddress> addrs_";
  protected final String TEXT_89 = " = new java.util.ArrayList<com.mongodb.ServerAddress>();";
  protected final String TEXT_90 = NL + "                        addrs_";
  protected final String TEXT_91 = ".add(new com.mongodb.ServerAddress(";
  protected final String TEXT_92 = ",";
  protected final String TEXT_93 = "));";
  protected final String TEXT_94 = NL + "                    mongo_";
  protected final String TEXT_95 = " = new com.mongodb.MongoClient(addrs_";
  protected final String TEXT_96 = ", mongoCredentialList_";
  protected final String TEXT_97 = ", clientOptions_";
  protected final String TEXT_98 = ");";
  protected final String TEXT_99 = NL + "                    com.mongodb.ServerAddress serverAddress_";
  protected final String TEXT_100 = " = new com.mongodb.ServerAddress(";
  protected final String TEXT_101 = ", ";
  protected final String TEXT_102 = ");" + NL + "                    mongo_";
  protected final String TEXT_103 = " = new com.mongodb.MongoClient(serverAddress_";
  protected final String TEXT_104 = ", mongoCredentialList_";
  protected final String TEXT_105 = ", clientOptions_";
  protected final String TEXT_106 = ");";
  protected final String TEXT_107 = NL + NL + "                db_";
  protected final String TEXT_108 = " = mongo_";
  protected final String TEXT_109 = ".getDB(";
  protected final String TEXT_110 = ");";
  protected final String TEXT_111 = NL + "                        mongo_";
  protected final String TEXT_112 = ".setReadPreference(com.mongodb.ReadPreference.primary());";
  protected final String TEXT_113 = NL + "                        mongo_";
  protected final String TEXT_114 = ".setReadPreference(com.mongodb.ReadPreference.primaryPreferred());";
  protected final String TEXT_115 = NL + "                        mongo_";
  protected final String TEXT_116 = ".setReadPreference(com.mongodb.ReadPreference.secondary());";
  protected final String TEXT_117 = NL + "                        mongo_";
  protected final String TEXT_118 = ".setReadPreference(com.mongodb.ReadPreference.secondaryPreferred());";
  protected final String TEXT_119 = NL + "                        mongo_";
  protected final String TEXT_120 = ".setReadPreference(com.mongodb.ReadPreference.nearest());";
  protected final String TEXT_121 = NL + "                        // Unhandled";
  protected final String TEXT_122 = "  " + NL + "                log.info(\"";
  protected final String TEXT_123 = " - Connecting to \" + mongo_";
  protected final String TEXT_124 = ".getServerAddressList() + \".\");";
  protected final String TEXT_125 = NL + NL;
  protected final String TEXT_126 = NL + "\t\t\tcom.mongodb.gridfs.GridFS gridfs_";
  protected final String TEXT_127 = " = new com.mongodb.gridfs.GridFS(db_";
  protected final String TEXT_128 = ", ";
  protected final String TEXT_129 = ");" + NL + "\t\t\tjava.util.List<com.mongodb.gridfs.GridFSDBFile> gridfsFiles_";
  protected final String TEXT_130 = ";" + NL + "" + NL + "\t\t\t";
  protected final String TEXT_131 = NL + "\t\t\t\tgridfsFiles_";
  protected final String TEXT_132 = " = gridfs_";
  protected final String TEXT_133 = ".find(";
  protected final String TEXT_134 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_135 = NL + "\t\t\t\tcom.mongodb.DBObject gridFSQuery_";
  protected final String TEXT_136 = " = (com.mongodb.DBObject) com.mongodb.util.JSON.parse(";
  protected final String TEXT_137 = ");" + NL + "\t\t\t\tgridfsFiles_";
  protected final String TEXT_138 = " = gridfs_";
  protected final String TEXT_139 = ".find(gridFSQuery_";
  protected final String TEXT_140 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_141 = NL + NL + "\t\t\tfor(com.mongodb.gridfs.GridFSDBFile file : gridfsFiles_";
  protected final String TEXT_142 = "){" + NL + "\t\t\t\tnb_line_";
  protected final String TEXT_143 = "++;" + NL + "\t\t\t\tObject valueObj_";
  protected final String TEXT_144 = "=null;" + NL + "\t\t\t\t";
  protected final String TEXT_145 = NL + "\t\t\t\t\t\tvalueObj_";
  protected final String TEXT_146 = " = file.get(\"";
  protected final String TEXT_147 = "\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_148 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_149 = ".";
  protected final String TEXT_150 = " = valueObj_";
  protected final String TEXT_151 = ";" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_152 = NL + "\t\t\t\t\t\t\tif(valueObj_";
  protected final String TEXT_153 = "!=null && valueObj_";
  protected final String TEXT_154 = ".toString().length() > 0) {" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_155 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_156 = ".";
  protected final String TEXT_157 = " = ParserUtils.parseTo_Date((java.util.Date)valueObj_";
  protected final String TEXT_158 = ", ";
  protected final String TEXT_159 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_160 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_161 = ".";
  protected final String TEXT_162 = " = java.nio.charset.Charset.defaultCharset().decode(java.nio.ByteBuffer.wrap((byte[])valueObj_";
  protected final String TEXT_163 = ")).toString().getBytes(utf8Charset);" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_164 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_165 = ".";
  protected final String TEXT_166 = " = valueObj_";
  protected final String TEXT_167 = ".toString();" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_168 = NL + "\t\t\t\t\t\t\t\t\tif (valueObj_";
  protected final String TEXT_169 = ".getClass().equals(Double.class)) {" + NL + "\t\t                                ";
  protected final String TEXT_170 = ".";
  protected final String TEXT_171 = " = ParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_172 = ".toString()).intValue();" + NL + "\t\t                            } else {" + NL + "\t\t                                ";
  protected final String TEXT_173 = ".";
  protected final String TEXT_174 = " = ParserUtils.parseTo_";
  protected final String TEXT_175 = "(valueObj_";
  protected final String TEXT_176 = ".toString());" + NL + "\t\t                            }" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_177 = NL + "\t\t\t\t\t\t\t\t\tif (valueObj_";
  protected final String TEXT_178 = ".getClass().equals(Double.class)) {" + NL + "\t\t                                ";
  protected final String TEXT_179 = ".";
  protected final String TEXT_180 = " = ParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_181 = ".toString()).longValue();" + NL + "\t\t                            } else {" + NL + "\t\t                                ";
  protected final String TEXT_182 = ".";
  protected final String TEXT_183 = " = ParserUtils.parseTo_";
  protected final String TEXT_184 = "(valueObj_";
  protected final String TEXT_185 = ".toString());" + NL + "\t\t                            }" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_186 = NL + "\t\t\t\t\t\t\t\t\tif (valueObj_";
  protected final String TEXT_187 = ".getClass().equals(Double.class)) {" + NL + "\t\t                                ";
  protected final String TEXT_188 = ".";
  protected final String TEXT_189 = " = ParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_190 = ".toString()).shortValue();" + NL + "\t\t                            } else {" + NL + "\t\t                                ";
  protected final String TEXT_191 = ".";
  protected final String TEXT_192 = " = ParserUtils.parseTo_";
  protected final String TEXT_193 = "(valueObj_";
  protected final String TEXT_194 = ".toString());" + NL + "\t\t                            }" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_195 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_196 = ".";
  protected final String TEXT_197 = " = ParserUtils.parseTo_";
  protected final String TEXT_198 = "(valueObj_";
  protected final String TEXT_199 = ".toString());" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_200 = NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_201 = NL;
  protected final String TEXT_202 = NL;
  protected final String TEXT_203 = NL;

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

    List<IMetadataTable> metadatas = node.getMetadataList();
    List<IMetadataColumn> columnList = null;
    List<? extends IConnection> outputs = node.getOutgoingSortedConnections();
    String firstConnName = "";
    if (outputs.size() > 0){
        IConnection out = outputs.get(0);
        if(out!=null && out.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){
            firstConnName = out.getName();
        }
    }
    
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_30);
    
    if(metadatas != null && metadatas.size() > 0){
        IMetadataTable metadata = metadatas.get(0);
        if(metadata != null){
        
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    
            if (useExistingConnection){
                String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
                
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_38);
    
                if(isLog4jEnabled){
                
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_43);
    
                }
            }else{
            
    stringBuffer.append(TEXT_44);
    
                List<Map<String,String>> replicaAddrs= (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__REPLICA_SET__");

                boolean useSSL = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_SSL__"));
                String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
                
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    
                if (useSSL) {
                    
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    
                }
                
                // Client Credentials
                
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    
                // Authentication
                if (authentication){
                    
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    
                    if((authenticationMechanism.equals("NEGOTIATE_MEC"))||(authenticationMechanism.equals("PLAIN_MEC"))||(authenticationMechanism.equals("SCRAMSHA1_MEC"))){
                        String passwordFieldName = "__PASSWORD__";
                        
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_55);
    } else {
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_58);
    }
    
                        if(authenticationMechanism.equals("NEGOTIATE_MEC")){
                            if(dbversion.equals("MONGODB_3_0_X") || dbversion.equals("MONGODB_3_2_X")) {

    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
                        
                            } else {

    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    
                            }
                        } else if(authenticationMechanism.equals("PLAIN_MEC")){
                            
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    
                        } else if(authenticationMechanism.equals("SCRAMSHA1_MEC")){
                            
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    
                        }
                    } else { // GSSAPI SASL (KERBEROS)
                        
    stringBuffer.append(TEXT_78);
    stringBuffer.append(krbRealm);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(krbKdc);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(krbUserPrincipal);
    stringBuffer.append(TEXT_82);
    
                    }
                    
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    
                }

                if(queryOptionNoTimeOut){
                    
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    
                }

                // the client
                if(useReplicaSet){
                    
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    
                    for(Map<String,String> replicaAddr:replicaAddrs){
                    
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(replicaAddr.get("REPLICA_HOST"));
    stringBuffer.append(TEXT_92);
    stringBuffer.append(replicaAddr.get("REPLICA_PORT"));
    stringBuffer.append(TEXT_93);
    
                    }
                    
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_98);
    
                }else{
                    
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    
                }
                
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_110);
    
            }

            if (setReadPreference) {
                    if (readPreference.equals("PRIMARY")) {
                        
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    
                    } else if (readPreference.equals("PRIMARY_PREFERRED")) {
                        
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    
                    } else if (readPreference.equals("SECONDARY")) {
                        
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    
                    } else if (readPreference.equals("SECONDARY_PREFERRED")) {
                        
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    
                    } else if (readPreference.equals("NEAREST")) {
                        
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    
                    } else {
                        
    stringBuffer.append(TEXT_121);
    
                    }
            }

            if(isLog4jEnabled){
                
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    
            }
            log4jFileUtil.startRetriveDataInfo();

    //} Missing }; must be added by importing component
//} Missing } ; must be added by importing component

    stringBuffer.append(TEXT_125);
    
	boolean givenStringFileName = "STRING_FILENAME".equalsIgnoreCase(ElementParameterParser.getValue(node, "__QUERY_TYPE__"));
    String stringFileName = ElementParameterParser.getValue(node, "__FILENAME__");
    String query = ElementParameterParser.getValue(node,"__QUERY__");
    query = query.replaceAll("\n","");
    query = query.replaceAll("\r","");
    String bucketName = ElementParameterParser.getValue(node, "__BUCKET__");

    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(bucketName);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    
			// FIND files with given filename
			if(givenStringFileName){
				
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(stringFileName);
    stringBuffer.append(TEXT_134);
    
			}
			// FIND files from a given Mongo Query 
			else {
				
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    
			}
			
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    
				if(firstConnName.length()>0){
					columnList = metadata.getListColumns();
					int sizeColumns = columnList.size();
					for (int i = 0; i < sizeColumns; i++) {
						IMetadataColumn column = columnList.get(i);
						String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
						JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
						String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
						
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_147);
    

						if(javaType == JavaTypesManager.OBJECT) {
							
    stringBuffer.append(TEXT_148);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_149);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    
						} else {
							
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    
								if(javaType == JavaTypesManager.DATE) {
									
    stringBuffer.append(TEXT_155);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_156);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_159);
    
								} else if(javaType == JavaTypesManager.BYTE_ARRAY) {
									
    stringBuffer.append(TEXT_160);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_161);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    
								} else if(javaType == JavaTypesManager.STRING){
									
    stringBuffer.append(TEXT_164);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_165);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_167);
    
								} else if(javaType == JavaTypesManager.INTEGER){
									
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_170);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_173);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_174);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_176);
    
								} else if(javaType == JavaTypesManager.LONG){
									
    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_179);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_182);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_183);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    
								} else if(javaType == JavaTypesManager.SHORT){
									
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_188);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_191);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_192);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    
								} else {
									
    stringBuffer.append(TEXT_195);
    stringBuffer.append(firstConnName );
    stringBuffer.append(TEXT_196);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_197);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    
								}
								
    stringBuffer.append(TEXT_200);
    
						}


					}
				}
				
    stringBuffer.append(TEXT_201);
    
		} // Close first { from GetMongoDBConnectionWithMainFlow.javajet
	} // Close second { from GetMongoDBConnectionWithMainFlow.javajet

    stringBuffer.append(TEXT_202);
    stringBuffer.append(TEXT_203);
    return stringBuffer.toString();
  }
}
