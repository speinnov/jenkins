package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;

public class TMongoDBOutputBeginJava
{
  protected static String nl;
  public static synchronized TMongoDBOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBOutputBeginJava result = new TMongoDBOutputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\tint nb_line_";
  protected final String TEXT_2 = " = 0;" + NL + "\t";
  protected final String TEXT_3 = NL + "\t\t\tclass DBObjectUtil_";
  protected final String TEXT_4 = " {" + NL + "\t\t\t" + NL + "\t\t\t\tprivate com.mongodb.BasicDBObject object = null;" + NL + "\t\t\t\t//Put value to embedded document" + NL + "\t\t\t\t//If have no embedded document, put the value to root document" + NL + "\t\t\t\tpublic void put(String parentNode, String curentName, Object value) {" + NL + "\t\t\t\t\tif (parentNode == null || \"\".equals(parentNode)) {" + NL + "\t\t\t\t\t\tobject.put(curentName, value);" + NL + "\t\t\t\t\t} else {" + NL + "\t\t\t\t\t\tString objNames[]= parentNode.split(\"\\\\.\");" + NL + "\t\t\t\t\t\tcom.mongodb.BasicDBObject lastNode = getParentNode(parentNode, objNames.length-1);" + NL + "\t\t\t\t\t\tlastNode.put(curentName, value);" + NL + "\t\t\t\t\t\tcom.mongodb.BasicDBObject parenttNode = null;" + NL + "\t\t\t\t\t\tfor (int i = objNames.length - 1; i >=0; i--) {" + NL + "\t\t\t\t\t\t\tparenttNode=getParentNode(parentNode, i-1);" + NL + "\t\t\t\t\t\t\tparenttNode.put(objNames[i], lastNode);" + NL + "\t\t\t\t\t\t\tlastNode=(com.mongodb.BasicDBObject)parenttNode.clone();" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\tobject=lastNode;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t\t//Get node(embedded document) by path configuration" + NL + "\t\t\t\tpublic com.mongodb.BasicDBObject getParentNode(String parentNode, int index) {" + NL + "\t\t\t\t\tcom.mongodb.BasicDBObject basicDBObject = object;" + NL + "\t\t\t\t\tif (parentNode == null || \"\".equals(parentNode)) {" + NL + "\t\t\t\t\t\treturn object;" + NL + "\t\t\t\t\t} else {" + NL + "\t\t\t\t\t\tString objNames[] = parentNode.split(\"\\\\.\");" + NL + "\t\t\t\t\t\tfor (int i = 0; i <= index; i++) {" + NL + "\t\t\t\t\t\t\tbasicDBObject = (com.mongodb.BasicDBObject) basicDBObject" + NL + "\t\t\t\t\t\t\t\t\t.get(objNames[i]);" + NL + "\t\t\t\t\t\t\tif (basicDBObject == null) {" + NL + "\t\t\t\t\t\t\t\tbasicDBObject = new com.mongodb.BasicDBObject();" + NL + "\t\t\t\t\t\t\t\treturn basicDBObject;" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\tif (i == index) {" + NL + "\t\t\t\t\t\t\t\tbreak;" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\treturn basicDBObject;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void putkeyNode(String parentNode, String curentName, Object value){" + NL + "\t\t\t\t\tif (parentNode == null || \"\".equals(parentNode) || \".\".equals(parentNode)) {" + NL + "\t\t\t\t\t\tput(parentNode, curentName, value);" + NL + "\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\tput(\"\", parentNode+\".\"+curentName, value);" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t" + NL + "\t\t\t\tpublic com.mongodb.BasicDBObject getObject() {" + NL + "\t\t\t\t\treturn this.object;" + NL + "\t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void setObject(com.mongodb.BasicDBObject object){" + NL + "\t\t\t\t\tthis.object=object;" + NL + "\t\t\t\t}" + NL + "\t\t\t" + NL + "\t\t\t}" + NL + "" + NL + "\t\t\tDBObjectUtil_";
  protected final String TEXT_5 = " updateObjectUtil_";
  protected final String TEXT_6 = "=new DBObjectUtil_";
  protected final String TEXT_7 = "();" + NL + "\t\t\tDBObjectUtil_";
  protected final String TEXT_8 = " queryObjectUtil_";
  protected final String TEXT_9 = "=new DBObjectUtil_";
  protected final String TEXT_10 = "();" + NL + "\t\t\tjava.util.Map<String, String> pathMap_";
  protected final String TEXT_11 = "=new java.util.HashMap<String, String>();" + NL + "\t\t\tcom.mongodb.Mongo mongo_";
  protected final String TEXT_12 = "=null;" + NL + "\t\t\tcom.mongodb.DB db_";
  protected final String TEXT_13 = " =null;" + NL + "\t\t\t";
  protected final String TEXT_14 = NL + "\t\t\t\tpathMap_";
  protected final String TEXT_15 = ".put(\"";
  protected final String TEXT_16 = "\",";
  protected final String TEXT_17 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\t\tmongo_";
  protected final String TEXT_19 = "=(com.mongodb.Mongo)globalMap.get(\"mongo_";
  protected final String TEXT_20 = "\");" + NL + "\t\t\t\tdb_";
  protected final String TEXT_21 = " = (com.mongodb.DB) globalMap.get(\"db_";
  protected final String TEXT_22 = "\");" + NL + "\t\t\t\t";
  protected final String TEXT_23 = "\t" + NL + "\t\t\t\t\tlog.info(\"";
  protected final String TEXT_24 = " - Get an existing client from \" + \"";
  protected final String TEXT_25 = "\" + \".\");" + NL + "\t\t\t\t\tlog.info(\"";
  protected final String TEXT_26 = " - Get an existing DB from \" + \"";
  protected final String TEXT_27 = "\" + \".\");" + NL + "\t\t\t\t";
  protected final String TEXT_28 = NL + "\t \t\t    // Empty client options" + NL + "                com.mongodb.MongoClientOptions clientOptions_";
  protected final String TEXT_29 = " = new com.mongodb.MongoClientOptions.Builder().build();";
  protected final String TEXT_30 = NL + "                    clientOptions_";
  protected final String TEXT_31 = " = new com.mongodb.MongoClientOptions.Builder()" + NL + "                    .socketFactory(javax.net.ssl.SSLSocketFactory.getDefault())" + NL + "                    .build();";
  protected final String TEXT_32 = NL + "                // Empty client credentials list" + NL + "                java.util.List<com.mongodb.MongoCredential> mongoCredentialList_";
  protected final String TEXT_33 = " = new java.util.ArrayList<com.mongodb.MongoCredential>();" + NL;
  protected final String TEXT_34 = NL + "                    com.mongodb.MongoCredential mongoCredential_";
  protected final String TEXT_35 = ";";
  protected final String TEXT_36 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_37 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_38 = ");";
  protected final String TEXT_39 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_40 = " = ";
  protected final String TEXT_41 = "; ";
  protected final String TEXT_42 = NL + "                    \t\t\tmongoCredential_";
  protected final String TEXT_43 = " = com.mongodb.MongoCredential.createCredential(";
  protected final String TEXT_44 = ", ";
  protected final String TEXT_45 = ", new String(decryptedPassword_";
  protected final String TEXT_46 = ").toCharArray());";
  protected final String TEXT_47 = NL + "                    \t\t\tmongoCredential_";
  protected final String TEXT_48 = " = com.mongodb.MongoCredential.createMongoCRCredential(";
  protected final String TEXT_49 = ", ";
  protected final String TEXT_50 = ", new String(decryptedPassword_";
  protected final String TEXT_51 = ").toCharArray());";
  protected final String TEXT_52 = NL + "                            mongoCredential_";
  protected final String TEXT_53 = " = com.mongodb.MongoCredential.createPlainCredential(";
  protected final String TEXT_54 = ", \"$external\", new String(decryptedPassword_";
  protected final String TEXT_55 = ").toCharArray());";
  protected final String TEXT_56 = NL + "                            mongoCredential_";
  protected final String TEXT_57 = " = com.mongodb.MongoCredential.createScramSha1Credential(";
  protected final String TEXT_58 = ", ";
  protected final String TEXT_59 = ", new String(decryptedPassword_";
  protected final String TEXT_60 = ").toCharArray());";
  protected final String TEXT_61 = NL + "                        System.setProperty(\"java.security.krb5.realm\", ";
  protected final String TEXT_62 = ");" + NL + "                        System.setProperty(\"java.security.krb5.kdc\", ";
  protected final String TEXT_63 = ");" + NL + "                        System.setProperty(\"javax.security.auth.useSubjectCredsOnly\", \"false\");" + NL + "                        mongoCredential_";
  protected final String TEXT_64 = " = com.mongodb.MongoCredential.createGSSAPICredential(";
  protected final String TEXT_65 = ");";
  protected final String TEXT_66 = NL + "                    mongoCredentialList_";
  protected final String TEXT_67 = ".add(mongoCredential_";
  protected final String TEXT_68 = ");";
  protected final String TEXT_69 = NL + "                    List<com.mongodb.ServerAddress> addrs_";
  protected final String TEXT_70 = " = new java.util.ArrayList<com.mongodb.ServerAddress>();";
  protected final String TEXT_71 = NL + "                        addrs_";
  protected final String TEXT_72 = ".add(new com.mongodb.ServerAddress(";
  protected final String TEXT_73 = ",";
  protected final String TEXT_74 = "));";
  protected final String TEXT_75 = NL + "                    mongo_";
  protected final String TEXT_76 = " = new ";
  protected final String TEXT_77 = "(addrs_";
  protected final String TEXT_78 = ", mongoCredentialList_";
  protected final String TEXT_79 = ", clientOptions_";
  protected final String TEXT_80 = ");";
  protected final String TEXT_81 = NL + "                    com.mongodb.ServerAddress serverAddress_";
  protected final String TEXT_82 = " = new com.mongodb.ServerAddress(";
  protected final String TEXT_83 = ", ";
  protected final String TEXT_84 = ");" + NL + "                    mongo_";
  protected final String TEXT_85 = " = new ";
  protected final String TEXT_86 = "(serverAddress_";
  protected final String TEXT_87 = ", mongoCredentialList_";
  protected final String TEXT_88 = ", clientOptions_";
  protected final String TEXT_89 = ");";
  protected final String TEXT_90 = NL + "\t\t\t\tresourceMap.put(\"mongo_";
  protected final String TEXT_91 = "\", mongo_";
  protected final String TEXT_92 = ");" + NL + "\t\t\t\tdb_";
  protected final String TEXT_93 = " = mongo_";
  protected final String TEXT_94 = ".getDB(";
  protected final String TEXT_95 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_96 = NL + "\t\t\t\tmongo_";
  protected final String TEXT_97 = ".setWriteConcern(com.mongodb.WriteConcern.";
  protected final String TEXT_98 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_99 = NL + "\t\t        mongo_";
  protected final String TEXT_100 = ".addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);" + NL + "\t\t        ";
  protected final String TEXT_101 = NL + "\t\t\t";
  protected final String TEXT_102 = "\t" + NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_103 = " - Connecting to \" + mongo_";
  protected final String TEXT_104 = ".getServerAddressList() + \".\");" + NL + "\t\t\t";
  protected final String TEXT_105 = NL + "\t\t\t\tif(db_";
  protected final String TEXT_106 = ".collectionExists(";
  protected final String TEXT_107 = ")){" + NL + "\t\t\t\t\tdb_";
  protected final String TEXT_108 = ".getCollection(";
  protected final String TEXT_109 = ").drop();" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_110 = NL + "\t\t\tcom.mongodb.DBCollection coll_";
  protected final String TEXT_111 = " = db_";
  protected final String TEXT_112 = ".getCollection(";
  protected final String TEXT_113 = ");" + NL + "\t\t\t";
  protected final String TEXT_114 = NL + "\t\t\t\tcom.mongodb.BulkWriteOperation bulkWriteOperation_";
  protected final String TEXT_115 = " = coll_";
  protected final String TEXT_116 = ".initialize";
  protected final String TEXT_117 = "BulkOperation();" + NL + "\t\t\t\tint bulkWriteOperationCounter_";
  protected final String TEXT_118 = " = 1;" + NL + "\t\t\t\tint bulkWriteOperationSize_";
  protected final String TEXT_119 = " = Integer.parseInt(";
  protected final String TEXT_120 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_121 = NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_122 = " - Start to write data into database [\" + db_";
  protected final String TEXT_123 = ".getName() + \"].\");" + NL + "\t\t\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
  	String cid = node.getUniqueName();

    String dbhost   = ElementParameterParser.getValue(node, "__HOST__");
    String dbname   = ElementParameterParser.getValue(node, "__DATABASE__");
    boolean authentication="true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REQUIRED_AUTHENTICATION__"));
    String authenticationMechanism = ElementParameterParser.getValue(node, "__AUTHENTICATION_MECHANISM__");
    String krbUserPrincipal = ElementParameterParser.getValue(node, "__KRB_USER_PRINCIPAL__");
    String krbRealm = ElementParameterParser.getValue(node, "__KRB_REALM__");
    String krbKdc = ElementParameterParser.getValue(node, "__KRB_KDC__");
    String dbuser   = ElementParameterParser.getValue(node, "__USERNAME__");
    String dbpass   = ElementParameterParser.getValue(node, "__PASSWORD__");
    String mongoPort = ElementParameterParser.getValue(node, "__PORT__");
    String dbport = mongoPort.startsWith("context.") ? "Integer.valueOf(" + mongoPort + ").intValue()" : mongoPort.replace("\"", "");
    String collection=ElementParameterParser.getValue(node, "__COLLECTION__");
    boolean dropExistCollection="true".equalsIgnoreCase(ElementParameterParser.getValue(node,"__DROP_COLLECTION_CREATE__"));
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
    String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
    
    List<Map<String, String>> mapping = (List<Map<String,String>>)ElementParameterParser.getObjectValueXML(node, "__MAPPING__");
    boolean useReplicaSet="true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_REPLICA_SET__"));

    boolean setWriteConcern = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_WRITE_CONCERN__"));
    String writeConcern = ElementParameterParser.getValue(node, "__WRITE_CONCERN__");

    boolean setBulkWrite = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_BULK_WRITE__"));
    String bulkWriteOperationSize = ElementParameterParser.getValue(node, "__BULK_WRITE_SIZE__");
    String bulkWriteType = ElementParameterParser.getValue(node, "__BULK_WRITE_TYPE__");
    // BulkWrite is only supportted on Mongo 2.6+ drivers
    final boolean bulkWrite = setBulkWrite & (!"MONGODB_2_5_X".equalsIgnoreCase(dbversion));
    boolean useAuthDB = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_AUTHENTICATION_DATABASE__"));
    String authDB = ElementParameterParser.getValue(node, "__AUTHENTICATION_DATABASE__");
    String usedAuthenticationDB = useAuthDB ? authDB : dbname;
    boolean queryOptionNoTimeOut = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__QUERYOPTION_NOTIMEOUT__"));

    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_2);
    
    List<IMetadataTable> metadatas = node.getMetadataList();
  	if(metadatas != null && metadatas.size() > 0){
      	IMetadataTable metadata = metadatas.get(0);
      	if(metadata != null){
		
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
			for (int i=0;i<mapping.size();i++) {
				String path = mapping.get(i).get("PARENT_NODE_PATH");
				if(path==null || "".equals(path)){
					path="\"\"";
				}
				String schemaColumn=mapping.get(i).get("SCHEMA_COLUMN");

				
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(schemaColumn);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(path);
    stringBuffer.append(TEXT_17);
    
			}
      		if (useExistingConnection){
      			String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
				
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_22);
    
				if(isLog4jEnabled){
				
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_27);
    
				}
			}else{
	 			List<Map<String,String>> replicaAddrs= (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__REPLICA_SET__");
	 		    boolean useSSL = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_SSL__"));

	 		    String mongoDbDriver = "com.mongodb.MongoClient";

	 		    
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    
                if (useSSL) {
                    
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
                }
                
                // Client Credentials
                
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    
                // Authentication
                if (authentication){
                    
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    
                    if((authenticationMechanism.equals("NEGOTIATE_MEC"))||(authenticationMechanism.equals("PLAIN_MEC"))||(authenticationMechanism.equals("SCRAMSHA1_MEC"))){
                        String passwordFieldName = "__PASSWORD__";
                        
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_38);
    } else {
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_41);
    }
    
                        if(authenticationMechanism.equals("NEGOTIATE_MEC")){
                			if(dbversion.equals("MONGODB_3_0_X") || dbversion.equals("MONGODB_3_2_X")) {

    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
                        
                			} else {

    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    
                			}
                        } else if(authenticationMechanism.equals("PLAIN_MEC")){
                            
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    
                        } else if(authenticationMechanism.equals("SCRAMSHA1_MEC")){
                            
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
                        }
                    } else { // GSSAPI SASL (KERBEROS)
                        
    stringBuffer.append(TEXT_61);
    stringBuffer.append(krbRealm);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(krbKdc);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(krbUserPrincipal);
    stringBuffer.append(TEXT_65);
    
                    }
                    
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    
                }

                // the client
                if(useReplicaSet){
                    
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    
                    for(Map<String,String> replicaAddr:replicaAddrs){
                    
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(replicaAddr.get("REPLICA_HOST"));
    stringBuffer.append(TEXT_73);
    stringBuffer.append(replicaAddr.get("REPLICA_PORT"));
    stringBuffer.append(TEXT_74);
    
                    }
                    
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(mongoDbDriver);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    
                }else{
                    
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(mongoDbDriver);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    
                }
                
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_95);
    

			}

			if(setWriteConcern){
				
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(writeConcern);
    stringBuffer.append(TEXT_98);
    
			}

			if(queryOptionNoTimeOut && !useExistingConnection){
		        
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    
    		}

			
    stringBuffer.append(TEXT_101);
    
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    
			}
			if(dropExistCollection){
			
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(collection);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(collection);
    stringBuffer.append(TEXT_109);
    
			}
			
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(collection);
    stringBuffer.append(TEXT_113);
    

			// BulkWrite
			if(bulkWrite){
			
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(bulkWriteOperationSize);
    stringBuffer.append(TEXT_120);
    
			}

			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    
			}
    	}
    }
	
    return stringBuffer.toString();
  }
}
