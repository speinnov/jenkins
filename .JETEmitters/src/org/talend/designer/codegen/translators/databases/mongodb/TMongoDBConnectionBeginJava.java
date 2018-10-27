package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.Map;
import java.util.List;

public class TMongoDBConnectionBeginJava
{
  protected static String nl;
  public static synchronized TMongoDBConnectionBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBConnectionBeginJava result = new TMongoDBConnectionBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    // Empty client options" + NL + "    com.mongodb.MongoClientOptions clientOptions_";
  protected final String TEXT_2 = " = new com.mongodb.MongoClientOptions.Builder().build();" + NL;
  protected final String TEXT_3 = NL + "    // Empty client credentials list" + NL + "    java.util.List<com.mongodb.MongoCredential> mongoCredentialList_";
  protected final String TEXT_4 = " = new java.util.ArrayList<com.mongodb.MongoCredential>();" + NL;
  protected final String TEXT_5 = NL + "        clientOptions_";
  protected final String TEXT_6 = " = new com.mongodb.MongoClientOptions.Builder()" + NL + "        .socketFactory(javax.net.ssl.SSLSocketFactory.getDefault())" + NL + "        .build();";
  protected final String TEXT_7 = NL + "        com.mongodb.MongoCredential mongoCredential_";
  protected final String TEXT_8 = ";";
  protected final String TEXT_9 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_10 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_11 = ");";
  protected final String TEXT_12 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_13 = " = ";
  protected final String TEXT_14 = "; ";
  protected final String TEXT_15 = NL + "                    mongoCredential_";
  protected final String TEXT_16 = " = com.mongodb.MongoCredential.createCredential(";
  protected final String TEXT_17 = ", ";
  protected final String TEXT_18 = ", new String(decryptedPassword_";
  protected final String TEXT_19 = ").toCharArray());";
  protected final String TEXT_20 = NL + "                    mongoCredential_";
  protected final String TEXT_21 = " = com.mongodb.MongoCredential.createMongoCRCredential(";
  protected final String TEXT_22 = ", ";
  protected final String TEXT_23 = ", new String(decryptedPassword_";
  protected final String TEXT_24 = ").toCharArray());";
  protected final String TEXT_25 = NL + "                mongoCredential_";
  protected final String TEXT_26 = " = com.mongodb.MongoCredential.createPlainCredential(";
  protected final String TEXT_27 = ", \"$external\", new String(decryptedPassword_";
  protected final String TEXT_28 = ").toCharArray());";
  protected final String TEXT_29 = NL + "                mongoCredential_";
  protected final String TEXT_30 = " = com.mongodb.MongoCredential.createScramSha1Credential(";
  protected final String TEXT_31 = ", ";
  protected final String TEXT_32 = ", new String(decryptedPassword_";
  protected final String TEXT_33 = ").toCharArray());";
  protected final String TEXT_34 = NL + "            System.setProperty(\"java.security.krb5.realm\", ";
  protected final String TEXT_35 = ");" + NL + "            System.setProperty(\"java.security.krb5.kdc\", ";
  protected final String TEXT_36 = ");" + NL + "            System.setProperty(\"javax.security.auth.useSubjectCredsOnly\", \"false\");" + NL + "            mongoCredential_";
  protected final String TEXT_37 = " = com.mongodb.MongoCredential.createGSSAPICredential(";
  protected final String TEXT_38 = ");";
  protected final String TEXT_39 = NL + "        mongoCredentialList_";
  protected final String TEXT_40 = ".add(mongoCredential_";
  protected final String TEXT_41 = ");";
  protected final String TEXT_42 = NL + "        List<com.mongodb.ServerAddress> serverAddressList_";
  protected final String TEXT_43 = " = new java.util.ArrayList<com.mongodb.ServerAddress>();";
  protected final String TEXT_44 = NL + "            serverAddressList_";
  protected final String TEXT_45 = ".add(new com.mongodb.ServerAddress(";
  protected final String TEXT_46 = ",";
  protected final String TEXT_47 = "));";
  protected final String TEXT_48 = NL + "        com.mongodb.Mongo mongo_";
  protected final String TEXT_49 = " = new ";
  protected final String TEXT_50 = "(serverAddressList_";
  protected final String TEXT_51 = ", mongoCredentialList_";
  protected final String TEXT_52 = ", clientOptions_";
  protected final String TEXT_53 = ");";
  protected final String TEXT_54 = NL + "        com.mongodb.ServerAddress serverAddress_";
  protected final String TEXT_55 = " = new com.mongodb.ServerAddress(";
  protected final String TEXT_56 = ", ";
  protected final String TEXT_57 = ");" + NL + "        com.mongodb.Mongo mongo_";
  protected final String TEXT_58 = " = new ";
  protected final String TEXT_59 = "(serverAddress_";
  protected final String TEXT_60 = ", mongoCredentialList_";
  protected final String TEXT_61 = ", clientOptions_";
  protected final String TEXT_62 = ");";
  protected final String TEXT_63 = NL + "    mongo_";
  protected final String TEXT_64 = ".getAddress() ;" + NL + "    com.mongodb.DB db_";
  protected final String TEXT_65 = " = mongo_";
  protected final String TEXT_66 = ".getDB(";
  protected final String TEXT_67 = ");";
  protected final String TEXT_68 = NL + "        log.info(\"";
  protected final String TEXT_69 = " - Connecting to \" + mongo_";
  protected final String TEXT_70 = ".getServerAddressList() + \".\");";
  protected final String TEXT_71 = NL + "            mongo_";
  protected final String TEXT_72 = ".addOption(com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT);";
  protected final String TEXT_73 = NL + "    globalMap.put(\"mongo_";
  protected final String TEXT_74 = "\", mongo_";
  protected final String TEXT_75 = ");" + NL + "    globalMap.put(\"db_";
  protected final String TEXT_76 = "\", db_";
  protected final String TEXT_77 = ");";
  protected final String TEXT_78 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;

    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    String dbhost = ElementParameterParser.getValue(node, "__HOST__");
    String dbname = ElementParameterParser.getValue(node, "__DATABASE__");
    String dbuser = ElementParameterParser.getValue(node, "__USERNAME__");
    String dbpass = ElementParameterParser.getValue(node, "__PASSWORD__");
    String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
    boolean authentication = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REQUIRED_AUTHENTICATION__"));
    String authenticationMechanism = ElementParameterParser.getValue(node, "__AUTHENTICATION_MECHANISM__");
    String krbUserPrincipal = ElementParameterParser.getValue(node, "__KRB_USER_PRINCIPAL__");
    String krbRealm = ElementParameterParser.getValue(node, "__KRB_REALM__");
    String krbKdc = ElementParameterParser.getValue(node, "__KRB_KDC__");
    boolean useReplicaSet = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_REPLICA_SET__"));
    boolean useSSL = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_SSL__"));
    String mongoPort = ElementParameterParser.getValue(node, "__PORT__");
    String dbport = mongoPort.startsWith("context.") ? "Integer.valueOf(" + mongoPort + ").intValue()" : mongoPort.replace("\"", "");
    List<Map<String,String>> replicaAddrs= (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__REPLICA_SET__");
    boolean setReadPreference = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_READ_PREFERENCE__"));
    String readPreference = ElementParameterParser.getValue(node, "__READ_PREFERENCE__");
    boolean useAuthDB = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_AUTHENTICATION_DATABASE__"));
    String authDB = ElementParameterParser.getValue(node, "__AUTHENTICATION_DATABASE__");
    String usedAuthenticationDB = useAuthDB ? authDB : dbname;
    boolean queryOptionNoTimeOut = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__QUERYOPTION_NOTIMEOUT__"));
    
    String mongoDbDriver = "com.mongodb.MongoClient";
    
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    // Client options
    
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    

    // Client Credentials
    
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    

    // SSL
    if (useSSL) {
        
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    
    }

    // Authentication
    if (authentication){
        
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
        if((authenticationMechanism.equals("NEGOTIATE_MEC"))||(authenticationMechanism.equals("PLAIN_MEC"))||(authenticationMechanism.equals("SCRAMSHA1_MEC"))){
            String passwordFieldName = "__PASSWORD__";

    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_11);
    } else {
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_14);
    }
    
            if(authenticationMechanism.equals("NEGOTIATE_MEC")){
                if(dbversion.equals("MONGODB_3_0_X") || dbversion.equals("MONGODB_3_2_X")) {

    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
                        
                } else {

    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    
                }
            } else if(authenticationMechanism.equals("PLAIN_MEC")){

    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    
            } else if(authenticationMechanism.equals("SCRAMSHA1_MEC")){

    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(usedAuthenticationDB);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    
            }
        } else { // GSSAPI SASL (KERBEROS)

    stringBuffer.append(TEXT_34);
    stringBuffer.append(krbRealm);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(krbKdc);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(krbUserPrincipal);
    stringBuffer.append(TEXT_38);
    
        }
        
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    
    }

    // The client
    if(useReplicaSet){
        
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    
        for(Map<String,String> replicaAddr:replicaAddrs){
        
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(replicaAddr.get("REPLICA_HOST"));
    stringBuffer.append(TEXT_46);
    stringBuffer.append(replicaAddr.get("REPLICA_PORT"));
    stringBuffer.append(TEXT_47);
    
        }
        
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(mongoDbDriver);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
    }else{
        
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(mongoDbDriver);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    
    }

    
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_67);
    

    if(isLog4jEnabled){
    
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    
    }

    if(queryOptionNoTimeOut){
        
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    
    }

    
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(TEXT_78);
    return stringBuffer.toString();
  }
}
