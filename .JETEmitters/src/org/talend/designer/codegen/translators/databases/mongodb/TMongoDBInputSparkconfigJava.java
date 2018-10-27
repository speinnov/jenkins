package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.designer.common.tsqlrow.TSqlRowUtil;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.spark.generator.storage.MongoDBSparkStorage;

public class TMongoDBInputSparkconfigJava
{
  protected static String nl;
  public static synchronized TMongoDBInputSparkconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBInputSparkconfigJava result = new TMongoDBInputSparkconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = NL + "    org.apache.hadoop.conf.Configuration mongoConfig_";
  protected final String TEXT_2 = " = new org.apache.hadoop.conf.Configuration();" + NL + "    mongoConfig_";
  protected final String TEXT_3 = ".set(\"mongo.job.input.format\", \"com.mongodb.hadoop.MongoInputFormat\");" + NL + "" + NL + "    String connectionString_";
  protected final String TEXT_4 = " = ";
  protected final String TEXT_5 = ";";
  protected final String TEXT_6 = NL + "        connectionString_";
  protected final String TEXT_7 = " += \"&readPreference=";
  protected final String TEXT_8 = "\";";
  protected final String TEXT_9 = NL + "        connectionString_";
  protected final String TEXT_10 = " += \"&readPreferenceTags=\" + ";
  protected final String TEXT_11 = ";";
  protected final String TEXT_12 = NL + "        String authString_";
  protected final String TEXT_13 = " = ";
  protected final String TEXT_14 = ";" + NL + "        mongoConfig_";
  protected final String TEXT_15 = ".set(\"mongo.auth.uri\", authString_";
  protected final String TEXT_16 = ");";
  protected final String TEXT_17 = NL + "    mongoConfig_";
  protected final String TEXT_18 = ".set(\"mongo.input.uri\", connectionString_";
  protected final String TEXT_19 = ");" + NL + "    mongoConfig_";
  protected final String TEXT_20 = ".set(\"mongo.input.query\", ";
  protected final String TEXT_21 = ");" + NL;
  protected final String TEXT_22 = NL + "        mongoConfig_";
  protected final String TEXT_23 = ".set(\"mongo.input.limit\", ";
  protected final String TEXT_24 = ");";
  protected final String TEXT_25 = NL + "        mongoConfig_";
  protected final String TEXT_26 = ".set(";
  protected final String TEXT_27 = ", ";
  protected final String TEXT_28 = ");";
  protected final String TEXT_29 = NL + "            org.apache.spark.api.java.JavaPairRDD<Object, org.bson.BSONObject> bsonRDD_";
  protected final String TEXT_30 = " = ctx.sparkContext().newAPIHadoopRDD(" + NL + "            mongoConfig_";
  protected final String TEXT_31 = "," + NL + "            com.mongodb.hadoop.MongoInputFormat.class," + NL + "            Object.class," + NL + "            org.bson.BSONObject.class" + NL + "            );";
  protected final String TEXT_32 = NL + "        org.apache.spark.api.java.JavaPairRDD<Object, org.bson.BSONObject> bsonRDD_";
  protected final String TEXT_33 = " = ctx.newAPIHadoopRDD(" + NL + "        mongoConfig_";
  protected final String TEXT_34 = "," + NL + "        com.mongodb.hadoop.MongoInputFormat.class," + NL + "        Object.class," + NL + "        org.bson.BSONObject.class" + NL + "        );";
  protected final String TEXT_35 = NL + NL + "    org.apache.spark.api.java.JavaRDD<";
  protected final String TEXT_36 = "> rdd_";
  protected final String TEXT_37 = " = bsonRDD_";
  protected final String TEXT_38 = ".values().map(new ";
  protected final String TEXT_39 = "_FromBsonTo";
  protected final String TEXT_40 = "(job));" + NL;
  protected final String TEXT_41 = NL + "        rdd_";
  protected final String TEXT_42 = ".cache();";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

// reuse tSqlRowUtil to get outStructName
TSqlRowUtil tSqlRowUtil = new TSqlRowUtil(node);
String validateError = tSqlRowUtil.validate(false, true);
if (validateError != null) {
    // Cause the job compilation to explicitly fail if there is a problem.
    return "throw new JobConfigurationError(\"" + validateError +"\");";
}

String outStructName = codeGenArgument.getRecordStructName(tSqlRowUtil.getOutgoingConnection());

String collection = ElementParameterParser.getValue(node,"__COLLECTION__");

String query = ElementParameterParser.getValue(node,"__QUERY__");
query = query.replaceAll("\n","");
query = query.replaceAll("\r","");
String limit = ElementParameterParser.getValue(node,"__LIMIT__");

java.util.List<java.util.Map<String, String>> hadoopConfiguration = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValueXML(node, "__ADVANCED_HADOOP_MONGODB__");

boolean setReadPreference = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_READ_PREFERENCE__"));
String readPreference = ElementParameterParser.getValue(node, "__READ_PREFERENCE__");
boolean setReadPreferenceTags = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_READ_PREFERENCE_TAGS__"));
String readPreferenceTags = ElementParameterParser.getValue(node, "__READ_PREFERENCE_TAGS__");

String mongoDBConfiguration = ElementParameterParser.getValue(node,"__MONGODB_CONFIGURATION__");
INode mongoDBConfigurationNode = null;

for (INode pNode1 : node.getProcess().getNodesOfType("tMongoDBConfiguration")) {
    if(mongoDBConfiguration!=null && mongoDBConfiguration.equals(pNode1.getUniqueName())) {
        mongoDBConfigurationNode = pNode1;
        break;
    }
}
MongoDBSparkStorage storage = new MongoDBSparkStorage(mongoDBConfigurationNode);


    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(storage.getIOConnectionString(collection));
    stringBuffer.append(TEXT_5);
    
    if(setReadPreference){
        
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(readPreference);
    stringBuffer.append(TEXT_8);
    
    }
    if(setReadPreference && setReadPreferenceTags){
        
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(readPreferenceTags);
    stringBuffer.append(TEXT_11);
    
    }

    if(storage.isUseAuthDB()){
        
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(storage.getAuthConnectionString());
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    
    }

    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_21);
    
    if (limit != null && !"\"\"".equals(limit) && limit.length() > 0){
        
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(limit);
    stringBuffer.append(TEXT_24);
    
    }

    for(java.util.Map<String,String> config: hadoopConfiguration){
        
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(config.get("PROPERTY"));
    stringBuffer.append(TEXT_27);
    stringBuffer.append(config.get("VALUE"));
    stringBuffer.append(TEXT_28);
    
    }
    if("SPARKSTREAMING".equals(node.getComponent().getType())){
        
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
    } else {
        
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
    }
    
    stringBuffer.append(TEXT_35);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(tSqlRowUtil.getOutgoingConnection().getName());
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_40);
    
    if("SPARKSTREAMING".equals(node.getComponent().getType()) && false){
    
    stringBuffer.append(TEXT_41);
    stringBuffer.append(tSqlRowUtil.getOutgoingConnection().getName());
    stringBuffer.append(TEXT_42);
    
    }
    
    

    return stringBuffer.toString();
  }
}
