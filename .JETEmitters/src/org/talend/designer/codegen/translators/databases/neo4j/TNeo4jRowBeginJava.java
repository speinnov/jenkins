package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;

public class TNeo4jRowBeginJava
{
  protected static String nl;
  public static synchronized TNeo4jRowBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jRowBeginJava result = new TNeo4jRowBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\torg.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_2 = " = (org.neo4j.graphdb.GraphDatabaseService) globalMap.get(\"";
  protected final String TEXT_3 = "\");" + NL + "\t";
  protected final String TEXT_4 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_5 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_6 = ");";
  protected final String TEXT_7 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_8 = " = ";
  protected final String TEXT_9 = "; ";
  protected final String TEXT_10 = NL + "                org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_11 = " = new org.neo4j.rest.graphdb.RestGraphDatabase(";
  protected final String TEXT_12 = ", ";
  protected final String TEXT_13 = ", decryptedPassword_";
  protected final String TEXT_14 = ");";
  protected final String TEXT_15 = NL + "                org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_16 = " = new org.neo4j.rest.graphdb.RestGraphDatabase(";
  protected final String TEXT_17 = ");";
  protected final String TEXT_18 = NL + "                org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_19 = " = new org.neo4j.kernel.EmbeddedGraphDatabase(";
  protected final String TEXT_20 = ");";
  protected final String TEXT_21 = NL + "                org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_22 = " = new org.neo4j.graphdb.factory.GraphDatabaseFactory().newEmbeddedDatabase(";
  protected final String TEXT_23 = ");";
  protected final String TEXT_24 = "   " + NL + "org.neo4j.rest.graphdb.query.QueryEngine<java.util.Map<String, Object>> queryEngine_";
  protected final String TEXT_25 = " = null;" + NL + "org.neo4j.cypher.ExecutionEngine engine_";
  protected final String TEXT_26 = " = null;" + NL + "if (databaseService_";
  protected final String TEXT_27 = " instanceof org.neo4j.rest.graphdb.RestGraphDatabase) {" + NL + "\torg.neo4j.rest.graphdb.RestGraphDatabase restDatabase_";
  protected final String TEXT_28 = " = (org.neo4j.rest.graphdb.RestGraphDatabase) databaseService_";
  protected final String TEXT_29 = "; " + NL + "\tqueryEngine_";
  protected final String TEXT_30 = " = new org.neo4j.rest.graphdb.query.RestCypherQueryEngine(restDatabase_";
  protected final String TEXT_31 = ".getRestAPI());" + NL + "} else {";
  protected final String TEXT_32 = NL + "        engine_";
  protected final String TEXT_33 = " = new org.neo4j.cypher.ExecutionEngine(databaseService_";
  protected final String TEXT_34 = ");";
  protected final String TEXT_35 = NL + "        engine_";
  protected final String TEXT_36 = " = new org.neo4j.cypher.ExecutionEngine(databaseService_";
  protected final String TEXT_37 = ", org.neo4j.kernel.impl.util.StringLogger.SYSTEM);";
  protected final String TEXT_38 = NL + "        engine_";
  protected final String TEXT_39 = " = new org.neo4j.cypher.ExecutionEngine(databaseService_";
  protected final String TEXT_40 = ", org.neo4j.logging.FormattedLogProvider.toOutputStream(System.out));";
  protected final String TEXT_41 = NL + "}" + NL + "" + NL + "if (databaseService_";
  protected final String TEXT_42 = ".getClass().equals(org.neo4j.kernel.EmbeddedGraphDatabase.class)) {";
  protected final String TEXT_43 = NL + "        ((org.neo4j.kernel.EmbeddedGraphDatabase)databaseService_";
  protected final String TEXT_44 = ").isAvailable(10000l);";
  protected final String TEXT_45 = NL + "        while (((org.neo4j.kernel.EmbeddedGraphDatabase)databaseService_";
  protected final String TEXT_46 = ").transactionRunning()){" + NL + "            // wait the end of the previous transaction" + NL + "        }";
  protected final String TEXT_47 = NL + "}" + NL + "org.neo4j.graphdb.Transaction tx_";
  protected final String TEXT_48 = " = databaseService_";
  protected final String TEXT_49 = ".beginTx();" + NL + "" + NL + "long counter_";
  protected final String TEXT_50 = " = 0;" + NL + "long nbNodeInserted_";
  protected final String TEXT_51 = " = 0;" + NL + "long nbRelationshipInserted_";
  protected final String TEXT_52 = " = 0;" + NL + "long nbPropertiesUpdated_";
  protected final String TEXT_53 = " = 0;" + NL + "long nbNodeDeleted_";
  protected final String TEXT_54 = " = 0;" + NL + "long nbRelationshipDeleted_";
  protected final String TEXT_55 = " = 0;";
  protected final String TEXT_56 = NL + "resourceMap.put(\"databaseService_";
  protected final String TEXT_57 = "\", databaseService_";
  protected final String TEXT_58 = "); ";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	

    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));
    String dbVersion = ElementParameterParser.getValue(node,"__DB_VERSION__");

    if (useExistingConnection) {
    	String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
        List<? extends INode> nodes = node.getProcess().getGeneratingNodes();
        for(INode targetNode : nodes){
            if (targetNode.getUniqueName().equals(connection)) {
                dbVersion = ElementParameterParser.getValue(targetNode, "__DB_VERSION__");
                break;
            }
        }
    	String dbconn = "dbService_" + connection;
		
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_3);
    
    } else {
    	boolean remoteServer = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REMOTE_SERVER__"));
    	if (remoteServer) {
            String serverUrl = ElementParameterParser.getValue(node, "__SERVER_URL__");
            boolean setUsername = ElementParameterParser.getBooleanValue(node, "__SET_USERNAME__");
            String username = ElementParameterParser.getValue(node, "__USERNAME__");
            String passwordFieldName = "__PASSWORD__";
            if ((setUsername) || ((!"NEO4J_1_X_X".equals(dbVersion)) && (!"NEO4J_2_1_X".equals(dbVersion)))) {
                
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_6);
    } else {
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_9);
    }
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(serverUrl);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
            } else {
                
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(serverUrl);
    stringBuffer.append(TEXT_17);
    
            }
    	} else {
	    	String databasePath = ElementParameterParser.getValue(node, "__DATABASE_PATH__");
	    	if ("NEO4J_1_X_X".equals(dbVersion)) {
                
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(databasePath);
    stringBuffer.append(TEXT_20);
    
            } else { // NEO4J_2_1_X or NEO4J_2_2_X or "NEO4J_2_3_X"
                
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(databasePath);
    stringBuffer.append(TEXT_23);
    
            }
		}
    }

    String userNbLine = ElementParameterParser.getValue(node, "__USE_NB_LINE__");

    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
    if ("NEO4J_1_X_X".equals(dbVersion)) {
        
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
    } else if (("NEO4J_2_1_X".equals(dbVersion)) || ("NEO4J_2_2_X".equals(dbVersion))) { // NEO4J_2_X before moving to spring framework
        
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    
    } else { // NEO4J_2_3 and more using spring framework
        
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    
    }
    
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    
    if ("NEO4J_2_2_X".equals(dbVersion) || "NEO4J_2_3_X".equals(dbVersion)) { // usage of Spring rest

        
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    
    } else {
        
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    
    }
    
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    
	if (!useExistingConnection||(useExistingConnection && shutdownDb)) {

    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    
	}

    return stringBuffer.toString();
  }
}
