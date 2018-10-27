package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class TNeo4jOutputEndJava
{
  protected static String nl;
  public static synchronized TNeo4jOutputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jOutputEndJava result = new TNeo4jOutputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "            org.neo4j.graphdb.Transaction tx_";
  protected final String TEXT_2 = " = databaseService_";
  protected final String TEXT_3 = ".beginTx();";
  protected final String TEXT_4 = NL + "                org.neo4j.graphdb.Transaction tx_";
  protected final String TEXT_5 = " = databaseService_";
  protected final String TEXT_6 = ".beginTx();";
  protected final String TEXT_7 = NL + "            if (counter_";
  protected final String TEXT_8 = " % Long.parseLong(";
  protected final String TEXT_9 = ") == 0) {" + NL + "                tx_";
  protected final String TEXT_10 = ".success();" + NL + "                tx_";
  protected final String TEXT_11 = ".finish();" + NL + "                tx_";
  protected final String TEXT_12 = " = databaseService_";
  protected final String TEXT_13 = ".beginTx();" + NL + "            }";
  protected final String TEXT_14 = NL + "                if (counter_";
  protected final String TEXT_15 = " % Long.parseLong(";
  protected final String TEXT_16 = ") == 0) {" + NL + "                    tx_";
  protected final String TEXT_17 = ".success();" + NL + "                    tx_";
  protected final String TEXT_18 = ".finish();" + NL + "                    tx_";
  protected final String TEXT_19 = " = databaseService_";
  protected final String TEXT_20 = ".beginTx();" + NL + "                }";
  protected final String TEXT_21 = NL + "            if (counter_";
  protected final String TEXT_22 = " % Long.parseLong(";
  protected final String TEXT_23 = ") > 0) {" + NL + "                tx_";
  protected final String TEXT_24 = ".success();" + NL + "                tx_";
  protected final String TEXT_25 = ".finish();" + NL + "            }";
  protected final String TEXT_26 = NL + "                if (counter_";
  protected final String TEXT_27 = " % Long.parseLong(";
  protected final String TEXT_28 = ") > 0) {" + NL + "                    tx_";
  protected final String TEXT_29 = ".success();" + NL + "                    tx_";
  protected final String TEXT_30 = ".finish();" + NL + "                }";
  protected final String TEXT_31 = NL + "                org.neo4j.graphdb.Transaction tx_begin_";
  protected final String TEXT_32 = " = databaseService_";
  protected final String TEXT_33 = ".beginTx();" + NL + "                try" + NL + "                {";
  protected final String TEXT_34 = NL + "                tx_begin_";
  protected final String TEXT_35 = ".success();" + NL + "                } // end of Try" + NL + "                finally" + NL + "                {" + NL + "                    tx_begin_";
  protected final String TEXT_36 = ".close();" + NL + "                }";
  protected final String TEXT_37 = NL + "\t\tdatabaseService_";
  protected final String TEXT_38 = ".shutdown();" + NL + "\t\tresourceMap.put(\"finish_";
  protected final String TEXT_39 = "\", true);" + NL + "\t\t";
  protected final String TEXT_40 = NL + "\t    \tindexInserterMap_";
  protected final String TEXT_41 = ".get(";
  protected final String TEXT_42 = ").flush();" + NL + "\t    \t";
  protected final String TEXT_43 = NL + "            // Must shutdown database to prevent data corruption after batch importation" + NL + "\t        if(indexProvider_";
  protected final String TEXT_44 = "!=null){" + NL + "                indexProvider_";
  protected final String TEXT_45 = ".shutdown();" + NL + "            }" + NL + "            if(inserter_";
  protected final String TEXT_46 = "!=null){" + NL + "                inserter_";
  protected final String TEXT_47 = ".shutdown();" + NL + "            }" + NL + "            resourceMap.put(\"finish_";
  protected final String TEXT_48 = "\", true); ";
  protected final String TEXT_49 = NL + "                databaseService_";
  protected final String TEXT_50 = " = new org.neo4j.kernel.EmbeddedGraphDatabase(storeDir_";
  protected final String TEXT_51 = ");" + NL + "                globalMap.put(\"";
  protected final String TEXT_52 = "\", databaseService_";
  protected final String TEXT_53 = ");";
  protected final String TEXT_54 = NL + "            // Must shutdown database to prevent data corruption after batch importation" + NL + "            if ((indexProvider_";
  protected final String TEXT_55 = " != null) && (globalMap.containsKey(\"";
  protected final String TEXT_56 = "_indexprovider\"))) {" + NL + "                indexProvider_";
  protected final String TEXT_57 = ".shutdown();" + NL + "                globalMap.remove(\"";
  protected final String TEXT_58 = "_indexprovider\");" + NL + "            }" + NL + "            if ((inserter_";
  protected final String TEXT_59 = " != null) && (globalMap.containsKey(\"";
  protected final String TEXT_60 = "_inserter\"))) {" + NL + "                try {" + NL + "                    inserter_";
  protected final String TEXT_61 = ".shutdown();" + NL + "                    globalMap.remove(\"";
  protected final String TEXT_62 = "_inserter\");" + NL + "                } catch (IllegalStateException e_";
  protected final String TEXT_63 = ") {" + NL + "                    System.err.println(e_";
  protected final String TEXT_64 = ".getMessage());" + NL + "                    // Nothing, the shutdown must be called two times due to multiples components" + NL + "                }" + NL + "            }" + NL + "            resourceMap.put(\"finish_";
  protected final String TEXT_65 = "\", true); " + NL;
  protected final String TEXT_66 = NL + "                // if the db connection is still shutted down, reopen it for further components." + NL + "                // Otherwise, that mean that another component with the embedded connection has done it previously" + NL + "                if (((Boolean) globalMap.get(\"";
  protected final String TEXT_67 = "_is_temporary_shutted_down\"))) {" + NL + "                    databaseService_";
  protected final String TEXT_68 = " = new org.neo4j.graphdb.factory.GraphDatabaseFactory().newEmbeddedDatabase((String)globalMap.get(\"";
  protected final String TEXT_69 = "_path\"));" + NL + "                    globalMap.put(\"";
  protected final String TEXT_70 = "\", databaseService_";
  protected final String TEXT_71 = ");" + NL + "                    globalMap.put(\"";
  protected final String TEXT_72 = "_is_temporary_shutted_down\", false);" + NL + "                }";
  protected final String TEXT_73 = NL + "globalMap.put(\"";
  protected final String TEXT_74 = "_NB_LINE\", nb_line_";
  protected final String TEXT_75 = ");";
  protected final String TEXT_76 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
class Neo4JTransactionHelper {
    private INode node;
    private String cid;
    private String dbVersion;
    private boolean batchImport;
    private boolean remoteServer;
    private String commitEvery;

    public Neo4JTransactionHelper(INode node) {
        this.node = node;
        this.cid = node.getUniqueName();

        this.batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
        this.commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__");
        boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));

        if (useExistingConnection) {
            String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
            for(INode targetNode : node.getProcess().getNodesOfType("tNeo4jConnection")){
                if (targetNode.getUniqueName().equals(connection)) {
                    this.dbVersion = ElementParameterParser.getValue(targetNode, "__DB_VERSION__");
                    this.remoteServer = "true".equalsIgnoreCase(ElementParameterParser.getValue(targetNode, "__REMOTE_SERVER__"));
                    break;
                }
            }
        } else {
            this.dbVersion = ElementParameterParser.getValue(node,"__DB_VERSION__");
            this.remoteServer = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REMOTE_SERVER__"));
        }

    }

    public String getDbVersion() {
        return this.dbVersion;
    }

    public void startTransaction() {
        if ("NEO4J_1_X_X".equals(this.dbVersion)) {
            
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            if (!this.batchImport) {
                
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    
            }
        }
    }

    public void restartTransaction() {
        if ("NEO4J_1_X_X".equals(this.dbVersion)) {
            
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_13);
    
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            if (!this.batchImport) {
                
    stringBuffer.append(TEXT_14);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(this.cid);
    stringBuffer.append(TEXT_20);
    
            }
        }
    }

    public void closeTransaction() {
        if ("NEO4J_1_X_X".equals(this.dbVersion)) {
            
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            if (!this.batchImport) {
                
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    
            }
        }
    }

    public void startEmbeddedTransaction() {
        if ("NEO4J_1_X_X".equals(this.dbVersion)) {
            // Nothing for neo4j 1.X.X
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            if (!remoteServer) {
                
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    
            }
        }
    }

    public void closeEmbeddedTransaction() {
        if ("NEO4J_1_X_X".equals(this.dbVersion)) {
            // Nothing for neo4j 1.X.X
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            if (!remoteServer) {
                
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    
            }
        }
    }
}

    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    Neo4JTransactionHelper neo4JTransactionHelper = new Neo4JTransactionHelper(node);
    String cid = node.getUniqueName();
    String commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__"); 

    boolean batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
    String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
    String dbVersion = ElementParameterParser.getValue(node,"__DB_VERSION__");

    if (useExistingConnection) {
        List<? extends INode> nodes = node.getProcess().getGeneratingNodes();
        for(INode targetNode : nodes){
            if (targetNode.getUniqueName().equals(connection)) {
                dbVersion = ElementParameterParser.getValue(targetNode, "__DB_VERSION__");
                break;
            }
        }
    }

    neo4JTransactionHelper.closeTransaction();

    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));

    String dbconn = "";
    if (useExistingConnection) {
        dbconn = "dbService_" + connection;
    } else {
        dbconn = "dbService_" + cid;
    }
    List<Map<String, String>> values = (List<Map<String, String>>) ElementParameterParser.getObjectValue(node, "__VALUES__");


	if (!batchImport && (!useExistingConnection || (useExistingConnection && shutdownDb))) {
		
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    
	}
	if (batchImport) {
		Set<String> indexNames = new HashSet();
	    for (Map<String, String> value : values) {
	   		String strIndexNames = value.get("INDEX_NAMES");
	   		if (strIndexNames != null && !strIndexNames.isEmpty()) {
	   			indexNames.addAll(Arrays.asList(strIndexNames.split(",")));
	   		}
	    }
	    for (String indexName : indexNames) {
	    	
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_42);
    
	    }

	    if ("NEO4J_1_X_X".equals(dbVersion)) {
	        
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    
            if (useExistingConnection && !shutdownDb) {
                
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
            }
	    } else { // NEO4J_2_1_X or NEO4J_2_2_X
	        
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    
            if (useExistingConnection && !shutdownDb) {
                
    stringBuffer.append(TEXT_66);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(dbconn);
    stringBuffer.append(TEXT_72);
    
            }
	    }
	}

    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(TEXT_76);
    return stringBuffer.toString();
  }
}
