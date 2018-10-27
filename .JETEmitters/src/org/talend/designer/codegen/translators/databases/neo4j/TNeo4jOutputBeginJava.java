package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;

public class TNeo4jOutputBeginJava
{
  protected static String nl;
  public static synchronized TNeo4jOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jOutputBeginJava result = new TNeo4jOutputBeginJava();
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
  protected final String TEXT_37 = NL + "            org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_38 = " = (org.neo4j.graphdb.GraphDatabaseService) globalMap.get(\"";
  protected final String TEXT_39 = "\");";
  protected final String TEXT_40 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_41 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_42 = ");";
  protected final String TEXT_43 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_44 = " = ";
  protected final String TEXT_45 = "; ";
  protected final String TEXT_46 = NL + "                    org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_47 = " = new org.neo4j.rest.graphdb.RestGraphDatabase(";
  protected final String TEXT_48 = ", ";
  protected final String TEXT_49 = ", decryptedPassword_";
  protected final String TEXT_50 = ");";
  protected final String TEXT_51 = NL + "                    org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_52 = " = new org.neo4j.rest.graphdb.RestGraphDatabase(";
  protected final String TEXT_53 = ");";
  protected final String TEXT_54 = NL + "                org.neo4j.graphdb.GraphDatabaseService databaseService_";
  protected final String TEXT_55 = " = null;" + NL + "                try{" + NL + "                \t\tdatabaseService_";
  protected final String TEXT_56 = " = new org.neo4j.graphdb.factory.GraphDatabaseFactory().newEmbeddedDatabase(";
  protected final String TEXT_57 = ");" + NL + "                }catch(java.lang.RuntimeException e){" + NL + "\t\t\t\t\t\t\tthrow new java.lang.RuntimeException(\"There may be inconsistency between the selected database version and the actual database version. Please check.\", e);" + NL + "                }";
  protected final String TEXT_58 = NL + "        if (databaseService_";
  protected final String TEXT_59 = " instanceof org.neo4j.kernel.EmbeddedGraphDatabase) {" + NL + "            org.neo4j.kernel.EmbeddedGraphDatabase db_";
  protected final String TEXT_60 = " = (org.neo4j.kernel.EmbeddedGraphDatabase) databaseService_";
  protected final String TEXT_61 = ";" + NL + "            storeDir_";
  protected final String TEXT_62 = " = db_";
  protected final String TEXT_63 = ".getStoreDir();" + NL + "            db_";
  protected final String TEXT_64 = ".shutdown();" + NL + "            inserter_";
  protected final String TEXT_65 = " = org.neo4j.unsafe.batchinsert.BatchInserters.inserter(db_";
  protected final String TEXT_66 = ".getStoreDir(), importProperties_";
  protected final String TEXT_67 = ");" + NL + "        } else {" + NL + "            throw new UnsupportedOperationException(\"Batch import is not supported with database server edition\");" + NL + "        }" + NL + "        indexProvider_";
  protected final String TEXT_68 = " = new org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider(inserter_";
  protected final String TEXT_69 = ");" + NL + "        resourceMap.put(\"inserter_";
  protected final String TEXT_70 = "\", inserter_";
  protected final String TEXT_71 = ");" + NL + "        resourceMap.put(\"indexProvider_";
  protected final String TEXT_72 = "\", indexProvider_";
  protected final String TEXT_73 = ");" + NL + "        java.util.Map<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex> indexInserterMap_";
  protected final String TEXT_74 = " = new java.util.HashMap<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex>();";
  protected final String TEXT_75 = NL + "            indexInserterMap_";
  protected final String TEXT_76 = ".put(";
  protected final String TEXT_77 = ", indexProvider_";
  protected final String TEXT_78 = ".nodeIndex(";
  protected final String TEXT_79 = ", org.neo4j.helpers.collection.MapUtil.stringMap(\"type\", \"exact\")));";
  protected final String TEXT_80 = NL + "            org.neo4j.graphdb.index.AutoIndexer<org.neo4j.graphdb.Node> autoIndexer_";
  protected final String TEXT_81 = " = databaseService_";
  protected final String TEXT_82 = ".index().getNodeAutoIndexer();" + NL + "            autoIndexer_";
  protected final String TEXT_83 = ".setEnabled(true);";
  protected final String TEXT_84 = NL + "            autoIndexer_";
  protected final String TEXT_85 = ".startAutoIndexingProperty(\"";
  protected final String TEXT_86 = "\");";
  protected final String TEXT_87 = NL + "        if (databaseService_";
  protected final String TEXT_88 = " instanceof org.neo4j.kernel.EmbeddedGraphDatabase) {" + NL + "            if (!globalMap.containsKey(\"";
  protected final String TEXT_89 = "_inserter\")) {" + NL + "                org.neo4j.kernel.EmbeddedGraphDatabase db_";
  protected final String TEXT_90 = " = (org.neo4j.kernel.EmbeddedGraphDatabase) databaseService_";
  protected final String TEXT_91 = ";" + NL + "                storeDir_";
  protected final String TEXT_92 = " = db_";
  protected final String TEXT_93 = ".getStoreDir();" + NL + "                db_";
  protected final String TEXT_94 = ".shutdown();" + NL + "                inserter_";
  protected final String TEXT_95 = " = org.neo4j.unsafe.batchinsert.BatchInserters.inserter(db_";
  protected final String TEXT_96 = ".getStoreDir(), importProperties_";
  protected final String TEXT_97 = ");" + NL + "                globalMap.put(\"";
  protected final String TEXT_98 = "_inserter\", inserter_";
  protected final String TEXT_99 = ");" + NL + "                globalMap.put(\"";
  protected final String TEXT_100 = "_is_temporary_shutted_down\", true);" + NL + "            } else {" + NL + "               inserter_";
  protected final String TEXT_101 = " = (org.neo4j.unsafe.batchinsert.BatchInserter) globalMap.get(\"";
  protected final String TEXT_102 = "_inserter\");" + NL + "            }" + NL + "        } else {" + NL + "            throw new UnsupportedOperationException(\"Batch import is not supported with database server edition\");" + NL + "        }" + NL + "" + NL + "        if (!globalMap.containsKey(\"";
  protected final String TEXT_103 = "_indexprovider\")) {" + NL + "            indexProvider_";
  protected final String TEXT_104 = " = new org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider(inserter_";
  protected final String TEXT_105 = ");" + NL + "            globalMap.put(\"";
  protected final String TEXT_106 = "_indexprovider\", indexProvider_";
  protected final String TEXT_107 = ");" + NL + "        } else {" + NL + "            indexProvider_";
  protected final String TEXT_108 = " = (org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider) globalMap.get(\"";
  protected final String TEXT_109 = "_indexprovider\");" + NL + "        }" + NL + "        resourceMap.put(\"inserter_";
  protected final String TEXT_110 = "\", inserter_";
  protected final String TEXT_111 = ");" + NL + "        resourceMap.put(\"indexProvider_";
  protected final String TEXT_112 = "\", indexProvider_";
  protected final String TEXT_113 = ");" + NL + "        java.util.Map<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex> indexInserterMap_";
  protected final String TEXT_114 = " = new java.util.HashMap<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex>();" + NL;
  protected final String TEXT_115 = NL + "            indexInserterMap_";
  protected final String TEXT_116 = ".put(";
  protected final String TEXT_117 = ", indexProvider_";
  protected final String TEXT_118 = ".nodeIndex(";
  protected final String TEXT_119 = ", org.neo4j.helpers.collection.MapUtil.stringMap(\"type\", \"exact\")));";
  protected final String TEXT_120 = NL + "            org.neo4j.unsafe.batchinsert.BatchInserterIndex autoindex_";
  protected final String TEXT_121 = " = indexProvider_";
  protected final String TEXT_122 = ".nodeIndex(\"node_auto_index\", org.neo4j.helpers.collection.MapUtil.stringMap(\"type\", \"exact\"));" + NL + "            long autoindex_id_";
  protected final String TEXT_123 = "  = inserter_";
  protected final String TEXT_124 = ".createNode(null);" + NL + "            java.util.Map<String, Object> autoindex_props_";
  protected final String TEXT_125 = " = new java.util.HashMap<String, Object>();";
  protected final String TEXT_126 = NL + "        if (databaseService_";
  protected final String TEXT_127 = " instanceof org.neo4j.kernel.impl.factory.GraphDatabaseFacade) {" + NL + "            if (!globalMap.containsKey(\"";
  protected final String TEXT_128 = "_inserter\")) {" + NL + "                org.neo4j.kernel.impl.factory.GraphDatabaseFacade db_";
  protected final String TEXT_129 = " = (org.neo4j.kernel.impl.factory.GraphDatabaseFacade) databaseService_";
  protected final String TEXT_130 = ";" + NL + "                storeDir_";
  protected final String TEXT_131 = " = db_";
  protected final String TEXT_132 = ".getStoreDir();" + NL + "                db_";
  protected final String TEXT_133 = ".shutdown();" + NL + "                inserter_";
  protected final String TEXT_134 = " = org.neo4j.unsafe.batchinsert.BatchInserters.inserter(db_";
  protected final String TEXT_135 = ".getStoreDir(), importProperties_";
  protected final String TEXT_136 = ");" + NL + "                globalMap.put(\"";
  protected final String TEXT_137 = "_inserter\", inserter_";
  protected final String TEXT_138 = ");" + NL + "                globalMap.put(\"";
  protected final String TEXT_139 = "_is_temporary_shutted_down\", true);" + NL + "            } else {" + NL + "               inserter_";
  protected final String TEXT_140 = " = (org.neo4j.unsafe.batchinsert.BatchInserter) globalMap.get(\"";
  protected final String TEXT_141 = "_inserter\");" + NL + "            }" + NL + "        } else {" + NL + "            throw new UnsupportedOperationException(\"Batch import is not supported with database server edition\");" + NL + "        }" + NL + "" + NL + "        if (!globalMap.containsKey(\"";
  protected final String TEXT_142 = "_indexprovider\")) {" + NL + "            indexProvider_";
  protected final String TEXT_143 = " = new org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider(inserter_";
  protected final String TEXT_144 = ");" + NL + "            globalMap.put(\"";
  protected final String TEXT_145 = "_indexprovider\", indexProvider_";
  protected final String TEXT_146 = ");" + NL + "        } else {" + NL + "            indexProvider_";
  protected final String TEXT_147 = " = (org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider) globalMap.get(\"";
  protected final String TEXT_148 = "_indexprovider\");" + NL + "        }" + NL + "        resourceMap.put(\"inserter_";
  protected final String TEXT_149 = "\", inserter_";
  protected final String TEXT_150 = ");" + NL + "        resourceMap.put(\"indexProvider_";
  protected final String TEXT_151 = "\", indexProvider_";
  protected final String TEXT_152 = ");" + NL + "        java.util.Map<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex> indexInserterMap_";
  protected final String TEXT_153 = " = new java.util.HashMap<String, org.neo4j.unsafe.batchinsert.BatchInserterIndex>();" + NL;
  protected final String TEXT_154 = NL + "            indexInserterMap_";
  protected final String TEXT_155 = ".put(";
  protected final String TEXT_156 = ", indexProvider_";
  protected final String TEXT_157 = ".nodeIndex(";
  protected final String TEXT_158 = ", org.neo4j.helpers.collection.MapUtil.stringMap(\"type\", \"exact\")));";
  protected final String TEXT_159 = NL + "            org.neo4j.unsafe.batchinsert.BatchInserterIndex autoindex_";
  protected final String TEXT_160 = " = indexProvider_";
  protected final String TEXT_161 = ".nodeIndex(\"node_auto_index\", org.neo4j.helpers.collection.MapUtil.stringMap(\"type\", \"exact\"));" + NL + "            long autoindex_id_";
  protected final String TEXT_162 = "  = inserter_";
  protected final String TEXT_163 = ".createNode(null);" + NL + "            java.util.Map<String, Object> autoindex_props_";
  protected final String TEXT_164 = " = new java.util.HashMap<String, Object>();";
  protected final String TEXT_165 = NL + "        java.util.Map<String, String> importProperties_";
  protected final String TEXT_166 = " = new java.util.HashMap<String, String>();" + NL + "        importProperties_";
  protected final String TEXT_167 = ".put(\"neostore.nodestore.db.mapped_memory\", ";
  protected final String TEXT_168 = ");" + NL + "        importProperties_";
  protected final String TEXT_169 = ".put(\"neostore.relationshipstore.db.mapped_memory\", ";
  protected final String TEXT_170 = ");" + NL + "        importProperties_";
  protected final String TEXT_171 = ".put(\"neostore.propertystore.db.mapped_memory\", ";
  protected final String TEXT_172 = ");" + NL + "        importProperties_";
  protected final String TEXT_173 = ".put(\"neostore.propertystore.db.strings.mapped_memory\", ";
  protected final String TEXT_174 = ");" + NL + "        importProperties_";
  protected final String TEXT_175 = ".put(\"neostore.propertystore.db.arrays.mapped_memory\", ";
  protected final String TEXT_176 = ");" + NL + "        String storeDir_";
  protected final String TEXT_177 = " = \"\";" + NL + "        org.neo4j.unsafe.batchinsert.BatchInserter inserter_";
  protected final String TEXT_178 = " = null;" + NL + "        org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider indexProvider_";
  protected final String TEXT_179 = " = null;" + NL;
  protected final String TEXT_180 = NL + "        java.util.Map<String, org.neo4j.graphdb.index.Index<org.neo4j.graphdb.Node>> indexNodeMap_";
  protected final String TEXT_181 = " = new java.util.HashMap<String,org.neo4j.graphdb.index.Index<org.neo4j.graphdb.Node>>();";
  protected final String TEXT_182 = NL + "            indexNodeMap_";
  protected final String TEXT_183 = ".put(";
  protected final String TEXT_184 = ", databaseService_";
  protected final String TEXT_185 = ".index().forNodes(";
  protected final String TEXT_186 = "));";
  protected final String TEXT_187 = NL + "            org.neo4j.graphdb.index.AutoIndexer<org.neo4j.graphdb.Node> autoIndexer_";
  protected final String TEXT_188 = " = databaseService_";
  protected final String TEXT_189 = ".index().getNodeAutoIndexer();" + NL + "            autoIndexer_";
  protected final String TEXT_190 = ".setEnabled(true);";
  protected final String TEXT_191 = NL + "            autoIndexer_";
  protected final String TEXT_192 = ".startAutoIndexingProperty(\"";
  protected final String TEXT_193 = "\");";
  protected final String TEXT_194 = NL + NL;
  protected final String TEXT_195 = NL + "int nb_line_";
  protected final String TEXT_196 = " = 0;" + NL + "long counter_";
  protected final String TEXT_197 = " = 0;";
  protected final String TEXT_198 = NL + "\tresourceMap.put(\"databaseService_";
  protected final String TEXT_199 = "\", databaseService_";
  protected final String TEXT_200 = ");";
  protected final String TEXT_201 = NL;

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

    
class Neo4JOutputUtil {
    private Neo4JTransactionHelper neo4JTransactionHelper;
    private INode node;
    private String cid;
    private boolean batchImport;
    private String dbConnectionName;
    private String dbVersion;
    private boolean useExistingConnection;
    private boolean remoteServer;

    public Neo4JOutputUtil(INode node) {
        this.node = node;
        this.cid = node.getUniqueName();
        this.neo4JTransactionHelper = new Neo4JTransactionHelper(node);

        this.batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
        this.useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));

        if (useExistingConnection) {
            String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
            this.dbConnectionName = "dbService_" + connection; 
            for(INode targetNode : node.getProcess().getNodesOfType("tNeo4jConnection")){
                if (targetNode.getUniqueName().equals(connection)) {
                    this.dbVersion = ElementParameterParser.getValue(targetNode, "__DB_VERSION__");
                    this.remoteServer = "true".equalsIgnoreCase(ElementParameterParser.getValue(targetNode, "__REMOTE_SERVER__"));
                    break;
                }
            }
        } else {
            this.dbConnectionName = "dbService_" + cid;
            this.dbVersion = ElementParameterParser.getValue(node,"__DB_VERSION__");
            this.remoteServer = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REMOTE_SERVER__"));
        }
    }


    public void createDatabaseService() {
        if (this.useExistingConnection) {
            
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_39);
    
        } else {
            if (this.remoteServer) {
                String serverUrl = ElementParameterParser.getValue(node, "__SERVER_URL__");
                boolean setUsername = ElementParameterParser.getBooleanValue(node, "__SET_USERNAME__");
                String username = ElementParameterParser.getValue(node, "__USERNAME__");
                String passwordFieldName = "__PASSWORD__";
                if ((setUsername) || ((!"NEO4J_1_X_X".equals(dbVersion)) && (!"NEO4J_2_1_X".equals(dbVersion)))) {
                    
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_42);
    } else {
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_45);
    }
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(serverUrl);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    
                } else {
                    
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(serverUrl);
    stringBuffer.append(TEXT_53);
    
                }
            } else {
                String databasePath = ElementParameterParser.getValue(node, "__DATABASE_PATH__");
                
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(databasePath);
    stringBuffer.append(TEXT_57);
    
            }
        }
    }

    private void writeBatchModeVersion1_9(Set<String> indexNames, List<String> autoIndexedColumns) {
        
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    
        for (String indexName : indexNames) {
            
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_79);
    
        }

        // Auto index configuration
        if (autoIndexedColumns.size() > 0) {
            
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    
        }
        for (String autoIndexedColumn : autoIndexedColumns) {
            
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(autoIndexedColumn);
    stringBuffer.append(TEXT_86);
    
        }
        neo4JTransactionHelper.startTransaction();
    }

    private void writeBatchModeVersion2_1(Set<String> indexNames, List<String> autoIndexedColumns) {
        
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    
        for (String indexName : indexNames) {
            
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_119);
    
        }

        // There is no autoindex in batch mode, so we did it by hand.
        if (autoIndexedColumns.size() > 0) {
            
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    
        }
    }

    private void writeBatchModeSpring(Set<String> indexNames, List<String> autoIndexedColumns) {
        
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(dbConnectionName);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(dbConnectionName);
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
    
        for (String indexName : indexNames) {
            
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_158);
    
        }

        // There is no autoindex in batch mode, so we did it by hand.
        if (autoIndexedColumns.size() > 0) {
            
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    
        }
    }

    private void writeBatchMode(Set<String> indexNames, List<String> autoIndexedColumns) {
        String nodeMappedMemory = ElementParameterParser.getValue(node, "__NODE_MAPPED_MEMORY__");
        String relationshipMappedMemory = ElementParameterParser.getValue(node, "__RELATIONSHIP_MAPPED_MEMORY__");
        String propertyMappedMemory = ElementParameterParser.getValue(node, "__PROPERTY_MAPPED_MEMORY__");
        String stringsMappedMemory = ElementParameterParser.getValue(node, "__STRINGS_MAPPED_MEMORY__");
        String arraysMappedMemory = ElementParameterParser.getValue(node, "__ARRAYS_MAPPED_MEMORY__");
        
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(nodeMappedMemory);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(relationshipMappedMemory);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(propertyMappedMemory);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(stringsMappedMemory);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(arraysMappedMemory);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_179);
    
        if ("NEO4J_1_X_X".equals(dbVersion)) {
            writeBatchModeVersion1_9(indexNames, autoIndexedColumns);
        } else if (("NEO4J_2_1_X".equals(dbVersion)) || ("NEO4J_2_2_X".equals(dbVersion))) { // NEO4J_2_X before moving to spring framework
            writeBatchModeVersion2_1(indexNames, autoIndexedColumns);
        } else { // NEO4J_2_3 and more using spring framework
            writeBatchModeSpring(indexNames, autoIndexedColumns);
        }
    }

    private void writeNormalMode(Set<String> indexNames, List<String> autoIndexedColumns) {
        
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    
        neo4JTransactionHelper.startEmbeddedTransaction();

        for (String indexName : indexNames) {
            
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_186);
    
        }

        neo4JTransactionHelper.closeEmbeddedTransaction();

        // Auto index configuration
        if (autoIndexedColumns.size() > 0) {
            
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    
        }
        for (String autoIndexedColumn : autoIndexedColumns) {
            
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(autoIndexedColumn);
    stringBuffer.append(TEXT_193);
    
        }
        neo4JTransactionHelper.startTransaction();
    }
}

    stringBuffer.append(TEXT_194);
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();

    boolean batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));
    List<Map<String, String>> values = (List<Map<String, String>>) ElementParameterParser.getObjectValue(node, "__VALUES__");

    List<String> autoIndexedColumns = new ArrayList<String>();
    Set<String> indexNames = new HashSet();
    for (Map<String, String> value : values) {
    	boolean autoIndexed = Boolean.valueOf(value.get("AUTO_INDEXED"));
   		if (autoIndexed) {
   			autoIndexedColumns.add(value.get("SCHEMA_COLUMN"));
   		}

   		String strIndexNames = value.get("INDEX_NAMES");
   		if (strIndexNames != null && !strIndexNames.isEmpty()) {
   			indexNames.addAll(Arrays.asList(strIndexNames.split(",")));
   		}
    }

    // Add update/delete index
    String action = (String) ElementParameterParser.getObjectValue(node, "__DATA_ACTION__");
    if (!action.equals("INSERT")) {
    	indexNames.add((String) ElementParameterParser.getObjectValue(node, "__INDEX_NAME__"));
    }

    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));

    Neo4JOutputUtil neo4JOutputUtil = new Neo4JOutputUtil(node);

    neo4JOutputUtil.createDatabaseService();

    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_197);
    
	if (!batchImport && (!useExistingConnection||(useExistingConnection && shutdownDb))) {

    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_200);
    
	}
    // Index configuration
    if (batchImport) {
        neo4JOutputUtil.writeBatchMode(indexNames, autoIndexedColumns);
    } else {
        neo4JOutputUtil.writeNormalMode(indexNames, autoIndexedColumns);
    }

    stringBuffer.append(TEXT_201);
    return stringBuffer.toString();
  }
}
