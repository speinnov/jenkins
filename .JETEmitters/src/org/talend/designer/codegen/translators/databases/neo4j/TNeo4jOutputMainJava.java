package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.designer.neo4j.data.Neo4jData;
import org.talend.designer.neo4j.data.Index;
import org.talend.designer.neo4j.data.Relationship;
import org.talend.designer.neo4j.ui.editor.MetadataColumnExt;
import java.util.List;

public class TNeo4jOutputMainJava
{
  protected static String nl;
  public static synchronized TNeo4jOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jOutputMainJava result = new TNeo4jOutputMainJava();
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
  protected final String TEXT_37 = NL + "        org.neo4j.graphdb.Node node_";
  protected final String TEXT_38 = " = indexNodeMap_";
  protected final String TEXT_39 = ".get(";
  protected final String TEXT_40 = ").get(";
  protected final String TEXT_41 = ", ";
  protected final String TEXT_42 = ".";
  protected final String TEXT_43 = ").getSingle();" + NL + "        if (node_";
  protected final String TEXT_44 = " != null) {" + NL + "            if (indexNodeMap_";
  protected final String TEXT_45 = ".get(";
  protected final String TEXT_46 = ").isWriteable()) {" + NL + "                indexNodeMap_";
  protected final String TEXT_47 = ".get(";
  protected final String TEXT_48 = ").remove(node_";
  protected final String TEXT_49 = ");" + NL + "            }" + NL + "            if (node_";
  protected final String TEXT_50 = ".hasRelationship()) {" + NL + "                for (org.neo4j.graphdb.Relationship relationship_";
  protected final String TEXT_51 = " : node_";
  protected final String TEXT_52 = ".getRelationships(org.neo4j.graphdb.Direction.BOTH)) {" + NL + "                    relationship_";
  protected final String TEXT_53 = ".delete();" + NL + "                }" + NL + "            }" + NL + "            node_";
  protected final String TEXT_54 = ".delete();" + NL + "        }";
  protected final String TEXT_55 = NL + "        java.util.Map<String, org.neo4j.graphdb.Node> endNode_";
  protected final String TEXT_56 = " = new java.util.HashMap<String,org.neo4j.graphdb.Node>();";
  protected final String TEXT_57 = NL + "            if (";
  protected final String TEXT_58 = ".";
  protected final String TEXT_59 = " != null) {" + NL + "                java.util.Date date_";
  protected final String TEXT_60 = " = (java.util.Date) ";
  protected final String TEXT_61 = ".";
  protected final String TEXT_62 = ";" + NL + "                node_";
  protected final String TEXT_63 = ".setProperty(\"";
  protected final String TEXT_64 = "\", date_";
  protected final String TEXT_65 = ".getTime());" + NL + "            }";
  protected final String TEXT_66 = NL + "                if (";
  protected final String TEXT_67 = ".";
  protected final String TEXT_68 = " != null) {" + NL + "                    node_";
  protected final String TEXT_69 = ".setProperty(\"";
  protected final String TEXT_70 = "\", ";
  protected final String TEXT_71 = ".";
  protected final String TEXT_72 = ");" + NL + "                }";
  protected final String TEXT_73 = NL + "                node_";
  protected final String TEXT_74 = ".setProperty(\"";
  protected final String TEXT_75 = "\", ";
  protected final String TEXT_76 = ".";
  protected final String TEXT_77 = ");";
  protected final String TEXT_78 = "                          " + NL + "            indexNodeMap_";
  protected final String TEXT_79 = ".get(";
  protected final String TEXT_80 = ").add(node_";
  protected final String TEXT_81 = ", ";
  protected final String TEXT_82 = ", ";
  protected final String TEXT_83 = ".";
  protected final String TEXT_84 = ");                     ";
  protected final String TEXT_85 = NL + "            indexNodeMap_";
  protected final String TEXT_86 = ".get(";
  protected final String TEXT_87 = ").putIfAbsent(node_";
  protected final String TEXT_88 = ", ";
  protected final String TEXT_89 = ", ";
  protected final String TEXT_90 = ".";
  protected final String TEXT_91 = ");";
  protected final String TEXT_92 = NL + "            indexNodeMap_";
  protected final String TEXT_93 = ".get(";
  protected final String TEXT_94 = ").putIfAbsent(node_";
  protected final String TEXT_95 = ", ";
  protected final String TEXT_96 = ", ";
  protected final String TEXT_97 = ");";
  protected final String TEXT_98 = NL + "            indexNodeMap_";
  protected final String TEXT_99 = ".get(";
  protected final String TEXT_100 = ").add(node_";
  protected final String TEXT_101 = ", ";
  protected final String TEXT_102 = ", ";
  protected final String TEXT_103 = ");             ";
  protected final String TEXT_104 = NL + "            endNode_";
  protected final String TEXT_105 = ".put(";
  protected final String TEXT_106 = ", indexNodeMap_";
  protected final String TEXT_107 = ".get(";
  protected final String TEXT_108 = ").get(";
  protected final String TEXT_109 = ", ";
  protected final String TEXT_110 = ").getSingle());";
  protected final String TEXT_111 = NL + "            if (";
  protected final String TEXT_112 = ".";
  protected final String TEXT_113 = " != null) {" + NL + "                endNode_";
  protected final String TEXT_114 = ".put(";
  protected final String TEXT_115 = ", indexNodeMap_";
  protected final String TEXT_116 = ".get(";
  protected final String TEXT_117 = ").get(";
  protected final String TEXT_118 = ", ";
  protected final String TEXT_119 = ".";
  protected final String TEXT_120 = ").getSingle());" + NL + "            }";
  protected final String TEXT_121 = NL + "            endNode_";
  protected final String TEXT_122 = ".put(";
  protected final String TEXT_123 = ", indexNodeMap_";
  protected final String TEXT_124 = ".get(";
  protected final String TEXT_125 = ").get(";
  protected final String TEXT_126 = ", ";
  protected final String TEXT_127 = ".";
  protected final String TEXT_128 = ").getSingle());";
  protected final String TEXT_129 = NL + "        if (endNode_";
  protected final String TEXT_130 = ".get(";
  protected final String TEXT_131 = ") != null) {";
  protected final String TEXT_132 = NL + "                java.util.Iterator<org.neo4j.graphdb.Relationship> relationShipIterator_";
  protected final String TEXT_133 = " = node_";
  protected final String TEXT_134 = ".getRelationships().iterator();" + NL + "                while(relationShipIterator_";
  protected final String TEXT_135 = ".hasNext()) {" + NL + "                    org.neo4j.graphdb.Relationship relation_";
  protected final String TEXT_136 = " = ((org.neo4j.graphdb.Relationship) relationShipIterator_";
  protected final String TEXT_137 = ".next());" + NL + "                    if(relation_";
  protected final String TEXT_138 = ".getOtherNode(node_";
  protected final String TEXT_139 = ").equals(endNode_";
  protected final String TEXT_140 = ".get(";
  protected final String TEXT_141 = "))) {" + NL + "                        relation_";
  protected final String TEXT_142 = ".delete();" + NL + "                    }" + NL + "                }";
  protected final String TEXT_143 = NL + "                    node_";
  protected final String TEXT_144 = ".createRelationshipTo(endNode_";
  protected final String TEXT_145 = ".get(";
  protected final String TEXT_146 = "), org.neo4j.graphdb.DynamicRelationshipType.withName(";
  protected final String TEXT_147 = "));";
  protected final String TEXT_148 = NL + "                    endNode_";
  protected final String TEXT_149 = ".get(";
  protected final String TEXT_150 = ").createRelationshipTo(node_";
  protected final String TEXT_151 = ", org.neo4j.graphdb.DynamicRelationshipType.withName(";
  protected final String TEXT_152 = "));";
  protected final String TEXT_153 = NL + "        }";
  protected final String TEXT_154 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_155 = " = databaseService_";
  protected final String TEXT_156 = ".createNode();" + NL + "\t\t";
  protected final String TEXT_157 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_158 = " = indexNodeMap_";
  protected final String TEXT_159 = ".get(";
  protected final String TEXT_160 = ").get(";
  protected final String TEXT_161 = ", ";
  protected final String TEXT_162 = ".";
  protected final String TEXT_163 = ").getSingle();" + NL + "\t\tif (node_";
  protected final String TEXT_164 = " == null) {" + NL + "\t\t\tnode_";
  protected final String TEXT_165 = " = databaseService_";
  protected final String TEXT_166 = ".createNode();" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_167 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_168 = " = indexNodeMap_";
  protected final String TEXT_169 = ".get(";
  protected final String TEXT_170 = ").get(";
  protected final String TEXT_171 = ", ";
  protected final String TEXT_172 = ".";
  protected final String TEXT_173 = ").getSingle();" + NL + "\t\tif (node_";
  protected final String TEXT_174 = " == null) {" + NL + "\t\t\tthrow new RuntimeException(\"Node not found\");" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_175 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_176 = " = databaseService_";
  protected final String TEXT_177 = ".createNode();";
  protected final String TEXT_178 = NL + "            org.neo4j.graphdb.Label label_";
  protected final String TEXT_179 = " = org.neo4j.graphdb.DynamicLabel.label(";
  protected final String TEXT_180 = ");" + NL + "    \t\tnode_";
  protected final String TEXT_181 = ".addLabel(label_";
  protected final String TEXT_182 = ");" + NL + "    \t\t";
  protected final String TEXT_183 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_184 = " = indexNodeMap_";
  protected final String TEXT_185 = ".get(";
  protected final String TEXT_186 = ").get(";
  protected final String TEXT_187 = ", ";
  protected final String TEXT_188 = ".";
  protected final String TEXT_189 = ").getSingle();" + NL + "\t\tif (node_";
  protected final String TEXT_190 = " == null) {" + NL + "\t\t\tnode_";
  protected final String TEXT_191 = " = databaseService_";
  protected final String TEXT_192 = ".createNode();" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_193 = NL + "            org.neo4j.graphdb.Label label_";
  protected final String TEXT_194 = " = org.neo4j.graphdb.DynamicLabel.label(";
  protected final String TEXT_195 = ");" + NL + "            node_";
  protected final String TEXT_196 = ".addLabel(label_";
  protected final String TEXT_197 = ");";
  protected final String TEXT_198 = NL + "\t\torg.neo4j.graphdb.Node node_";
  protected final String TEXT_199 = " = indexNodeMap_";
  protected final String TEXT_200 = ".get(";
  protected final String TEXT_201 = ").get(";
  protected final String TEXT_202 = ", ";
  protected final String TEXT_203 = ".";
  protected final String TEXT_204 = ").getSingle();" + NL + "\t\tif (node_";
  protected final String TEXT_205 = " == null) {" + NL + "\t\t\tthrow new RuntimeException(\"Node not found\");" + NL + "\t\t}" + NL + "\t\t";
  protected final String TEXT_206 = NL + "            org.neo4j.graphdb.Label label_";
  protected final String TEXT_207 = " = org.neo4j.graphdb.DynamicLabel.label(";
  protected final String TEXT_208 = ");" + NL + "            node_";
  protected final String TEXT_209 = ".addLabel(label_";
  protected final String TEXT_210 = ");";
  protected final String TEXT_211 = NL + "    \t\tif (";
  protected final String TEXT_212 = ".";
  protected final String TEXT_213 = " != null) {" + NL + "    \t\t\tjava.util.Date date_";
  protected final String TEXT_214 = " = (java.util.Date) ";
  protected final String TEXT_215 = ".";
  protected final String TEXT_216 = ";" + NL + "    \t\t\tproperties_";
  protected final String TEXT_217 = ".put(\"";
  protected final String TEXT_218 = "\", date_";
  protected final String TEXT_219 = ".getTime());" + NL + "    \t\t}" + NL + "    \t\t";
  protected final String TEXT_220 = NL + "\t    \t\tif (";
  protected final String TEXT_221 = ".";
  protected final String TEXT_222 = " != null) {" + NL + "\t    \t\t\tproperties_";
  protected final String TEXT_223 = ".put(\"";
  protected final String TEXT_224 = "\", ";
  protected final String TEXT_225 = ".";
  protected final String TEXT_226 = ");" + NL + "\t    \t\t}" + NL + "\t\t    \t";
  protected final String TEXT_227 = NL + "\t    \t\tproperties_";
  protected final String TEXT_228 = ".put(\"";
  protected final String TEXT_229 = "\", ";
  protected final String TEXT_230 = ".";
  protected final String TEXT_231 = ");" + NL + "\t    \t\t";
  protected final String TEXT_232 = NL + "\t\t\tindexProperties_";
  protected final String TEXT_233 = ".put(";
  protected final String TEXT_234 = ", ";
  protected final String TEXT_235 = ".";
  protected final String TEXT_236 = ");" + NL + "\t\t\t";
  protected final String TEXT_237 = NL + "\t\t\tindexProperties_";
  protected final String TEXT_238 = ".put(";
  protected final String TEXT_239 = ", ";
  protected final String TEXT_240 = ");" + NL + "\t\t\t";
  protected final String TEXT_241 = NL + "\t\tindexPropertiesMap_";
  protected final String TEXT_242 = ".put(";
  protected final String TEXT_243 = ", indexProperties_";
  protected final String TEXT_244 = ");" + NL + "\t\t";
  protected final String TEXT_245 = NL + "\t\t\tindexInserterMap_";
  protected final String TEXT_246 = ".get(";
  protected final String TEXT_247 = ").updateOrAdd(node_";
  protected final String TEXT_248 = ", indexPropertiesMap_";
  protected final String TEXT_249 = ".get(";
  protected final String TEXT_250 = "));" + NL + "\t\t\t";
  protected final String TEXT_251 = NL + "\t\t\tindexInserterMap_";
  protected final String TEXT_252 = ".get(";
  protected final String TEXT_253 = ").add(node_";
  protected final String TEXT_254 = ", indexPropertiesMap_";
  protected final String TEXT_255 = ".get(";
  protected final String TEXT_256 = "));\t\t\t" + NL + "\t\t\t";
  protected final String TEXT_257 = NL + "\t\tendNode_";
  protected final String TEXT_258 = ".put(";
  protected final String TEXT_259 = ", 0L);" + NL + "\t\t";
  protected final String TEXT_260 = NL + "\t\t\tendNode_";
  protected final String TEXT_261 = ".put(";
  protected final String TEXT_262 = ", indexInserterMap_";
  protected final String TEXT_263 = ".get(";
  protected final String TEXT_264 = ").get(";
  protected final String TEXT_265 = ", ";
  protected final String TEXT_266 = ").getSingle());" + NL + "\t\t";
  protected final String TEXT_267 = NL + "\t\t\tif (";
  protected final String TEXT_268 = ".";
  protected final String TEXT_269 = " != null) {" + NL + "\t\t\t\tendNode_";
  protected final String TEXT_270 = ".put(";
  protected final String TEXT_271 = ", indexInserterMap_";
  protected final String TEXT_272 = ".get(";
  protected final String TEXT_273 = ").get(";
  protected final String TEXT_274 = ", ";
  protected final String TEXT_275 = ".";
  protected final String TEXT_276 = ").getSingle());" + NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_277 = NL + "\t\t\tendNode_";
  protected final String TEXT_278 = ".put(";
  protected final String TEXT_279 = ", indexInserterMap_";
  protected final String TEXT_280 = ".get(";
  protected final String TEXT_281 = ").get(";
  protected final String TEXT_282 = ", ";
  protected final String TEXT_283 = ".";
  protected final String TEXT_284 = ").getSingle());" + NL + "\t\t\t";
  protected final String TEXT_285 = NL + "\t\t\t\tinserter_";
  protected final String TEXT_286 = ".createRelationship(node_";
  protected final String TEXT_287 = ", endNode_";
  protected final String TEXT_288 = ".get(";
  protected final String TEXT_289 = "), org.neo4j.graphdb.DynamicRelationshipType.withName(";
  protected final String TEXT_290 = "), null);" + NL + "\t\t\t\t";
  protected final String TEXT_291 = NL + "\t\t\t\tinserter_";
  protected final String TEXT_292 = ".createRelationship(endNode_";
  protected final String TEXT_293 = ".get(";
  protected final String TEXT_294 = "), node_";
  protected final String TEXT_295 = ", org.neo4j.graphdb.DynamicRelationshipType.withName(";
  protected final String TEXT_296 = "), null);" + NL + "\t\t\t\t";
  protected final String TEXT_297 = NL + "        java.util.Map<String, Object> properties_";
  protected final String TEXT_298 = " = new java.util.HashMap<String, Object>();";
  protected final String TEXT_299 = NL + "        long node_";
  protected final String TEXT_300 = " = inserter_";
  protected final String TEXT_301 = ".createNode(properties_";
  protected final String TEXT_302 = ");" + NL + "        java.util.Map<String, Long> endNode_";
  protected final String TEXT_303 = " = new java.util.HashMap<String,Long>();" + NL + "        java.util.Map<String, Object> indexProperties_";
  protected final String TEXT_304 = " = new java.util.HashMap<String, Object>();" + NL + "        java.util.Map<String, java.util.Map<String, Object>> indexPropertiesMap_";
  protected final String TEXT_305 = " = new java.util.HashMap<String, java.util.Map<String, Object>>();";
  protected final String TEXT_306 = NL + "\t\tjava.util.Map<String, Object> properties_";
  protected final String TEXT_307 = " = new java.util.HashMap<String, Object>();" + NL + "\t\t";
  protected final String TEXT_308 = NL + "                    autoindex_props_";
  protected final String TEXT_309 = ".put(\"";
  protected final String TEXT_310 = "\", ";
  protected final String TEXT_311 = ".";
  protected final String TEXT_312 = ");" + NL + "                    autoindex_";
  protected final String TEXT_313 = ".add(autoindex_id_";
  protected final String TEXT_314 = ", autoindex_props_";
  protected final String TEXT_315 = ");" + NL + "                    autoindex_";
  protected final String TEXT_316 = ".flush();";
  protected final String TEXT_317 = NL + "            org.neo4j.graphdb.Label label_";
  protected final String TEXT_318 = " = org.neo4j.graphdb.DynamicLabel.label(";
  protected final String TEXT_319 = ");" + NL + "            long node_";
  protected final String TEXT_320 = " = inserter_";
  protected final String TEXT_321 = ".createNode(properties_";
  protected final String TEXT_322 = ", label_";
  protected final String TEXT_323 = ");";
  protected final String TEXT_324 = NL + "            long node_";
  protected final String TEXT_325 = " = inserter_";
  protected final String TEXT_326 = ".createNode(properties_";
  protected final String TEXT_327 = ");";
  protected final String TEXT_328 = NL + "\t\tjava.util.Map<String, Long> endNode_";
  protected final String TEXT_329 = " = new java.util.HashMap<String,Long>();" + NL + "\t\tjava.util.Map<String, Object> indexProperties_";
  protected final String TEXT_330 = " = new java.util.HashMap<String, Object>();" + NL + "\t\tjava.util.Map<String, java.util.Map<String, Object>> indexPropertiesMap_";
  protected final String TEXT_331 = " = new java.util.HashMap<String, java.util.Map<String, Object>>();" + NL + "\t\t";
  protected final String TEXT_332 = NL + "\t    \t";
  protected final String TEXT_333 = ".";
  protected final String TEXT_334 = " = ";
  protected final String TEXT_335 = ".";
  protected final String TEXT_336 = ";" + NL + "\t    \t";
  protected final String TEXT_337 = NL + NL + "counter_";
  protected final String TEXT_338 = "++;" + NL;
  protected final String TEXT_339 = NL + NL + "nb_line_";
  protected final String TEXT_340 = "++;";

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

    
class Neo4jUtils {

    private List<IMetadataColumn> columnList;
    private String dataAction;

    protected String incomingConnName;
    protected String cid;
    protected INode node;

    public Neo4jUtils(String cid, INode node,String dataAction, String incomingConnName, List<IMetadataColumn> columnList) {
        this.cid = cid;
        this.incomingConnName = incomingConnName;
        this.columnList = columnList;
        this.node = node;
        this.dataAction = dataAction;
    }

    public void generateNodeInsert() {
        // to be overrided
    }

    public void generateNodeInsertOrUpdate() {
	 	// to be overrided
    }

    public void generateNodeUpdate() {
	 	// to be overrided
    }

    public void generateNodeDelete() {
        String indexName = ElementParameterParser.getValue(node, "__INDEX_NAME__");
        String indexKey = ElementParameterParser.getValue(node, "__INDEX_KEY__");
        String indexColumnValue = ElementParameterParser.getValue(node, "__INDEX_VALUE__");
        
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(indexKey);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(indexColumnValue);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(indexName);
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
    
    }

    protected void generateNode() {

    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    
        for (IMetadataColumn column : columnList) {             
            if (column instanceof MetadataColumnExt) {                  
                MetadataColumnExt columnExt = (MetadataColumnExt) column;                   
                Neo4jData data = columnExt.getData();
                generateNodePropertyInsert(column);
                for (Index index : data.getIndexes()) {
                    generateIndexCreation(index, column);
                }
                for (Relationship relationship : data.getRelationships()) {
                    generateRelationshipCreation(relationship, column);
                }
            }
        }
    }

    private void generateNodePropertyInsert(IMetadataColumn column) {
        if (column.getTalendType().equals("id_Date")) {
            
    stringBuffer.append(TEXT_57);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    
        } else {
            if (column.isNullable()) {
                
    stringBuffer.append(TEXT_66);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_72);
    
            } else {
                
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_77);
    
            }
        }
    }


    private void generateIndexCreation(Index index, IMetadataColumn column) {
        boolean fromValueColumn = index.getValue() == null || index.getValue().isEmpty();
        if (fromValueColumn) {
            
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_82);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_84);
    
        } else if (fromValueColumn && index.isUnique()){
            
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_89);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_91);
    
        } else if (index.isUnique()) {
            
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_96);
    stringBuffer.append(index.getValue());
    stringBuffer.append(TEXT_97);
    
        } else {
            
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_102);
    stringBuffer.append(index.getValue());
    stringBuffer.append(TEXT_103);
    
        }
    }

    protected void generateRelationshipCreation(Relationship relationship, IMetadataColumn column) {
        if (relationship.getIndex().getValue() != null && !relationship.getIndex().getValue().isEmpty()) {
        
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_108);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_109);
    stringBuffer.append(relationship.getIndex().getValue());
    stringBuffer.append(TEXT_110);
    
        } else if (column.isNullable()) {
        
    stringBuffer.append(TEXT_111);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_117);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_118);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_120);
    
        } else {
            
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_125);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_126);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_128);
    
        }
        
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_131);
    
            if(dataAction.equals("UPDATE") || dataAction.equals("INSERT_OR_UPDATE")) {

    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    
            }
            switch (relationship.getDirection()) {
                case OUTGOING :
                    
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_146);
    stringBuffer.append(relationship.getType());
    stringBuffer.append(TEXT_147);
    
                    break;
                case INCOMING :
                    
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(relationship.getType());
    stringBuffer.append(TEXT_152);
    
                    break;
            }
            
    stringBuffer.append(TEXT_153);
    
    }
}

    
class Neo4jUtils1_9 extends Neo4jUtils {

    public Neo4jUtils1_9(String cid, INode node,String dataAction, String incomingConnName, List<IMetadataColumn> columnList) {
		super(cid, node, dataAction, incomingConnName, columnList);
	}

	public void generateNodeInsert() {
		
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_156);
    
		generateNode();
	}

	public void generateNodeInsertOrUpdate() {
		String indexName = ElementParameterParser.getValue(node, "__INDEX_NAME__");
		String indexKey = ElementParameterParser.getValue(node, "__INDEX_KEY__");
		String indexColumnValue = ElementParameterParser.getValue(node, "__INDEX_VALUE__");
		
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(indexKey);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(indexColumnValue);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    
		generateNode();
	}

	public void generateNodeUpdate() {
		String indexName = ElementParameterParser.getValue(node, "__INDEX_NAME__");
		String indexKey = ElementParameterParser.getValue(node, "__INDEX_KEY__");
		String indexColumnValue = ElementParameterParser.getValue(node, "__INDEX_VALUE__");
		
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(indexKey);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(indexColumnValue);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_174);
    
		generateNode();
	}

}

     
class Neo4jUtils2_0 extends Neo4jUtils {
    private boolean useLabel;
    private String neo4jLabel;

    public Neo4jUtils2_0(String cid, INode node, String dataAction, String incomingConnName, List<IMetadataColumn> columnList, boolean useLabel, String neo4jLabel) {
		super(cid, node, dataAction, incomingConnName, columnList);

        this.useLabel = useLabel;
        this.neo4jLabel = neo4jLabel;
	}

	public void generateNodeInsert() {
	    
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_177);
    
        if (useLabel) {
            
    stringBuffer.append(TEXT_178);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_179);
    stringBuffer.append(neo4jLabel);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    
	    }
		generateNode();
	}

	public void generateNodeInsertOrUpdate() {
		String indexName = ElementParameterParser.getValue(node, "__INDEX_NAME__");
		String indexKey = ElementParameterParser.getValue(node, "__INDEX_KEY__");
		String indexColumnValue = ElementParameterParser.getValue(node, "__INDEX_VALUE__");
		
    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(indexKey);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(indexColumnValue);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    
        if (useLabel) {
            
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(neo4jLabel);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_197);
    
        }
        generateNode();
	}

	public void generateNodeUpdate() {
		String indexName = ElementParameterParser.getValue(node, "__INDEX_NAME__");
		String indexKey = ElementParameterParser.getValue(node, "__INDEX_KEY__");
		String indexColumnValue = ElementParameterParser.getValue(node, "__INDEX_VALUE__");
		
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(indexName);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(indexKey);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(indexColumnValue);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_205);
    
		if (useLabel) {
            
    stringBuffer.append(TEXT_206);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(neo4jLabel);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    
        }
        generateNode();
	}
}

    
class Neo4jBatchUtils {

    protected List<IMetadataColumn> columnList;
    protected String incomingConnName;
    protected String cid;
    protected INode node;

	public Neo4jBatchUtils(String cid, INode node, String incomingConnName, List<IMetadataColumn> columnList) {
	    this.cid = cid;
		this.incomingConnName = incomingConnName;
		this.columnList = columnList;
		this.node = node;
	}

	public void generateNodeBatchInsert() {
        // To be overrided

	}

	protected void putBatchPropertyInsertInMap(IMetadataColumn column) {
		if (column.getTalendType().equals("id_Date")) {
    		
    stringBuffer.append(TEXT_211);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_219);
    
		} else {
	    	if (column.isNullable()) {
		    	
    stringBuffer.append(TEXT_220);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_222);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_226);
    
	    	} else {
		    	
    stringBuffer.append(TEXT_227);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_231);
    
	    	}
	    }
	}

	protected void generateNode() {
        // To be overrided
	}

	protected void generateIndexCreation(Index index, IMetadataColumn column) {
		boolean fromValueColumn = index.getValue() == null || index.getValue().isEmpty();
		if (fromValueColumn) {
			
    stringBuffer.append(TEXT_232);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_233);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_234);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_236);
    
		} else {
			
    stringBuffer.append(TEXT_237);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_238);
    stringBuffer.append(index.getKey());
    stringBuffer.append(TEXT_239);
    stringBuffer.append(index.getValue());
    stringBuffer.append(TEXT_240);
    
		}
		
    stringBuffer.append(TEXT_241);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_243);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_244);
    

		if (index.isUnique()) {
			
    stringBuffer.append(TEXT_245);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_247);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_250);
    
		} else {
			
    stringBuffer.append(TEXT_251);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_253);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(index.getName());
    stringBuffer.append(TEXT_256);
    
		}
	}

	protected void generateRelationshipCreation(Relationship relationship, IMetadataColumn column) {
		
    stringBuffer.append(TEXT_257);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_259);
    
		if (relationship.getIndex().getValue() != null && !relationship.getIndex().getValue().isEmpty()) {
		
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_264);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_265);
    stringBuffer.append(relationship.getIndex().getValue());
    stringBuffer.append(TEXT_266);
    
		} else if (column.isNullable()) {
		
    stringBuffer.append(TEXT_267);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_269);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_270);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_271);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_273);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_274);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_275);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_276);
    
		} else {
			
    stringBuffer.append(TEXT_277);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_279);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_281);
    stringBuffer.append(relationship.getIndex().getKey());
    stringBuffer.append(TEXT_282);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_284);
    
		}
		switch (relationship.getDirection()) {
			case OUTGOING :
				
    stringBuffer.append(TEXT_285);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_286);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_288);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_289);
    stringBuffer.append(relationship.getType());
    stringBuffer.append(TEXT_290);
    
				break;
			case INCOMING :
				
    stringBuffer.append(TEXT_291);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_293);
    stringBuffer.append(relationship.getIndex().getName());
    stringBuffer.append(TEXT_294);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_295);
    stringBuffer.append(relationship.getType());
    stringBuffer.append(TEXT_296);
    
				break;
		}
	}
}

    
class Neo4jBatchUtils1_9 extends Neo4jBatchUtils {

    public Neo4jBatchUtils1_9(String cid, INode node, String incomingConnName, List<IMetadataColumn> columnList) {
				super(cid, node, incomingConnName, columnList);
    }

    @Override
    public void generateNodeBatchInsert() {
        
    stringBuffer.append(TEXT_297);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_298);
    
        for (IMetadataColumn column : columnList) {
            putBatchPropertyInsertInMap(column);
        }
        // Must compute all properties before node creation for best performances
        generateNode();
        for (IMetadataColumn column : columnList) {
            if (column instanceof MetadataColumnExt) {
                MetadataColumnExt columnExt = (MetadataColumnExt) column;
                Neo4jData data = columnExt.getData();
                for (Index index : data.getIndexes()) {
                    generateIndexCreation(index, column);
                }
                for (Relationship relationship : data.getRelationships()) {
                    generateRelationshipCreation(relationship, column);
                }
            }
        }
    }

    @Override
    protected void generateNode() {
        
    stringBuffer.append(TEXT_299);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_300);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_301);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_302);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_303);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_304);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_305);
    
    }


}

    
class Neo4jBatchUtils2_0 extends Neo4jBatchUtils {

    private boolean useLabel;
    private String neo4jLabel;

	public Neo4jBatchUtils2_0(String cid, INode node, String incomingConnName, List<IMetadataColumn> columnList, boolean useLabel, String neo4jLabel) {
		super(cid, node, incomingConnName, columnList);
        this.useLabel = useLabel;
        this.neo4jLabel = neo4jLabel;
	}

	public void generateNodeBatchInsert() {
		
    stringBuffer.append(TEXT_306);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_307);
    
		for (IMetadataColumn column : columnList) {
    		putBatchPropertyInsertInMap(column);
	    }
	    // Must compute all properties before node creation for best performances
	    generateNode();
	    for (IMetadataColumn column : columnList) {
	    	if (column instanceof MetadataColumnExt) {
	    		MetadataColumnExt columnExt = (MetadataColumnExt) column;
	    		Neo4jData data = columnExt.getData();
                if (data.isAutoIndexed()) {
                    
    stringBuffer.append(TEXT_308);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_309);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_310);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_311);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_313);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_315);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_316);
    
                }
	    		for (Index index : data.getIndexes()) {
	    			generateIndexCreation(index, column);
	    		}
	    		for (Relationship relationship : data.getRelationships()) {
	    			generateRelationshipCreation(relationship, column);
	    		}
	    	}
    	}
	}

	protected void generateNode() {
        if (useLabel) {
            
    stringBuffer.append(TEXT_317);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_318);
    stringBuffer.append(neo4jLabel);
    stringBuffer.append(TEXT_319);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_320);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_321);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_322);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_323);
    
        } else {
            
    stringBuffer.append(TEXT_324);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_326);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_327);
    
        }
		
    stringBuffer.append(TEXT_328);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_329);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_330);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_331);
    
	}
}

    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    final INode node = (INode)codeGenArgument.getArgument();
    final String cid = node.getUniqueName();
    final String dataAction = ElementParameterParser.getValue(node, "__DATA_ACTION__");
    String commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__");
    boolean batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
    boolean useLabel = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_LABEL__"));
    String neo4jLabel = ElementParameterParser.getValue(node, "__LABEL_NAME__");
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
    String dbVersion = ElementParameterParser.getValue(node,"__DB_VERSION__");
    Neo4JTransactionHelper neo4JTransactionHelper = new Neo4JTransactionHelper(node);

    if (useExistingConnection) {
        String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
        List<? extends INode> nodes = node.getProcess().getGeneratingNodes();
        for(INode targetNode : nodes){
            if (targetNode.getUniqueName().equals(connection)) {
                dbVersion = ElementParameterParser.getValue(targetNode, "__DB_VERSION__");
                break;
            }
        }
    }

    List<IMetadataColumn> columnList = null;
    List<IMetadataTable> metadatas = node.getMetadataList();
    
    if (metadatas != null && metadatas.size() > 0) {
    	IMetadataTable metadata = metadatas.get(0);
    	if (metadata != null) {
    		columnList = metadata.getListColumns();
    	}
    }	
    
    List<? extends IConnection> inputs = node.getIncomingConnections();
    List<? extends IConnection> outputs = node.getOutgoingSortedConnections();
    String incomingConnName = null;
    String outgoingConnName = null;
    
    if (inputs.size() > 0) {
    	IConnection in = inputs.get(0);
    	incomingConnName = in.getName();
    }
    
    if (outputs.size() > 0) {
    	IConnection out = outputs.get(0);
    	outgoingConnName = out.getName();
    }
    for (IMetadataColumn column : columnList) {
	    if (outgoingConnName != null) {
	    	
    stringBuffer.append(TEXT_332);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_333);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(column);
    stringBuffer.append(TEXT_336);
    
		}
    }

    if (batchImport) {
        Neo4jBatchUtils neo4jBatchUtils;
        if ("NEO4J_1_X_X".equals(dbVersion)) {
            neo4jBatchUtils = new Neo4jBatchUtils1_9(cid, node, incomingConnName, columnList);
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            neo4jBatchUtils  = new Neo4jBatchUtils2_0(cid, node, incomingConnName, columnList, useLabel, neo4jLabel);
        }

        neo4jBatchUtils.generateNodeBatchInsert();
    } else {
        Neo4jUtils neo4jUtils;
        if ("NEO4J_1_X_X".equals(dbVersion)) {
            neo4jUtils = new Neo4jUtils1_9(cid, node, dataAction, incomingConnName, columnList);
        } else { // NEO4J_2_1_X or NEO4J_2_2_X
            neo4jUtils  = new Neo4jUtils2_0(cid, node, dataAction, incomingConnName, columnList, useLabel, neo4jLabel);
        }
	    if (dataAction.equals("INSERT")) {
		    neo4jUtils.generateNodeInsert();
		} else if (dataAction.equals("INSERT_OR_UPDATE")) {
			neo4jUtils.generateNodeInsertOrUpdate();
		} else if (dataAction.equals("UPDATE")) {
			neo4jUtils.generateNodeUpdate();
		} else if (dataAction.equals("DELETE")) {
			neo4jUtils.generateNodeDelete();
		}
	}

    stringBuffer.append(TEXT_337);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_338);
    
neo4JTransactionHelper.restartTransaction();

    stringBuffer.append(TEXT_339);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_340);
    return stringBuffer.toString();
  }
}
