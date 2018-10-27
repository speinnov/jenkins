package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;

public class TMongoDBOutputMainJava
{
  protected static String nl;
  public static synchronized TMongoDBOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBOutputMainJava result = new TMongoDBOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t\tcom.mongodb.WriteResult resultMessage_";
  protected final String TEXT_2 = "=null;" + NL + "\t\t\t\tupdateObjectUtil_";
  protected final String TEXT_3 = ".setObject(new com.mongodb.BasicDBObject());" + NL + "" + NL + "\t\t\t\t";
  protected final String TEXT_4 = NL + "\t\t\t\t\tqueryObjectUtil_";
  protected final String TEXT_5 = ".setObject(new com.mongodb.BasicDBObject());" + NL + "\t\t\t\t\tint countKey_";
  protected final String TEXT_6 = "=0;" + NL + "\t\t\t\t";
  protected final String TEXT_7 = NL + "\t                            if (";
  protected final String TEXT_8 = ".";
  protected final String TEXT_9 = " != null) {" + NL + "    \t                            ";
  protected final String TEXT_10 = NL + "    \t                                    queryObjectUtil_";
  protected final String TEXT_11 = ".putkeyNode(pathMap_";
  protected final String TEXT_12 = ".get(\"";
  protected final String TEXT_13 = "\"),\"";
  protected final String TEXT_14 = "\", ";
  protected final String TEXT_15 = ".";
  protected final String TEXT_16 = ");" + NL + "    \t                                    countKey_";
  protected final String TEXT_17 = "++;" + NL + "    \t                                ";
  protected final String TEXT_18 = NL + "                                    updateObjectUtil_";
  protected final String TEXT_19 = ".put(pathMap_";
  protected final String TEXT_20 = ".get(\"";
  protected final String TEXT_21 = "\"),\"";
  protected final String TEXT_22 = "\", ";
  protected final String TEXT_23 = ".";
  protected final String TEXT_24 = ");" + NL + "\t                            }";
  protected final String TEXT_25 = NL + "                                        queryObjectUtil_";
  protected final String TEXT_26 = ".putkeyNode(pathMap_";
  protected final String TEXT_27 = ".get(\"";
  protected final String TEXT_28 = "\"),\"";
  protected final String TEXT_29 = "\", ";
  protected final String TEXT_30 = ".";
  protected final String TEXT_31 = ");" + NL + "                                        countKey_";
  protected final String TEXT_32 = "++;";
  protected final String TEXT_33 = NL + "                                updateObjectUtil_";
  protected final String TEXT_34 = ".put(pathMap_";
  protected final String TEXT_35 = ".get(\"";
  protected final String TEXT_36 = "\"),\"";
  protected final String TEXT_37 = "\", ";
  protected final String TEXT_38 = ".";
  protected final String TEXT_39 = ");";
  protected final String TEXT_40 = NL + "\t\t\t\tcom.mongodb.BasicDBObject updateObj_";
  protected final String TEXT_41 = " = updateObjectUtil_";
  protected final String TEXT_42 = ".getObject();" + NL + "\t\t\t\t";
  protected final String TEXT_43 = NL + "\t\t\t\t\t\tif(bulkWriteOperationCounter_";
  protected final String TEXT_44 = " < bulkWriteOperationSize_";
  protected final String TEXT_45 = "){" + NL + "\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_46 = ".insert(updateObj_";
  protected final String TEXT_47 = ");" + NL + "\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_48 = "++;" + NL + "\t\t\t\t\t\t} else {" + NL + "                            bulkWriteOperation_";
  protected final String TEXT_49 = ".insert(updateObj_";
  protected final String TEXT_50 = ");" + NL + "\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_51 = ".execute();" + NL + "\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_52 = "=1;" + NL + "\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_53 = " = coll_";
  protected final String TEXT_54 = ".initialize";
  protected final String TEXT_55 = "BulkOperation();" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_56 = NL + "\t\t\t\t\t\tresultMessage_";
  protected final String TEXT_57 = "=coll_";
  protected final String TEXT_58 = ".insert(updateObj_";
  protected final String TEXT_59 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_60 = NL + "\t\t\t\t\tif(countKey_";
  protected final String TEXT_61 = " <=0){" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_62 = NL + "\t\t\t\t\t\t\tthrow new Exception(\"Must have at least one key in schema\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_63 = NL + "\t\t\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_64 = " - Must have at least one key in schema\");" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_65 = NL + "\t\t\t\t\t\t\tSystem.err.println(\"Must have at least one key in schema\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_66 = NL + "\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\tcom.mongodb.BasicDBObject queryObj_";
  protected final String TEXT_67 = " = queryObjectUtil_";
  protected final String TEXT_68 = ".getObject();" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_69 = NL + "\t\t\t\t\t\t\t\tif(bulkWriteOperationCounter_";
  protected final String TEXT_70 = " < bulkWriteOperationSize_";
  protected final String TEXT_71 = "){" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_72 = ".find(queryObj_";
  protected final String TEXT_73 = ").replaceOne(updateObj_";
  protected final String TEXT_74 = ");" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_75 = "++;" + NL + "\t\t\t\t\t\t\t\t}else{" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_76 = ".find(queryObj_";
  protected final String TEXT_77 = ").replaceOne(updateObj_";
  protected final String TEXT_78 = ");" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_79 = ".execute();" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_80 = "=1;" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_81 = " = coll_";
  protected final String TEXT_82 = ".initialize";
  protected final String TEXT_83 = "BulkOperation();" + NL + "\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_84 = NL + "\t\t\t\t\t\t\t\tresultMessage_";
  protected final String TEXT_85 = "=coll_";
  protected final String TEXT_86 = ".update(queryObj_";
  protected final String TEXT_87 = ", updateObj_";
  protected final String TEXT_88 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_89 = NL + "                                if(bulkWriteOperationCounter_";
  protected final String TEXT_90 = " < bulkWriteOperationSize_";
  protected final String TEXT_91 = "){" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_92 = ".find(queryObj_";
  protected final String TEXT_93 = ").update(new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_94 = "));" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_95 = "++;" + NL + "                                }else{" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_96 = ".find(queryObj_";
  protected final String TEXT_97 = ").update(new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_98 = "));" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_99 = ".execute();" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_100 = "=1;" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_101 = " = coll_";
  protected final String TEXT_102 = ".initialize";
  protected final String TEXT_103 = "BulkOperation();" + NL + "                                }";
  protected final String TEXT_104 = NL + "                                resultMessage_";
  protected final String TEXT_105 = "=coll_";
  protected final String TEXT_106 = ".update(queryObj_";
  protected final String TEXT_107 = ",new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_108 = "), false, ";
  protected final String TEXT_109 = ");";
  protected final String TEXT_110 = NL + "                                if(bulkWriteOperationCounter_";
  protected final String TEXT_111 = " < bulkWriteOperationSize_";
  protected final String TEXT_112 = "){" + NL + "                                    com.mongodb.BulkWriteRequestBuilder bulkWriteRequestBuilder_";
  protected final String TEXT_113 = " = bulkWriteOperation_";
  protected final String TEXT_114 = ".find(queryObj_";
  protected final String TEXT_115 = ");" + NL + "                                    com.mongodb.BulkUpdateRequestBuilder bulkUpdateRequestBuilder_";
  protected final String TEXT_116 = " = bulkWriteRequestBuilder_";
  protected final String TEXT_117 = ".upsert();" + NL + "                                    bulkUpdateRequestBuilder_";
  protected final String TEXT_118 = ".replaceOne(updateObj_";
  protected final String TEXT_119 = ");" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_120 = "++;" + NL + "                                }else{" + NL + "                                    com.mongodb.BulkWriteRequestBuilder bulkWriteRequestBuilder_";
  protected final String TEXT_121 = " = bulkWriteOperation_";
  protected final String TEXT_122 = ".find(queryObj_";
  protected final String TEXT_123 = ");" + NL + "                                    com.mongodb.BulkUpdateRequestBuilder bulkUpdateRequestBuilder_";
  protected final String TEXT_124 = " = bulkWriteRequestBuilder_";
  protected final String TEXT_125 = ".upsert();" + NL + "                                    bulkUpdateRequestBuilder_";
  protected final String TEXT_126 = ".replaceOne(updateObj_";
  protected final String TEXT_127 = ");" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_128 = ".execute();" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_129 = "=1;" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_130 = " = coll_";
  protected final String TEXT_131 = ".initialize";
  protected final String TEXT_132 = "BulkOperation();" + NL + "                                }";
  protected final String TEXT_133 = NL + "                                resultMessage_";
  protected final String TEXT_134 = "=coll_";
  protected final String TEXT_135 = ".update(queryObj_";
  protected final String TEXT_136 = ",updateObj_";
  protected final String TEXT_137 = ",true,false);";
  protected final String TEXT_138 = NL + "                                if(bulkWriteOperationCounter_";
  protected final String TEXT_139 = " < bulkWriteOperationSize_";
  protected final String TEXT_140 = "){" + NL + "                                    com.mongodb.BulkWriteRequestBuilder bulkWriteRequestBuilder_";
  protected final String TEXT_141 = " = bulkWriteOperation_";
  protected final String TEXT_142 = ".find(queryObj_";
  protected final String TEXT_143 = ");" + NL + "                                    com.mongodb.BulkUpdateRequestBuilder bulkUpdateRequestBuilder_";
  protected final String TEXT_144 = " = bulkWriteRequestBuilder_";
  protected final String TEXT_145 = ".upsert();" + NL + "                                    bulkUpdateRequestBuilder_";
  protected final String TEXT_146 = ".update(new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_147 = "));" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_148 = "++;" + NL + "                                }else{" + NL + "                                    com.mongodb.BulkWriteRequestBuilder bulkWriteRequestBuilder_";
  protected final String TEXT_149 = " = bulkWriteOperation_";
  protected final String TEXT_150 = ".find(queryObj_";
  protected final String TEXT_151 = ");" + NL + "                                    com.mongodb.BulkUpdateRequestBuilder bulkUpdateRequestBuilder_";
  protected final String TEXT_152 = " = bulkWriteRequestBuilder_";
  protected final String TEXT_153 = ".upsert();" + NL + "                                    bulkUpdateRequestBuilder_";
  protected final String TEXT_154 = ".update(new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_155 = "));" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_156 = ".execute();" + NL + "                                    bulkWriteOperationCounter_";
  protected final String TEXT_157 = "=1;" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_158 = " = coll_";
  protected final String TEXT_159 = ".initialize";
  protected final String TEXT_160 = "BulkOperation();" + NL + "                                }";
  protected final String TEXT_161 = NL + "                                resultMessage_";
  protected final String TEXT_162 = "=coll_";
  protected final String TEXT_163 = ".update(queryObj_";
  protected final String TEXT_164 = ",new com.mongodb.BasicDBObject(\"$set\", updateObj_";
  protected final String TEXT_165 = "),true,";
  protected final String TEXT_166 = ");";
  protected final String TEXT_167 = NL + "\t\t\t\t\t\t\t\tif(bulkWriteOperationCounter_";
  protected final String TEXT_168 = " < bulkWriteOperationSize_";
  protected final String TEXT_169 = "){" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_170 = ".find(queryObj_";
  protected final String TEXT_171 = ").remove();" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_172 = "++;" + NL + "\t\t\t\t\t\t\t\t}else{" + NL + "                                    bulkWriteOperation_";
  protected final String TEXT_173 = ".find(queryObj_";
  protected final String TEXT_174 = ").remove();" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_175 = ".execute();" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperationCounter_";
  protected final String TEXT_176 = "=1;" + NL + "\t\t\t\t\t\t\t\t\tbulkWriteOperation_";
  protected final String TEXT_177 = " = coll_";
  protected final String TEXT_178 = ".initialize";
  protected final String TEXT_179 = "BulkOperation();" + NL + "\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_180 = NL + "\t\t\t\t\t\t\t\tresultMessage_";
  protected final String TEXT_181 = "=coll_";
  protected final String TEXT_182 = ".remove(queryObj_";
  protected final String TEXT_183 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_184 = NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_185 = NL + "    \t\t\t\tif(resultMessage_";
  protected final String TEXT_186 = "!=null && resultMessage_";
  protected final String TEXT_187 = ".getError()!=null){" + NL + "    \t\t\t\t\t";
  protected final String TEXT_188 = NL + "    \t\t\t\t\t\tthrow new Exception(resultMessage_";
  protected final String TEXT_189 = ".getError());" + NL + "    \t\t\t\t\t";
  protected final String TEXT_190 = NL + "    \t\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_191 = " - \" + resultMessage_";
  protected final String TEXT_192 = ".getError());" + NL + "    \t\t\t\t\t\t";
  protected final String TEXT_193 = NL + "    \t\t\t\t\t\tSystem.err.println(resultMessage_";
  protected final String TEXT_194 = ".getError());" + NL + "    \t\t\t\t\t";
  protected final String TEXT_195 = NL + "    \t\t\t\t}" + NL + "    \t\t\t\t";
  protected final String TEXT_196 = NL + "\t\t\t\tnb_line_";
  protected final String TEXT_197 = " ++;" + NL + "\t\t\t\t";
  protected final String TEXT_198 = NL + "\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_199 = " - [";
  protected final String TEXT_200 = "] - Writing the record \" + nb_line_";
  protected final String TEXT_201 = " + \" into database.\");" + NL + "\t\t\t\t";
  protected final String TEXT_202 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	String dataAction = ElementParameterParser.getValue(node,"__DATA_ACTION__");
	boolean dieOnError = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__DIE_ON_ERROR__"));
	List<? extends IConnection> inConns = node.getIncomingConnections(EConnectionType.FLOW_MAIN);
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
    String dbversion = ElementParameterParser.getValue(node, "__DB_VERSION__");
    java.util.List<java.util.Map<String, String>> mappings = (java.util.List<java.util.Map<String, String>>)ElementParameterParser.getObjectValueXML(node, "__MAPPING__");

    boolean setBulkWrite = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SET_BULK_WRITE__"));
    String bulkWriteOperationSize = ElementParameterParser.getValue(node, "__BULK_WRITE_SIZE__");
    String bulkWriteType = ElementParameterParser.getValue(node, "__BULK_WRITE_TYPE__");
    // BulkWrite is only supportted on Mongo 2.6+ drivers
    final boolean bulkWrite = setBulkWrite & (!"MONGODB_2_5_X".equalsIgnoreCase(dbversion));
    boolean updateAll = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__UPDATE_ALL__"));

	IConnection inConn = null;
	IMetadataTable metadata = null;
	
	if(inConns!=null && inConns.size()> 0) {
		inConn = inConns.get(0);
		metadata = inConn.getMetadataTable();
	}

	if (metadata!=null) {
		List< ? extends IConnection> conns = node.getIncomingConnections();
		for (IConnection conn : conns) {
			if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
				List<IMetadataColumn> columnList = metadata.getListColumns();
				int sizeColumns = columnList.size();

				// Create the BasicDBObject
				
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
				if(!"INSERT".equalsIgnoreCase(dataAction)){
				
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    
				}
				// Set key/values of the BasicObject
				for (int i = 0; i < sizeColumns; i++) {
					IMetadataColumn column = columnList.get(i);
					
					for (java.util.Map<String, String> mapping: mappings) {
					    if (column.getLabel().equals(mapping.get("SCHEMA_COLUMN"))) {
	                        if ("true".equals(mapping.get("REMOVE_NULL_FIELD"))) {
	                            
    stringBuffer.append(TEXT_7);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_9);
    
    	                            if (!"INSERT".equalsIgnoreCase(dataAction) && column.isKey()){
    	                                
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_13);
    stringBuffer.append(column.getOriginalDbColumnName() );
    stringBuffer.append(TEXT_14);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    
                                    }
                                    
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(column.getOriginalDbColumnName() );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_23);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_24);
    
                            } else {
                                if (!"INSERT".equalsIgnoreCase(dataAction) && column.isKey()){
                                    
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(column.getOriginalDbColumnName() );
    stringBuffer.append(TEXT_29);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_30);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
                                }
                                
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_36);
    stringBuffer.append(column.getOriginalDbColumnName() );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_39);
    
                            }
                            break;
                        }
		            }
  				}
  				// Get the BasicDBObject
				
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    

				// INSERT operation
				if ("INSERT".equalsIgnoreCase(dataAction)) {
					// Bulk Writes
					if(bulkWrite){
						// Add inserts untill bulksize is reached then execute and re-initialze the bulk operation
						
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
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_55);
    
					// Single Writes
					}else{
					
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    
					}
				// Other operations (UPDATE, UPSERT, DELETE)
				}else{
				
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    
						if(dieOnError){
						
    stringBuffer.append(TEXT_62);
    
						}else{
							if(isLog4jEnabled){
							
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    
							}
							
    stringBuffer.append(TEXT_65);
    
						}
						
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    
						// UPDATE
						if("UPDATE".equalsIgnoreCase(dataAction)){
							// Bulk Updates 
							if(bulkWrite){
								
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_83);
    
							}else{
								
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    
							}
						// SET
						} else if("SET".equalsIgnoreCase(dataAction)){
                            // Bulk Updates
                            if(bulkWrite){
                                
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_103);
    
                            }else{
                            	
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(updateAll);
    stringBuffer.append(TEXT_109);
    
                            }
                        // UPSERT
						} else if("UPSERT".equalsIgnoreCase(dataAction)){
                            // Bulk Upsert
                            if(bulkWrite){
                                
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_132);
    
                            }else{
                                
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    
                            }
                        // UPSERT WITH SET
                        } else if("UPSERT_WITH_SET".equalsIgnoreCase(dataAction)){
                            // Bulk Upsert
                            if(bulkWrite){
                                
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(cid);
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_160);
    
                            }else{
                            	
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(updateAll);
    stringBuffer.append(TEXT_166);
    
                            }
                        // DELETE
                        } else if("DELETE".equalsIgnoreCase(dataAction)){
							// Bulk Delete
							if(bulkWrite){
								
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(bulkWriteType);
    stringBuffer.append(TEXT_179);
    
							}else{
								
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    
							}
						}
						
    stringBuffer.append(TEXT_184);
    
				}

				// Die on Error 
				// Used only with MONGODB 2.5
				// 2.5 doesn't support BulkWrites => No BulkWrites Die and Error
                if (dbversion.equals("MONGODB_2_5_X")) {
                    
    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    
    					if(dieOnError){
    					
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    
    					}else{
    						if(isLog4jEnabled){
    						
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    
    						}
    						
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    
    					}
    					
    stringBuffer.append(TEXT_195);
    
                }

    			
    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_197);
    

				if(isLog4jEnabled){
				
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(dataAction);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_201);
    
				}

			}
		}
	}
	
    stringBuffer.append(TEXT_202);
    return stringBuffer.toString();
  }
}
