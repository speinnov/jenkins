package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public class TNeo4jRowMainJava
{
  protected static String nl;
  public static synchronized TNeo4jRowMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jRowMainJava result = new TNeo4jRowMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    java.util.Map<String, Object> parameters_";
  protected final String TEXT_2 = " = new java.util.HashMap<String, Object>();";
  protected final String TEXT_3 = NL + "    \tparameters_";
  protected final String TEXT_4 = ".put(";
  protected final String TEXT_5 = ", ";
  protected final String TEXT_6 = ".";
  protected final String TEXT_7 = ");" + NL + "    \t";
  protected final String TEXT_8 = NL + "\torg.neo4j.cypher.ExecutionResult result_";
  protected final String TEXT_9 = " = null;" + NL + "    try {" + NL + "    \tif(engine_";
  protected final String TEXT_10 = "!=null){" + NL + "    \t\tresult_";
  protected final String TEXT_11 = " = engine_";
  protected final String TEXT_12 = ".execute(";
  protected final String TEXT_13 = ", parameters_";
  protected final String TEXT_14 = ");" + NL + "    \t}" + NL + "    \tif(queryEngine_";
  protected final String TEXT_15 = "!=null){" + NL + "    \t\tqueryEngine_";
  protected final String TEXT_16 = ".query(";
  protected final String TEXT_17 = ", parameters_";
  protected final String TEXT_18 = ");" + NL + "    \t}" + NL + "    } catch (Exception e_";
  protected final String TEXT_19 = ") {" + NL + "    \t";
  protected final String TEXT_20 = NL + "    \t\tthrow(e_";
  protected final String TEXT_21 = ");" + NL + "    \t\t";
  protected final String TEXT_22 = NL + "    \t\t\t";
  protected final String TEXT_23 = " = new ";
  protected final String TEXT_24 = "Struct();" + NL + "    \t\t\t";
  protected final String TEXT_25 = NL + "    \t\t\t\t\t";
  protected final String TEXT_26 = ".";
  protected final String TEXT_27 = " = ";
  protected final String TEXT_28 = ".";
  protected final String TEXT_29 = ";" + NL + "    \t\t\t\t\t";
  protected final String TEXT_30 = NL + "    \t\t\t";
  protected final String TEXT_31 = ".errorMessage = e_";
  protected final String TEXT_32 = ".getMessage() + \" - Line: \" + tos_count_";
  protected final String TEXT_33 = ";" + NL + "    \t\t\t";
  protected final String TEXT_34 = NL + "    \t\t\tSystem.err.print(e_";
  protected final String TEXT_35 = ".getMessage());" + NL + "    \t\t\t";
  protected final String TEXT_36 = NL + "    }" + NL + "" + NL + "\tif(result_";
  protected final String TEXT_37 = "!=null) {" + NL + "\t\tnbNodeInserted_";
  protected final String TEXT_38 = " += result_";
  protected final String TEXT_39 = ".queryStatistics().nodesCreated();" + NL + "\t\tnbRelationshipInserted_";
  protected final String TEXT_40 = " += result_";
  protected final String TEXT_41 = ".queryStatistics().relationshipsCreated();" + NL + "\t\tnbPropertiesUpdated_";
  protected final String TEXT_42 = " += result_";
  protected final String TEXT_43 = ".queryStatistics().propertiesSet();" + NL + "\t\t";
  protected final String TEXT_44 = NL + "            nbNodeDeleted_";
  protected final String TEXT_45 = " += result_";
  protected final String TEXT_46 = ".queryStatistics().deletedNodes();" + NL + "            nbRelationshipDeleted_";
  protected final String TEXT_47 = " += result_";
  protected final String TEXT_48 = ".queryStatistics().deletedRelationships();" + NL + "\t        ";
  protected final String TEXT_49 = NL + "            nbNodeDeleted_";
  protected final String TEXT_50 = " += result_";
  protected final String TEXT_51 = ".queryStatistics().nodesDeleted();" + NL + "            nbRelationshipDeleted_";
  protected final String TEXT_52 = " += result_";
  protected final String TEXT_53 = ".queryStatistics().relationshipsDeleted();" + NL + "\t        ";
  protected final String TEXT_54 = NL + "\t}" + NL + "\t\t" + NL + "\tcounter_";
  protected final String TEXT_55 = "++;" + NL + "" + NL + "\tif (counter_";
  protected final String TEXT_56 = " % Long.parseLong(";
  protected final String TEXT_57 = ") == 0) {" + NL + "\t    ";
  protected final String TEXT_58 = NL + "            tx_";
  protected final String TEXT_59 = ".success();" + NL + "            tx_";
  protected final String TEXT_60 = ".finish();";
  protected final String TEXT_61 = NL + "            try {" + NL + "                tx_";
  protected final String TEXT_62 = ".success();" + NL + "            } finally {" + NL + "                tx_";
  protected final String TEXT_63 = ".close();" + NL + "            }" + NL + "" + NL + "            if (databaseService_";
  protected final String TEXT_64 = ".getClass().equals(org.neo4j.kernel.EmbeddedGraphDatabase.class)) {" + NL + "                while (((org.neo4j.kernel.EmbeddedGraphDatabase)databaseService_";
  protected final String TEXT_65 = ").transactionRunning()){" + NL + "                    // wait the end of the previous transaction" + NL + "                }" + NL + "            }";
  protected final String TEXT_66 = NL + "            try {" + NL + "                tx_";
  protected final String TEXT_67 = ".success();" + NL + "            } finally {" + NL + "                tx_";
  protected final String TEXT_68 = ".close();" + NL + "            }" + NL + "" + NL + "            if (databaseService_";
  protected final String TEXT_69 = ".getClass().equals(org.neo4j.kernel.EmbeddedGraphDatabase.class)) {" + NL + "                ((org.neo4j.kernel.EmbeddedGraphDatabase)databaseService_";
  protected final String TEXT_70 = ").isAvailable(10000l);" + NL + "            }";
  protected final String TEXT_71 = NL + NL + "\t\ttx_";
  protected final String TEXT_72 = " = databaseService_";
  protected final String TEXT_73 = ".beginTx();" + NL + "\t}";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
    
    String query = ElementParameterParser.getValue(node, "__QUERY__");
    query = query.replaceAll("\n"," ");
    query = query.replaceAll("\r"," ");
    List<Map<String, String>> listParameters = (List<Map<String, String>>) ElementParameterParser.getObjectValue(node, "__PARAMETERS__");
    boolean dieOnError = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__DIE_ON_ERROR__"));   
    String commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__"); 

    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
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
    
    String rejectConnName = null;
    List<? extends IConnection> rejectConns = node.getOutgoingConnections("REJECT");
    if (rejectConns != null && rejectConns.size() > 0) {
    	IConnection rejectConn = rejectConns.get(0);
    	rejectConnName = rejectConn.getName();
    }
    
    List<IMetadataColumn> rejectColumnList = null;
    IMetadataTable metadataTable = node.getMetadataFromConnector("REJECT");
    if (metadataTable != null) {
    	rejectColumnList = metadataTable.getListColumns();
    }
    
    Set<String> inputCols = new HashSet();
	if ((metadatas!=null)&&(metadatas.size()>0)) {
		IMetadataTable metadata = metadatas.get(0);
		if (metadata!=null) {
			List< ? extends IConnection> conns = node.getIncomingConnections();
			columnList = metadata.getListColumns();
			if(conns != null && conns.size()>0){
				IConnection conn = conns.get(0);
				if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
					incomingConnName = conn.getName();
					IMetadataTable inputMetadataTable = conn.getMetadataTable();
					for (IMetadataColumn inputCol : inputMetadataTable.getListColumns()) {
						inputCols.add(inputCol.getLabel());
					}
				}
			}
		}
	}
    
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    
    for (Map<String, String> parameter : listParameters) {
    	
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(parameter.get("PARAMETER_NAME"));
    stringBuffer.append(TEXT_5);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(parameter.get("PARAMETER_VALUE"));
    stringBuffer.append(TEXT_7);
    
    }
    
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    
    	if (dieOnError) {
    		
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    
    	} else {
    		if (rejectConnName != null && rejectColumnList != null && rejectColumnList.size() > 0) {
    			
    stringBuffer.append(TEXT_22);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_24);
    
    			if (incomingConnName != null) {
    				for (IMetadataColumn column : columnList) {
    					if (inputCols.contains(column.getLabel())) {
    					
    stringBuffer.append(TEXT_25);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_27);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_29);
    
    					}
    				}
    			}
    			
    stringBuffer.append(TEXT_30);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_33);
    
    		} else {
    			
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    
    		}
    	}
    	
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    
	    if ("NEO4J_1_X_X".equals(dbVersion)) {
	        
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    
	    } else { // NEO4J_2_1_X or NEO4J_2_2_X
	        
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
	    }
	    
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_57);
    
        if ("NEO4J_1_X_X".equals(dbVersion)) {
            
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
        } else if ("NEO4J_2_1_X".equals(dbVersion)) { // NEO4J_2_1_X
            
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    
        } else { // NEO4J_2_2_X : usage of Spring rest
            
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    
        }
        
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    return stringBuffer.toString();
  }
}
