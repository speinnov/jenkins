package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;
import java.util.List;

public class TNeo4jRowEndJava
{
  protected static String nl;
  public static synchronized TNeo4jRowEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jRowEndJava result = new TNeo4jRowEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        if (counter_";
  protected final String TEXT_2 = " % Long.parseLong(";
  protected final String TEXT_3 = ") > 0) {" + NL + "            tx_";
  protected final String TEXT_4 = ".success();" + NL + "            tx_";
  protected final String TEXT_5 = ".finish();" + NL + "        }";
  protected final String TEXT_6 = NL + "        try {" + NL + "            tx_";
  protected final String TEXT_7 = ".success();" + NL + "        } finally {" + NL + "            tx_";
  protected final String TEXT_8 = ".close();" + NL + "        }";
  protected final String TEXT_9 = NL + "\tdatabaseService_";
  protected final String TEXT_10 = ".shutdown();" + NL + "\tresourceMap.put(\"finish_";
  protected final String TEXT_11 = "\", true); ";
  protected final String TEXT_12 = NL + "\tif(engine_";
  protected final String TEXT_13 = "!=null){" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_14 = "_NB_NODE_INSERTED\", nbNodeInserted_";
  protected final String TEXT_15 = ");" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_16 = "_NB_RELATIONSHIP_INSERTED\", nbRelationshipInserted_";
  protected final String TEXT_17 = ");" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_18 = "_NB_PROPERTY_UPDATED\", nbPropertiesUpdated_";
  protected final String TEXT_19 = ");" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_20 = "_NB_NODE_DELETED\", nbNodeDeleted_";
  protected final String TEXT_21 = ");" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_22 = "_NB_RELATIONSHIP_DELETED\", nbRelationshipDeleted_";
  protected final String TEXT_23 = ");" + NL + "\t}" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));
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
    if ("NEO4J_1_X_X".equals(dbVersion)) {
        
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(commitEvery);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
    } else { // NEO4J_2_1_X or NEO4J_2_2_X
        
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
    }
	if (!useExistingConnection||(useExistingConnection && shutdownDb)) {

    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
	}

    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    return stringBuffer.toString();
  }
}
