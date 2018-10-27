package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class TNeo4jOutputFinallyJava
{
  protected static String nl;
  public static synchronized TNeo4jOutputFinallyJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jOutputFinallyJava result = new TNeo4jOutputFinallyJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\tif(resourceMap.get(\"finish_";
  protected final String TEXT_2 = "\") == null){" + NL + "\t\t\tif(resourceMap.get(\"databaseService_";
  protected final String TEXT_3 = "\") != null){" + NL + "\t\t\t\t((org.neo4j.graphdb.GraphDatabaseService)resourceMap.get(\"databaseService_";
  protected final String TEXT_4 = "\")).shutdown();" + NL + "\t\t\t}" + NL + "\t\t}" + NL + "\t";
  protected final String TEXT_5 = NL + "\t\tif(resourceMap.get(\"finish_";
  protected final String TEXT_6 = "\") == null){" + NL + "\t\t\tif(resourceMap.get(\"indexProvider_";
  protected final String TEXT_7 = "\") != null){" + NL + "\t\t\t\t((org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider)resourceMap.get(\"indexProvider_";
  protected final String TEXT_8 = "\")).shutdown();" + NL + "\t\t\t}" + NL + "\t\t\tif(resourceMap.get(\"inserter_";
  protected final String TEXT_9 = "\") != null){" + NL + "\t\t\t\t((org.neo4j.unsafe.batchinsert.BatchInserter)resourceMap.get(\"inserter_";
  protected final String TEXT_10 = "\")).shutdown();" + NL + "\t\t\t}" + NL + "\t\t}" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
    String commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__"); 
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
    boolean batchImport = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__BATCH_IMPORT__"));
    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));
    String connection = ElementParameterParser.getValue(node, "__CONNECTION__");
    String dbconn = "dbService_" + connection;
    List<Map<String, String>> values = (List<Map<String, String>>) ElementParameterParser.getObjectValue(node, "__VALUES__");

	if (!batchImport && (!useExistingConnection||(useExistingConnection && shutdownDb))) {
	
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    
	}
	if (batchImport) {
	// Must shutdown database to prevent data corruption after batch importation
	
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
    
	}

    return stringBuffer.toString();
  }
}
