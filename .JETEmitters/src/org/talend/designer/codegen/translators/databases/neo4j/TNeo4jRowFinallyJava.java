package org.talend.designer.codegen.translators.databases.neo4j;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;

public class TNeo4jRowFinallyJava
{
  protected static String nl;
  public static synchronized TNeo4jRowFinallyJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TNeo4jRowFinallyJava result = new TNeo4jRowFinallyJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\tif(resourceMap.get(\"finish_";
  protected final String TEXT_2 = "\") == null){" + NL + "\t\t\tif(resourceMap.get(\"databaseService_";
  protected final String TEXT_3 = "\") != null){" + NL + "\t\t\t\t((org.neo4j.graphdb.GraphDatabaseService)resourceMap.get(\"databaseService_";
  protected final String TEXT_4 = "\")).shutdown();" + NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_5 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
    boolean useExistingConnection = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"));
    boolean shutdownDb = "true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__SHUTDOWN_DB__"));
    String commitEvery = ElementParameterParser.getValue(node, "__COMMIT_EVERY__"); 
	if (!useExistingConnection||(useExistingConnection && shutdownDb)) {

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    
	}

    stringBuffer.append(TEXT_5);
    return stringBuffer.toString();
  }
}
