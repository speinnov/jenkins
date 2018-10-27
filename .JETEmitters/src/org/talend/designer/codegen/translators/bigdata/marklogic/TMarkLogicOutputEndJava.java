package org.talend.designer.codegen.translators.bigdata.marklogic;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TMarkLogicOutputEndJava
{
  protected static String nl;
  public static synchronized TMarkLogicOutputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMarkLogicOutputEndJava result = new TMarkLogicOutputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "       globalMap.put(\"";
  protected final String TEXT_3 = "_NB_LINE\", nb_line_";
  protected final String TEXT_4 = ");" + NL + "       globalMap.put(\"";
  protected final String TEXT_5 = "_NB_LINE_UPSERTED\", nb_line_inserted_";
  protected final String TEXT_6 = ");" + NL + "       globalMap.put(\"";
  protected final String TEXT_7 = "_NB_LINE_DELETED\", nb_line_deleted_";
  protected final String TEXT_8 = ");" + NL + "       globalMap.put(\"";
  protected final String TEXT_9 = "_NB_LINE_PATCHED\", nb_line_updated_";
  protected final String TEXT_10 = ");" + NL + "       globalMap.put(\"";
  protected final String TEXT_11 = "_NB_LINE_REJECTED\", nb_line_rejected_";
  protected final String TEXT_12 = ");" + NL;
  protected final String TEXT_13 = NL + "       client_";
  protected final String TEXT_14 = ".release();";
  protected final String TEXT_15 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();

    boolean reuseConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
    
    stringBuffer.append(TEXT_2);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    
    if(!reuseConn){
    
    stringBuffer.append(TEXT_13);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_14);
    
    }

    stringBuffer.append(TEXT_15);
    return stringBuffer.toString();
  }
}
