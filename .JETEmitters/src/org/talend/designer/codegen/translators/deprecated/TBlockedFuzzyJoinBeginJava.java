package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.EConnectionType;
import java.util.List;
import java.util.Map;

public class TBlockedFuzzyJoinBeginJava
{
  protected static String nl;
  public static synchronized TBlockedFuzzyJoinBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TBlockedFuzzyJoinBeginJava result = new TBlockedFuzzyJoinBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        java.util.Map<";
  protected final String TEXT_2 = "Struct, ";
  protected final String TEXT_3 = "Struct> tHash_";
  protected final String TEXT_4 = " = (java.util.Map<";
  protected final String TEXT_5 = "Struct, ";
  protected final String TEXT_6 = "Struct>) globalMap.get(\"tHash_";
  protected final String TEXT_7 = "\");" + NL + "        int nb_matches_";
  protected final String TEXT_8 = " = 0;" + NL + "        int nb_possible_matches_";
  protected final String TEXT_9 = " = 0;" + NL + "        int nb_none_matches_";
  protected final String TEXT_10 = " = 0;";
  protected final String TEXT_11 = NL + NL + NL + NL + NL + NL + NL;
  protected final String TEXT_12 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();    

List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas != null) && (metadatas.size() > 0)) {
  IMetadataTable metadata = metadatas.get(0);
  if (metadata != null){
    List< ? extends IConnection> connsIn = node.getIncomingConnections();
    for (IConnection connIn: connsIn){
      if (connIn.getLineStyle().equals(EConnectionType.FLOW_REF)){
        String connInName = connIn.getName();
        
    stringBuffer.append(TEXT_1);
    stringBuffer.append(connInName);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(connInName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(connInName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(connInName);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(connInName );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    
      }
    }
  }
}

    stringBuffer.append(TEXT_11);
    stringBuffer.append(TEXT_12);
    return stringBuffer.toString();
  }
}
