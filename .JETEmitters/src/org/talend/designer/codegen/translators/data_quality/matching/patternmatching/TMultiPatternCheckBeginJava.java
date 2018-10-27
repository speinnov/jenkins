package org.talend.designer.codegen.translators.data_quality.matching.patternmatching;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.IMetadataTable;
import java.util.List;

public class TMultiPatternCheckBeginJava
{
  protected static String nl;
  public static synchronized TMultiPatternCheckBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMultiPatternCheckBeginJava result = new TMultiPatternCheckBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "  " + NL + "    int nb_line_";
  protected final String TEXT_2 = " = 0;" + NL + "    int nb_line_ok_";
  protected final String TEXT_3 = " = 0;" + NL + "    int nb_line_reject_";
  protected final String TEXT_4 = " = 0;";
  protected final String TEXT_5 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

List<IMetadataTable> metadatas = node.getMetadataList();

if ((metadatas != null) && (metadatas.size() > 0)) {
  IMetadataTable metadata = metadatas.get(0);
  
  if (metadata != null) {
  
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    
  }
}

    stringBuffer.append(TEXT_5);
    return stringBuffer.toString();
  }
}
