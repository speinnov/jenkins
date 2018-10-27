package org.talend.designer.codegen.translators.data_quality;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TLastRegexlibExpressionsEndJava
{
  protected static String nl;
  public static synchronized TLastRegexlibExpressionsEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TLastRegexlibExpressionsEndJava result = new TLastRegexlibExpressionsEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t}" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();

String cid = node.getUniqueName();

    stringBuffer.append(TEXT_2);
    return stringBuffer.toString();
  }
}
