package org.talend.designer.codegen.translators.internet.esb;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TPetalsInputEndJava
{
  protected static String nl;
  public static synchronized TPetalsInputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TPetalsInputEndJava result = new TPetalsInputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t}\t// End of loop" + NL + "}\t// End of condition" + NL + "" + NL + "globalMap.put(\"";
  protected final String TEXT_2 = "_NB_LINE\",nb_line_";
  protected final String TEXT_3 = ");";
  protected final String TEXT_4 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();

String cid = node.getUniqueName();

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(TEXT_4);
    return stringBuffer.toString();
  }
}
