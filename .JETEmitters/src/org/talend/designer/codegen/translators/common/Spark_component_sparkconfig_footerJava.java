package org.talend.designer.codegen.translators.common;

import org.talend.core.model.process.INode;
import org.talend.core.model.temp.ECodePart;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class Spark_component_sparkconfig_footerJava
{
  protected static String nl;
  public static synchronized Spark_component_sparkconfig_footerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Spark_component_sparkconfig_footerJava result = new Spark_component_sparkconfig_footerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "/**" + NL + " * [";
  protected final String TEXT_2 = " ";
  protected final String TEXT_3 = " ] stop" + NL + " */";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	ECodePart codePart = codeGenArgument.getCodePart();

    stringBuffer.append(TEXT_1);
    stringBuffer.append(node.getUniqueName() );
    stringBuffer.append(TEXT_2);
    stringBuffer.append(codePart );
    stringBuffer.append(TEXT_3);
    return stringBuffer.toString();
  }
}
