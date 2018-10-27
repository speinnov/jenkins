package org.talend.designer.codegen.translators.custom_code;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.runprocess.ProcessorUtilities;
import java.util.List;

public class THadoopConfManagerMrcodeJava
{
  protected static String nl;
  public static synchronized THadoopConfManagerMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    THadoopConfManagerMrcodeJava result = new THadoopConfManagerMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    return stringBuffer.toString();
  }
}
