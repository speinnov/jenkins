package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TQASAddressIncompleteEndJava
{
  protected static String nl;
  public static synchronized TQASAddressIncompleteEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TQASAddressIncompleteEndJava result = new TQASAddressIncompleteEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "globalMap.put(\"";
  protected final String TEXT_3 = "_NB_LINE\",nb_line_";
  protected final String TEXT_4 = ");" + NL + "globalMap.put(\"";
  protected final String TEXT_5 = "_NB_VERIFIED\",nb_verified_";
  protected final String TEXT_6 = ");" + NL + "globalMap.put(\"";
  protected final String TEXT_7 = "_NB_REJECT\",nb_reject_";
  protected final String TEXT_8 = ");";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();

    stringBuffer.append(TEXT_2);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_6);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    return stringBuffer.toString();
  }
}
