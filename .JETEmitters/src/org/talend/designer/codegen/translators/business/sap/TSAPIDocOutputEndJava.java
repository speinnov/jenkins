package org.talend.designer.codegen.translators.business.sap;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.Map;
import java.util.List;

public class TSAPIDocOutputEndJava
{
  protected static String nl;
  public static synchronized TSAPIDocOutputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPIDocOutputEndJava result = new TSAPIDocOutputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\tif(connection_";
  protected final String TEXT_3 = "!=null && connection_";
  protected final String TEXT_4 = ".isAlive()) {" + NL + "\t\tconnection_";
  protected final String TEXT_5 = ".close();" + NL + "\t}";
  protected final String TEXT_6 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	boolean fromXML = "true".equals(ElementParameterParser.getValue(node, "__FROM_XML__"));
	
	if(fromXML) {
		return "";
	}
	
	boolean useExistingConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
	
	if(!useExistingConn) {

    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
	}

    stringBuffer.append(TEXT_6);
    return stringBuffer.toString();
  }
}
