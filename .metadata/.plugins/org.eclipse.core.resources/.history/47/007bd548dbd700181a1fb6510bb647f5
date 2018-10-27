package org.talend.designer.codegen.translators.common;

import java.util.List;
import java.util.Vector;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IProcess;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class Header_additionalJava
{
  protected static String nl;
  public static synchronized Header_additionalJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Header_additionalJava result = new Header_additionalJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = "\t" + NL + "\t" + NL + "\t" + NL + "\t// These fields are defined for \"Petals\"  " + NL + "\tprivate org.w3c.dom.Document petalsOutMessage = null;" + NL + "\t" + NL + "\tpublic org.w3c.dom.Document getPetalsOutMessage() {" + NL + "\t\treturn this.petalsOutMessage;" + NL + "\t}" + NL + "\t" + NL + "\tprivate org.w3c.dom.Document petalsInMessage = null;" + NL + "\t" + NL + "\tpublic void setPetalsInMessage(org.w3c.dom.Document message) {" + NL + "\t\tthis.petalsInMessage = message;" + NL + "\t}" + NL + "" + NL + "\t// End of Petals fields" + NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
boolean withPetals = false;

CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
Vector v = (Vector) codeGenArgument.getArgument();
IProcess process = (IProcess)v.get(0);

List<? extends INode> tPetalsInputList = process.getNodesOfType("tPetalsInput");
List<? extends INode> tPetalsOutputList = process.getNodesOfType("tPetalsOutput");

if((tPetalsInputList != null && tPetalsInputList.size() > 0) ||  (tPetalsOutputList != null && tPetalsOutputList.size() > 0)){
	withPetals = true;
}

if(withPetals){

    stringBuffer.append(TEXT_2);
    
}

    return stringBuffer.toString();
  }
}
