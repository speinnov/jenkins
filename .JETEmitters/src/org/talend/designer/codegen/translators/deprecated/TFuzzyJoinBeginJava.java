package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.EConnectionType;
import java.util.List;
import java.util.Map;

public class TFuzzyJoinBeginJava
{
  protected static String nl;
  public static synchronized TFuzzyJoinBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TFuzzyJoinBeginJava result = new TFuzzyJoinBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    final java.util.Map<";
  protected final String TEXT_2 = "Struct, ";
  protected final String TEXT_3 = "Struct> tHash_";
  protected final String TEXT_4 = " = (java.util.Map<";
  protected final String TEXT_5 = "Struct, ";
  protected final String TEXT_6 = "Struct>) globalMap.get(\"tHash_";
  protected final String TEXT_7 = "\");" + NL + "    final java.util.Set< ";
  protected final String TEXT_8 = "Struct> tSet_";
  protected final String TEXT_9 = "_";
  protected final String TEXT_10 = " = tHash_";
  protected final String TEXT_11 = ".keySet();" + NL + "    int nb_main_";
  protected final String TEXT_12 = " = 0;" + NL + "    int nb_reject_";
  protected final String TEXT_13 = " = 0;";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();

List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas!=null)&&(metadatas.size()>0)) {//1
    IMetadataTable metadata = metadatas.get(0);
    String lookupConName = "";
    if (metadata!=null) {//2
    
        String cid = node.getUniqueName();
        String hashName = "";

        //get the input Main and Lookup connection
        IConnection inMainCon = null;
        IConnection inRefCon = null;   
        List< ? extends IConnection> connsIn = node.getIncomingConnections();     
        for (IConnection conn : connsIn) {//3
        	if (conn.getLineStyle().equals(EConnectionType.FLOW_MAIN)) {
        		inMainCon = conn;
        	}
        	else if(conn.getLineStyle().equals(EConnectionType.FLOW_REF))
        	{
        		inRefCon = conn;
        		lookupConName = inRefCon.getName();
        		hashName = lookupConName;
        		if (conn.getSource().getComponent().isHashComponent()) {
        			hashName = ElementParameterParser.getValue(conn.getSource(), "__LIST__");
        			lookupConName = "row2";
        		}
        	}
        }//3 
        
        if(inMainCon == null || inRefCon == null){
        	return "";
        }
        
        String incomingName = inMainCon.getName();
        
        List<Map<String, String>> joinKeys = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__JOIN_KEY__");

    stringBuffer.append(TEXT_1);
    stringBuffer.append(lookupConName );
    stringBuffer.append(TEXT_2);
    stringBuffer.append(lookupConName );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(lookupConName );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(lookupConName );
    stringBuffer.append(TEXT_6);
    stringBuffer.append(hashName );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(lookupConName );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
    
    }//2
}//1

    return stringBuffer.toString();
  }
}
