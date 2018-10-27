package org.talend.designer.codegen.translators.data_quality.matching;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.Map;
import java.util.List;
import org.talend.core.model.process.ElementParameterParser;

public class TFuzzyUniqRowBeginJava
{
  protected static String nl;
  public static synchronized TFuzzyUniqRowBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TFuzzyUniqRowBeginJava result = new TFuzzyUniqRowBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\tjava.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap_";
  protected final String TEXT_3 = " = (java.util.concurrent.ConcurrentHashMap) globalMap.get(\"concurrentHashMap\");";
  protected final String TEXT_4 = NL + "\t\tjava.util.List<String> ";
  protected final String TEXT_5 = "_list_";
  protected final String TEXT_6 = " = new java.util.ArrayList<String>();";
  protected final String TEXT_7 = NL + NL + "java.util.List<Integer> UID_list_";
  protected final String TEXT_8 = " = new java.util.ArrayList<Integer>();" + NL + "" + NL + "concurrentHashMap_";
  protected final String TEXT_9 = ".putIfAbsent(\"nb_uniques_";
  protected final String TEXT_10 = "\", new java.util.concurrent.atomic.AtomicInteger(0));" + NL + "java.util.concurrent.atomic.AtomicInteger nb_uniques_";
  protected final String TEXT_11 = " = (java.util.concurrent.atomic.AtomicInteger) concurrentHashMap_";
  protected final String TEXT_12 = ".get(\"nb_uniques_";
  protected final String TEXT_13 = "\");" + NL + "" + NL + "concurrentHashMap_";
  protected final String TEXT_14 = ".putIfAbsent(\"nb_duplicates_";
  protected final String TEXT_15 = "\",new java.util.concurrent.atomic.AtomicInteger(0));" + NL + "java.util.concurrent.atomic.AtomicInteger nb_duplicates_";
  protected final String TEXT_16 = " = (java.util.concurrent.atomic.AtomicInteger) concurrentHashMap_";
  protected final String TEXT_17 = ".get(\"nb_duplicates_";
  protected final String TEXT_18 = "\");" + NL + "" + NL + "concurrentHashMap_";
  protected final String TEXT_19 = ".putIfAbsent(\"nb_line_";
  protected final String TEXT_20 = "\",new java.util.concurrent.atomic.AtomicInteger(0));" + NL + "java.util.concurrent.atomic.AtomicInteger nb_line_";
  protected final String TEXT_21 = " = (java.util.concurrent.atomic.AtomicInteger) concurrentHashMap_";
  protected final String TEXT_22 = ".get(\"nb_line_";
  protected final String TEXT_23 = "\");" + NL + "" + NL + "int dId_";
  protected final String TEXT_24 = " = 0;";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();
String onlyOnceEachDuplicatedKey = ElementParameterParser.getValue(node, "__ONLY_ONCE_EACH_DUPLICATED_KEY__");
List<Map<String, String>> keyColumns = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__UNIQUE_KEY__");


    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    

for(Map<String, String> keyColumn:keyColumns){
	if(keyColumn.get("KEY_ATTRIBUTE").equals("true")){
		String colName = keyColumn.get("SCHEMA_COLUMN");

    stringBuffer.append(TEXT_4);
    stringBuffer.append(colName );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_6);
    
	}
}

    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_24);
    return stringBuffer.toString();
  }
}
