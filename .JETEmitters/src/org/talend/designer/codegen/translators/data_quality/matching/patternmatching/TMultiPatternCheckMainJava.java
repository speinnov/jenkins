package org.talend.designer.codegen.translators.data_quality.matching.patternmatching;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.utils.ParameterValueUtil;
import java.util.List;
import java.util.Map;

public class TMultiPatternCheckMainJava
{
  protected static String nl;
  public static synchronized TMultiPatternCheckMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMultiPatternCheckMainJava result = new TMultiPatternCheckMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        ";
  protected final String TEXT_2 = "  = null;";
  protected final String TEXT_3 = NL + "    " + NL + "    java.util.Map<String, String> sbMatchResult_";
  protected final String TEXT_4 = " = new java.util.HashMap<String, String>();" + NL + "    java.util.regex.Pattern pattern_";
  protected final String TEXT_5 = " = null;" + NL + "    " + NL + "    try{";
  protected final String TEXT_6 = NL + "            pattern_";
  protected final String TEXT_7 = "=java.util.regex.Pattern.compile(";
  protected final String TEXT_8 = ");";
  protected final String TEXT_9 = NL + "            pattern_";
  protected final String TEXT_10 = " = java.util.regex.Pattern.compile(";
  protected final String TEXT_11 = ", java.util.regex.Pattern.CASE_INSENSITIVE);";
  protected final String TEXT_12 = NL + "\t\t  sbMatchResult_";
  protected final String TEXT_13 = ".put(\"";
  protected final String TEXT_14 = "\", (pattern_";
  protected final String TEXT_15 = ".matcher(";
  protected final String TEXT_16 = ".";
  protected final String TEXT_17 = " + \"\").find() ? \"1\" : \"0\") + \"";
  protected final String TEXT_18 = "\".trim());";
  protected final String TEXT_19 = NL + "    } catch(java.util.regex.PatternSyntaxException pe){" + NL + "      System.err.println(\"Error in component tMultiPatternCheck: the pattern defined contains errors\");" + NL + "    }" + NL + "    nb_line_";
  protected final String TEXT_20 = " ++;" + NL + "    boolean bMatches_";
  protected final String TEXT_21 = " = true;";
  protected final String TEXT_22 = NL + "      if (sbMatchResult_";
  protected final String TEXT_23 = ".size() > 0) {" + NL + "\t    for (String columnName : sbMatchResult_";
  protected final String TEXT_24 = ".keySet()) {" + NL + "\t\t  if (\"0\".equals(sbMatchResult_";
  protected final String TEXT_25 = ".get(columnName).substring(0, 1))) {" + NL + "\t\t    bMatches_";
  protected final String TEXT_26 = " = false;" + NL + "\t\t    break;" + NL + "\t\t  }" + NL + "\t\t}" + NL + "\t  }";
  protected final String TEXT_27 = NL + "      bMatches_";
  protected final String TEXT_28 = " = false;" + NL + "      if (sbMatchResult_";
  protected final String TEXT_29 = ".size() > 0) {" + NL + "\t    for (String columnName : sbMatchResult_";
  protected final String TEXT_30 = ".keySet()) {" + NL + "\t\t  if (\"1\".equals(sbMatchResult_";
  protected final String TEXT_31 = ".get(columnName).substring(0, 1))) {" + NL + "\t\t    bMatches_";
  protected final String TEXT_32 = " = true;" + NL + "\t\t    break;" + NL + "\t\t  }" + NL + "\t\t}" + NL + "\t  }";
  protected final String TEXT_33 = NL + "    " + NL + "    if (bMatches_";
  protected final String TEXT_34 = "){";
  protected final String TEXT_35 = "        ";
  protected final String TEXT_36 = NL + "        ";
  protected final String TEXT_37 = " = new ";
  protected final String TEXT_38 = "Struct();";
  protected final String TEXT_39 = NL + "          ";
  protected final String TEXT_40 = ".";
  protected final String TEXT_41 = " = ";
  protected final String TEXT_42 = ".";
  protected final String TEXT_43 = ";";
  protected final String TEXT_44 = NL + "      nb_line_ok_";
  protected final String TEXT_45 = "++;" + NL + "    } else {";
  protected final String TEXT_46 = NL + "        ";
  protected final String TEXT_47 = " = new ";
  protected final String TEXT_48 = "Struct();";
  protected final String TEXT_49 = NL + "          ";
  protected final String TEXT_50 = ".";
  protected final String TEXT_51 = " = ";
  protected final String TEXT_52 = ".";
  protected final String TEXT_53 = ";";
  protected final String TEXT_54 = NL + "\t\tString invalidMsg = \"\";" + NL + "\t\tString tempStr = \"\";" + NL + "\t\tfor (String columnName : sbMatchResult_";
  protected final String TEXT_55 = ".keySet()) {" + NL + "\t\t  String strResult = sbMatchResult_";
  protected final String TEXT_56 = ".get(columnName);" + NL + "\t\t  if (\"0\".equals(strResult.substring(0, 1))) {" + NL + "\t\t    String tempMsg = strResult.substring(1, strResult.length());" + NL + "\t\t    if (\"\".equals(tempMsg)) {" + NL + "\t\t\t  invalidMsg += tempStr + \"invalid \" + columnName;" + NL + "\t\t\t} else {" + NL + "\t\t\t  // get the context value if the REGEX_INVALIDITY_MESSAGE is context" + NL + "              String temp = \"\";" + NL + "              if (tempMsg.startsWith(\"context.\")) {" + NL + "                temp = context.getProperty(tempMsg.substring(tempMsg.indexOf(\".\") +1, tempMsg.length()));" + NL + "              }" + NL + "              if (temp != null && !\"\".equals(temp.trim())) {" + NL + "                tempMsg = temp;" + NL + "              }" + NL + "\t\t\t  invalidMsg += tempStr + tempMsg;" + NL + "\t\t\t}" + NL + "\t\t\ttempStr = \" AND \";" + NL + "\t\t  }" + NL + "\t\t}";
  protected final String TEXT_57 = NL + "        ";
  protected final String TEXT_58 = ".REGEX_INVALIDITY_MESSAGE = invalidMsg;" + NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_59 = NL + "      nb_line_reject_";
  protected final String TEXT_60 = "++;" + NL + "    }    ";
  protected final String TEXT_61 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

List<IMetadataTable> metadatas = node.getMetadataList();  
if (metadatas != null && metadatas.size() > 0){
  IMetadataTable metadata = metadatas.get(0);
  String sInConnName = null, sOKConnName = null, sKOConnName = null;
  
  if (node.getIncomingConnections().size() > 0){
    sInConnName  = node.getIncomingConnections().get(0).getName();
    List< ? extends IConnection> connsOut = node.getOutgoingConnections();    
    String connOutCntorName = null, connOutFlowName = null;
    
    //initalize out connections
    for (IConnection connOut : connsOut){
      connOutCntorName  = connOut.getConnectorName();
      connOutFlowName = connOut.getName();
      
    stringBuffer.append(TEXT_1);
    stringBuffer.append(connOutFlowName);
    stringBuffer.append(TEXT_2);
    
      if ("ROW_PATTERN_OK".equals(connOutCntorName)){
        sOKConnName = connOutFlowName;
      } else if ("ROW_PATTERN_KO".equals(connOutCntorName)){
        sKOConnName = connOutFlowName;
      }
    }
    
    //get table properties
    List<Map<String, String>> tablePatterns = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__SCHEMA_PATTERN_CHECK__");
    
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_5);
    
      for (Map<String, String> p : tablePatterns){
		int i = 0;
        if ("true".equals(p.get("TOCHECK"))) {
           
          String pattern = p.get("PATTERN_REGEX");
        
          if ("true".equals(p.get("CASE_SENSITIVE"))) {
          
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_8);
    
          } else{
          
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_11);
    
          }
          
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(p.get("SCHEMA_COLUMN"));
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(sInConnName );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(p.get("SCHEMA_COLUMN"));
    stringBuffer.append(TEXT_17);
    stringBuffer.append(ParameterValueUtil.handleSpecialCharacters(p.get("REGEX_INVALIDITY_MESSAGE")));
    stringBuffer.append(TEXT_18);
    
        }
		i++;
      }
      
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    
    String logical = ElementParameterParser.getValue(node, "__SCHEMA_LOGICAL_OP__");
    
    if ("AND".equals(logical)){
    
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    
    } else{
    
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
    }
    
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
      // output to OK flow 
      if (sOKConnName != null){
      
    stringBuffer.append(TEXT_35);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(sOKConnName);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(sOKConnName);
    stringBuffer.append(TEXT_38);
    
        for (IMetadataColumn column : metadata.getListColumns()) {
          String label = column.getLabel();
          
    stringBuffer.append(TEXT_39);
    stringBuffer.append(sOKConnName);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(sInConnName);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_43);
    
        }
      }
      
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    
      // output to KO flow
      if (sKOConnName != null){
      
    stringBuffer.append(TEXT_46);
    stringBuffer.append(sKOConnName);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(sKOConnName);
    stringBuffer.append(TEXT_48);
    
        for (IMetadataColumn column : metadata.getListColumns()) {
          String label = column.getLabel();
		  
    stringBuffer.append(TEXT_49);
    stringBuffer.append(sKOConnName);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(sInConnName);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_53);
    
        }
		
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(sKOConnName);
    stringBuffer.append(TEXT_58);
    
      }
      
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
  }
}

    stringBuffer.append(TEXT_61);
    return stringBuffer.toString();
  }
}
