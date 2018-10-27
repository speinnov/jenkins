package org.talend.designer.codegen.translators.data_quality;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import java.util.Map;
import java.util.List;

public class TLastRegexlibExpressionsBeginJava
{
  protected static String nl;
  public static synchronized TLastRegexlibExpressionsBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TLastRegexlibExpressionsBeginJava result = new TLastRegexlibExpressionsBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + NL + NL + NL + "org.talend.regexUtil.ListAllAsXMLUtil util_";
  protected final String TEXT_3 = "=new org.talend.regexUtil.ListAllAsXMLUtil();" + NL + "java.io.InputStream inStream_";
  protected final String TEXT_4 = "=util_";
  protected final String TEXT_5 = ".getReturnXMLInputStreamOfLast(";
  protected final String TEXT_6 = ");" + NL + "" + NL + "" + NL + "" + NL + "" + NL + "org.dom4j.io.SAXReader reader_";
  protected final String TEXT_7 = " = new org.dom4j.io.SAXReader();" + NL + "" + NL + "String loopElement_";
  protected final String TEXT_8 = "=\"/soap:Envelope/soap:Body/ListAllAsXmlResponse/ListAllAsXmlResult/Expression\";" + NL + "org.dom4j.Document doc_";
  protected final String TEXT_9 = "= reader_";
  protected final String TEXT_10 = ".read(inStream_";
  protected final String TEXT_11 = ");" + NL + "" + NL + "org.talend.regexUtil.NameSpaceTool nsTool_";
  protected final String TEXT_12 = " = new org.talend.regexUtil.NameSpaceTool();" + NL + "nsTool_";
  protected final String TEXT_13 = ".countNSMap(doc_";
  protected final String TEXT_14 = ".getRootElement());" + NL + "java.util.HashMap<String,String> xmlNameSpaceMap_";
  protected final String TEXT_15 = " = nsTool_";
  protected final String TEXT_16 = ".xmlNameSpaceMap;  " + NL + "" + NL + "" + NL + "org.dom4j.XPath x_";
  protected final String TEXT_17 = " = doc_";
  protected final String TEXT_18 = ".createXPath(nsTool_";
  protected final String TEXT_19 = ".addDefaultNSPrefix(loopElement_";
  protected final String TEXT_20 = ",loopElement_";
  protected final String TEXT_21 = "));  " + NL + "x_";
  protected final String TEXT_22 = ".setNamespaceURIs(xmlNameSpaceMap_";
  protected final String TEXT_23 = "); " + NL + "" + NL + "java.util.List<org.dom4j.tree.AbstractNode> nodeList_";
  protected final String TEXT_24 = " = (java.util.List<org.dom4j.tree.AbstractNode>)x_";
  protected final String TEXT_25 = ".selectNodes(doc_";
  protected final String TEXT_26 = ");\t" + NL + "org.talend.regexUtil.XML_API xml_api_";
  protected final String TEXT_27 = " = new org.talend.regexUtil.XML_API();" + NL + "String str_";
  protected final String TEXT_28 = " = \"\";" + NL + "org.dom4j.Node node_";
  protected final String TEXT_29 = " = null;";
  protected final String TEXT_30 = NL + NL + NL + "//begin" + NL + "for (org.dom4j.tree.AbstractNode temp_";
  protected final String TEXT_31 = ": nodeList_";
  protected final String TEXT_32 = "){" + NL;
  protected final String TEXT_33 = NL + "\t" + NL + "" + NL + "\torg.dom4j.XPath xTmp";
  protected final String TEXT_34 = "_";
  protected final String TEXT_35 = " = temp_";
  protected final String TEXT_36 = ".createXPath(nsTool_";
  protected final String TEXT_37 = ".addDefaultNSPrefix(\"";
  protected final String TEXT_38 = "\",loopElement_";
  protected final String TEXT_39 = "));" + NL + "    xTmp";
  protected final String TEXT_40 = "_";
  protected final String TEXT_41 = ".setNamespaceURIs(xmlNameSpaceMap_";
  protected final String TEXT_42 = "); " + NL + "    node_";
  protected final String TEXT_43 = " = xTmp";
  protected final String TEXT_44 = "_";
  protected final String TEXT_45 = ".selectSingleNode(temp_";
  protected final String TEXT_46 = ");" + NL + "    str_";
  protected final String TEXT_47 = " = xTmp";
  protected final String TEXT_48 = "_";
  protected final String TEXT_49 = ".valueOf(temp_";
  protected final String TEXT_50 = ");" + NL;
  protected final String TEXT_51 = NL + "\t\t\t\t\t\t\t\t\tif(xml_api_";
  protected final String TEXT_52 = ".isDefNull(node_";
  protected final String TEXT_53 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_54 = ".";
  protected final String TEXT_55 = " =null;" + NL + "\t\t\t\t\t\t\t\t\t}else if(xml_api_";
  protected final String TEXT_56 = ".isEmpty(node_";
  protected final String TEXT_57 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_58 = ".";
  protected final String TEXT_59 = " =\"\";" + NL + "\t\t\t\t\t\t\t\t\t}else if(xml_api_";
  protected final String TEXT_60 = ".isMissing(node_";
  protected final String TEXT_61 = " )){ " + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_62 = ".";
  protected final String TEXT_63 = " =";
  protected final String TEXT_64 = ";" + NL + "\t\t\t\t\t\t\t\t\t}else{";
  protected final String TEXT_65 = NL + "\t\t\t\t\t\t\t\t\tif(xml_api_";
  protected final String TEXT_66 = ".isEmpty(node_";
  protected final String TEXT_67 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_68 = ".";
  protected final String TEXT_69 = " =\"\";" + NL + "\t\t\t\t\t\t\t\t\t}else if(xml_api_";
  protected final String TEXT_70 = ".isMissing(node_";
  protected final String TEXT_71 = " )){ " + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_72 = ".";
  protected final String TEXT_73 = " =";
  protected final String TEXT_74 = ";" + NL + "\t\t\t\t\t\t\t\t\t}else{";
  protected final String TEXT_75 = "\t" + NL + "\t\t\t\t\t\t\t\t\t\tif(xml_api_";
  protected final String TEXT_76 = ".isDefNull(node_";
  protected final String TEXT_77 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_78 = ".";
  protected final String TEXT_79 = " =null;" + NL + "\t\t\t\t\t\t\t\t\t\t}else if(xml_api_";
  protected final String TEXT_80 = ".isEmpty(node_";
  protected final String TEXT_81 = ") || xml_api_";
  protected final String TEXT_82 = ".isMissing(node_";
  protected final String TEXT_83 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_84 = ".";
  protected final String TEXT_85 = "=";
  protected final String TEXT_86 = ";" + NL + "\t\t\t\t\t\t\t\t\t\t}else{";
  protected final String TEXT_87 = NL + "\t\t\t\t\t\t\t\t\t\tif(xml_api_";
  protected final String TEXT_88 = ".isMissing(node_";
  protected final String TEXT_89 = ") || xml_api_";
  protected final String TEXT_90 = ".isEmpty(node_";
  protected final String TEXT_91 = ")){" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_92 = ".";
  protected final String TEXT_93 = " =";
  protected final String TEXT_94 = ";" + NL + "\t\t\t\t\t\t\t\t\t\t}else{";
  protected final String TEXT_95 = NL + "\t\t";
  protected final String TEXT_96 = ".";
  protected final String TEXT_97 = " = str_";
  protected final String TEXT_98 = ";";
  protected final String TEXT_99 = NL + "\t\t";
  protected final String TEXT_100 = ".";
  protected final String TEXT_101 = " = Integer.parseInt((str_";
  protected final String TEXT_102 = "));";
  protected final String TEXT_103 = NL + "\t}";
  protected final String TEXT_104 = NL + "\t" + NL + NL + NL + NL;
  protected final String TEXT_105 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

String maxRows  = ElementParameterParser.getValue(node, "__MAX_ROWS__");


    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(maxRows );
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
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
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    
String[] queryElement={"Title", "Pattern", "Description", "MatchingText", "NonMatchingText","AuthorName","Rating"};

    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
String firstConnName = "";
List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas!=null)&&(metadatas.size()>0)) {
	IMetadataTable metadata = metadatas.get(0);
	if (metadata!=null) {
		List<IMetadataColumn> columns=metadata.getListColumns();
		if (conns!=null) {
			if (conns.size()>0) {
				IConnection conn = conns.get(0);
				firstConnName = conn.getName();
				for (int i=0;i<queryElement.length;i++) { 
					String query = queryElement[i];
					

    stringBuffer.append(TEXT_33);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(query);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    				
       IMetadataColumn column=columns.get(i);	
					if(column!=null) {
						
							
								String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
								JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
								String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
								
								boolean isNotSetDefault = false;
								String defaultValue=column.getDefault();
								if(defaultValue!=null){
									isNotSetDefault = defaultValue.length()==0;
								}else{
									isNotSetDefault=true;
								}
								
								if(javaType == JavaTypesManager.STRING){
									if(column.isNullable()){

    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_54);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_58);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_62);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_63);
    stringBuffer.append(isNotSetDefault?null:column.getDefault());
    stringBuffer.append(TEXT_64);
    
									}else{ // column.isNullable()

    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_68);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_72);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_73);
    stringBuffer.append(isNotSetDefault?JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate):column.getDefault());
    stringBuffer.append(TEXT_74);
    
									}
								}else{ // other type
									if(column.isNullable()){

    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_78);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_84);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_85);
    stringBuffer.append(isNotSetDefault?null:column.getDefault());
    stringBuffer.append(TEXT_86);
    
								  }else{ // column.isNullable()

    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_92);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_93);
    stringBuffer.append(isNotSetDefault?JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate):column.getDefault());
    stringBuffer.append(TEXT_94);
    
									}
								}
								if (javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {

    stringBuffer.append(TEXT_95);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_96);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_98);
    
		}					
        else {//Integer type.

    stringBuffer.append(TEXT_99);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_100);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_102);
    
								}

    stringBuffer.append(TEXT_103);
    
							}
						}
					}
				
			

		}
		}
		}
	
		
	
		

    stringBuffer.append(TEXT_104);
    stringBuffer.append(TEXT_105);
    return stringBuffer.toString();
  }
}
