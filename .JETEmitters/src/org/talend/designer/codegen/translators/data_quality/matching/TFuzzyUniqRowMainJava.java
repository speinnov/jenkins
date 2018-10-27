package org.talend.designer.codegen.translators.data_quality.matching;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.IConnection;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;

public class TFuzzyUniqRowMainJava
{
  protected static String nl;
  public static synchronized TFuzzyUniqRowMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TFuzzyUniqRowMainJava result = new TFuzzyUniqRowMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "/*-------------fuzzy match util begin----------------*/" + NL;
  protected final String TEXT_2 = NL + NL + "class FuzzyMatch_";
  protected final String TEXT_3 = " {" + NL + "" + NL + "\tpublic boolean match(String comp, String ref, int minDistance, int maxDistance, String mode){" + NL + "\t\tif(\"EXACT_MATCH\".equals(mode)){" + NL + "\t\t\treturn exactMatch(comp,ref);" + NL + "\t\t}else if(\"LEVENSHTEIN\".equals(mode)){" + NL + "\t\t\treturn levenshteinMatch(comp, ref, minDistance, maxDistance);" + NL + "\t\t}else if(\"METAPHONE\".equals(mode)){" + NL + "\t\t\treturn metaphoneMatch(comp, ref);" + NL + "\t\t}else if(\"DOUBLE_METAPHONE\".equals(mode)){" + NL + "\t\t\treturn doublemetaphoneMatch(comp, ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn false;" + NL + "\t\t}" + NL + "\t}" + NL + "" + NL + "\tpublic boolean exactMatch(String comp, String ref){" + NL + "\t\tif(comp != null || ref != null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn comp == ref;" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean levenshteinMatch(String comp, String ref, int minDistance, int maxDistance){" + NL + "\t\tif(comp != null || ref != null){" + NL + "\t\t\tint distance = org.apache.commons.lang.StringUtils.getLevenshteinDistance(comp, ref);" + NL + "\t\t\treturn minDistance <= distance && distance <= maxDistance;" + NL + "\t\t}else{" + NL + "\t\t\treturn comp == ref;" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean metaphoneMatch(String comp, String ref){" + NL + "\t\torg.apache.commons.codec.language.Metaphone metaphone = new org.apache.commons.codec.language.Metaphone();" + NL + "\t\tif(comp == null || ref == null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else if(\"\".equals(comp) || \"\".equals(ref)){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn metaphone.metaphone(comp).equals(metaphone.metaphone(ref));" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean doublemetaphoneMatch(String comp, String ref){" + NL + "\t\torg.apache.commons.codec.language.DoubleMetaphone doublemetaphone = new org.apache.commons.codec.language.DoubleMetaphone();" + NL + "\t\tif(comp == null || ref == null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else if(\"\".equals(comp) || \"\".equals(ref)){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn doublemetaphone.doubleMetaphone(comp).equals(doublemetaphone.doubleMetaphone(ref));" + NL + "\t\t}" + NL + "\t}" + NL + "}" + NL + "" + NL + "/*-------------fuzzy match util end----------------*/";
  protected final String TEXT_4 = NL + "\t";
  protected final String TEXT_5 = " = null;\t\t\t";
  protected final String TEXT_6 = NL + "\tBoolean isMatch_";
  protected final String TEXT_7 = " = false;" + NL + "\tFuzzyMatch_";
  protected final String TEXT_8 = " fuzzyMatch_";
  protected final String TEXT_9 = "= new FuzzyMatch_";
  protected final String TEXT_10 = "();";
  protected final String TEXT_11 = NL + "\tString temp2_";
  protected final String TEXT_12 = " = ( ";
  protected final String TEXT_13 = ".";
  protected final String TEXT_14 = " == null ||  \"\".equals(";
  protected final String TEXT_15 = ".";
  protected final String TEXT_16 = ") )? \"null\" : ";
  protected final String TEXT_17 = ".";
  protected final String TEXT_18 = ";";
  protected final String TEXT_19 = NL + "\tfor (int i=0; i < ";
  protected final String TEXT_20 = "_list_";
  protected final String TEXT_21 = ".size(); i++) {";
  protected final String TEXT_22 = NL + "\t\tString temp1_";
  protected final String TEXT_23 = " = (String)";
  protected final String TEXT_24 = "_list_";
  protected final String TEXT_25 = ".get(i);" + NL + "\t\tif(fuzzyMatch_";
  protected final String TEXT_26 = ".match(temp1_";
  protected final String TEXT_27 = ",temp2_";
  protected final String TEXT_28 = ",";
  protected final String TEXT_29 = ",";
  protected final String TEXT_30 = ",\"";
  protected final String TEXT_31 = "\")){";
  protected final String TEXT_32 = NL + "\t\t\tisMatch_";
  protected final String TEXT_33 = " = true;" + NL + "\t\t\tif(UID_list_";
  protected final String TEXT_34 = ".size()>0){" + NL + "\t\t\t    dId_";
  protected final String TEXT_35 = " = UID_list_";
  protected final String TEXT_36 = ".get(i);" + NL + "\t\t\t}" + NL + "\t\t\tbreak;";
  protected final String TEXT_37 = NL + "\t\t}";
  protected final String TEXT_38 = NL + NL + "\t\t}" + NL + "\t\t" + NL + "\t\tif (isMatch_";
  protected final String TEXT_39 = " == false || nb_line_";
  protected final String TEXT_40 = ".get() == 0) {";
  protected final String TEXT_41 = NL + "\t\t\t";
  protected final String TEXT_42 = "_list_";
  protected final String TEXT_43 = ".add(temp2_";
  protected final String TEXT_44 = ");";
  protected final String TEXT_45 = NL + "\t\t}" + NL + "\t\tnb_line_";
  protected final String TEXT_46 = ".incrementAndGet();" + NL + "\t\tif (isMatch_";
  protected final String TEXT_47 = ") {";
  protected final String TEXT_48 = NL + "\t\t\tnb_duplicates_";
  protected final String TEXT_49 = ".incrementAndGet();" + NL + "\t\t\t";
  protected final String TEXT_50 = " = new ";
  protected final String TEXT_51 = "Struct();" + NL + "\t\t\t";
  protected final String TEXT_52 = ".FID = String.valueOf(dId_";
  protected final String TEXT_53 = ");";
  protected final String TEXT_54 = "\t\t\t" + NL + "\t\t    ";
  protected final String TEXT_55 = ".";
  protected final String TEXT_56 = " = ";
  protected final String TEXT_57 = ".";
  protected final String TEXT_58 = ";\t\t\t";
  protected final String TEXT_59 = NL + "\t\t}" + NL + "\t\telse {";
  protected final String TEXT_60 = NL + "\t\t\t";
  protected final String TEXT_61 = " = new ";
  protected final String TEXT_62 = "Struct();" + NL + "\t\t\tint i_";
  protected final String TEXT_63 = " = nb_uniques_";
  protected final String TEXT_64 = ".incrementAndGet();" + NL + "\t\t\tUID_list_";
  protected final String TEXT_65 = ".add(i_";
  protected final String TEXT_66 = ");" + NL + "\t\t\t";
  protected final String TEXT_67 = ".UID = String.valueOf(i_";
  protected final String TEXT_68 = ");";
  protected final String TEXT_69 = "\t\t\t" + NL + "\t\t    ";
  protected final String TEXT_70 = ".";
  protected final String TEXT_71 = " = ";
  protected final String TEXT_72 = ".";
  protected final String TEXT_73 = ";\t\t\t";
  protected final String TEXT_74 = NL + "\t\t}" + NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    

	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();


    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    

List<? extends IConnection> conns = node.getOutgoingSortedConnections();
List<? extends IConnection> connsUnique = node.getOutgoingConnections("UNIQUE");
List<? extends IConnection> connsDuplicate = node.getOutgoingConnections("DUPLICATE");
String onlyOnceEachDuplicatedKey = ElementParameterParser.getValue(node, "__ONLY_ONCE_EACH_DUPLICATED_KEY__");
List<Map<String, String>> keyColumns = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__UNIQUE_KEY__");

String connName = null;
if (node.getIncomingConnections().size()==1) {
	IConnection conn = node.getIncomingConnections().get(0);
	connName = conn.getName();
}

List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas!=null)&&(metadatas.size()>0)) {
    IMetadataTable metadata = metadatas.get(0);
    if (metadata!=null && connName != null && conns != null) {
	
		int currentKeyColumnNumber = 0;
		int numberOfKeyColumns = 0;
		String firstKeyColumn = null;

		for(Map<String, String> keyColumn : keyColumns) {
			if (keyColumn.get("KEY_ATTRIBUTE").equals("true")) {
				numberOfKeyColumns++;
				if (numberOfKeyColumns == 1) {
					firstKeyColumn = keyColumn.get("SCHEMA_COLUMN");
				}
			}
		}

		for (int i=0;i<conns.size();i++) {
			IConnection conn = conns.get(i);
			if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

    stringBuffer.append(TEXT_4);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_5);
    
			}
		}


    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    
	for(Map<String, String> keyColumn : keyColumns) { //1
		Boolean isKey = keyColumn.get("KEY_ATTRIBUTE").equals("true");
		String colName = keyColumn.get("SCHEMA_COLUMN");
		Boolean caseSensetive = keyColumn.get("CASE_SENSITIVE").equals("true");
		if (isKey == true) { // 2

    stringBuffer.append(TEXT_11);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(colName);
    stringBuffer.append(((!caseSensetive)?".toLowerCase()":""));
    stringBuffer.append(TEXT_18);
    
		}
	}

    stringBuffer.append(TEXT_19);
    stringBuffer.append(firstKeyColumn);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    

		for(Map<String, String> keyColumn : keyColumns) { //1
			String colName = keyColumn.get("SCHEMA_COLUMN");
			Boolean isKey = keyColumn.get("KEY_ATTRIBUTE").equals("true");
			String matchingType = keyColumn.get("MATCHING_TYPE");
			Boolean caseSensetive = keyColumn.get("CASE_SENSITIVE").equals("true");
			Integer minDistance = Integer.parseInt(keyColumn.get("MIN_DISTANCE"));
			Integer maxDistance = Integer.parseInt(keyColumn.get("MAX_DISTANCE"));
			if (isKey == true) { // 2

    stringBuffer.append(TEXT_22);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(minDistance);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(maxDistance);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(matchingType);
    stringBuffer.append(TEXT_31);
    
				currentKeyColumnNumber++;
				if (currentKeyColumnNumber == numberOfKeyColumns) { //3

    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_36);
    
				} //3
			} // 2
		} // 1
		
		// close all the open ifs above
		for(Map<String, String> keyColumn : keyColumns) {
			if (keyColumn.get("KEY_ATTRIBUTE").equals("true")) {

    stringBuffer.append(TEXT_37);
    
			}
		}

    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_40);
    
		for(Map<String, String> keyColumn : keyColumns) {
			if (keyColumn.get("KEY_ATTRIBUTE").equals("true")) {
				String colName = keyColumn.get("SCHEMA_COLUMN");
				Boolean caseSensetive = keyColumn.get("CASE_SENSITIVE").equals("true");

    stringBuffer.append(TEXT_41);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_44);
    
			}
		}

    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    
			if (connsDuplicate != null && connsDuplicate.size() == 1) {
				IConnection conn = connsDuplicate.get(0);
				if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_49);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_50);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_53);
    
					for (IMetadataColumn column: metadata.getListColumns()) {

    stringBuffer.append(TEXT_54);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_56);
    stringBuffer.append(connName );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_58);
    					 
					}
				}
			}

    stringBuffer.append(TEXT_59);
    
			if (connsUnique != null && connsUnique.size() == 1) {
				IConnection conn = connsUnique.get(0);
				if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

    stringBuffer.append(TEXT_60);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_61);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    
					for (IMetadataColumn column: metadata.getListColumns()) {

    stringBuffer.append(TEXT_69);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_71);
    stringBuffer.append(connName );
    stringBuffer.append(TEXT_72);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_73);
    					 
					}
				}
			}

    stringBuffer.append(TEXT_74);
    
		}
	}

    return stringBuffer.toString();
  }
}
