package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;

public class TFuzzyJoinMainJava
{
  protected static String nl;
  public static synchronized TFuzzyJoinMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TFuzzyJoinMainJava result = new TFuzzyJoinMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "/*-------------fuzzy match util begin----------------*/" + NL + "" + NL + "class FuzzyMatch {" + NL + "" + NL + "\tpublic boolean match(String comp, String ref, int minDistance, int maxDistance, String mode){" + NL + "\t\tif(\"EXACT_MATCH\".equals(mode)){" + NL + "\t\t\treturn exactMatch(comp,ref);" + NL + "\t\t}else if(\"LEVENSHTEIN\".equals(mode)){" + NL + "\t\t\treturn levenshteinMatch(comp, ref, minDistance, maxDistance);" + NL + "\t\t}else if(\"METAPHONE\".equals(mode)){" + NL + "\t\t\treturn metaphoneMatch(comp, ref);" + NL + "\t\t}else if(\"DOUBLE_METAPHONE\".equals(mode)){" + NL + "\t\t\treturn doublemetaphoneMatch(comp, ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn false;" + NL + "\t\t}" + NL + "\t}" + NL + "" + NL + "\tpublic boolean exactMatch(String comp, String ref){" + NL + "\t\tif(comp != null || ref != null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn comp == ref;" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean levenshteinMatch(String comp, String ref, int minDistance, int maxDistance){" + NL + "\t\tif(comp != null || ref != null){" + NL + "\t\t\tint distance = org.apache.commons.lang.StringUtils.getLevenshteinDistance(comp, ref);" + NL + "\t\t\treturn minDistance <= distance && distance <= maxDistance;" + NL + "\t\t}else{" + NL + "\t\t\treturn comp == ref;" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean metaphoneMatch(String comp, String ref){" + NL + "\t\torg.apache.commons.codec.language.Metaphone metaphone = new org.apache.commons.codec.language.Metaphone();" + NL + "\t\tif(comp == null || ref == null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else if(\"\".equals(comp) || \"\".equals(ref)){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn metaphone.metaphone(comp).equals(metaphone.metaphone(ref));" + NL + "\t\t}" + NL + "\t}" + NL + "\t" + NL + "\tpublic boolean doublemetaphoneMatch(String comp, String ref){" + NL + "\t\torg.apache.commons.codec.language.DoubleMetaphone doublemetaphone = new org.apache.commons.codec.language.DoubleMetaphone();" + NL + "\t\tif(comp == null || ref == null){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else if(\"\".equals(comp) || \"\".equals(ref)){" + NL + "\t\t\treturn comp.equals(ref);" + NL + "\t\t}else{" + NL + "\t\t\treturn doublemetaphone.doubleMetaphone(comp).equals(doublemetaphone.doubleMetaphone(ref));" + NL + "\t\t}" + NL + "\t}" + NL + "}" + NL + "" + NL + "/*-------------fuzzy match util end----------------*/";
  protected final String TEXT_2 = NL + "\t\t\t\t";
  protected final String TEXT_3 = " = null;";
  protected final String TEXT_4 = NL + "\t\t\t\t";
  protected final String TEXT_5 = " = new ";
  protected final String TEXT_6 = "Struct();\t";
  protected final String TEXT_7 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_8 = ".";
  protected final String TEXT_9 = " = ";
  protected final String TEXT_10 = ".";
  protected final String TEXT_11 = ";\t\t\t";
  protected final String TEXT_12 = NL + NL + "\t\t\t// if(util_";
  protected final String TEXT_13 = ".isJoined(";
  protected final String TEXT_14 = ")){         delete" + NL + "\t\t\tBoolean isMatch_";
  protected final String TEXT_15 = " = false;" + NL + "\t" + NL + "\t\t\t";
  protected final String TEXT_16 = "Struct ";
  protected final String TEXT_17 = "_tmp_";
  protected final String TEXT_18 = " = null;" + NL + "\t\t\tjava.util.Iterator<";
  protected final String TEXT_19 = "Struct> tItr_";
  protected final String TEXT_20 = "_";
  protected final String TEXT_21 = " = tSet_";
  protected final String TEXT_22 = "_";
  protected final String TEXT_23 = ".iterator();" + NL + "\t" + NL + "\t\t\tFuzzyMatch fuzzyMatch_";
  protected final String TEXT_24 = " = new FuzzyMatch();" + NL + "    \t\twhile(tItr_";
  protected final String TEXT_25 = "_";
  protected final String TEXT_26 = ".hasNext()){" + NL + "\t\t\t\t";
  protected final String TEXT_27 = "_tmp_";
  protected final String TEXT_28 = " = tItr_";
  protected final String TEXT_29 = "_";
  protected final String TEXT_30 = ".next();";
  protected final String TEXT_31 = NL + "\t\t\t\t\t\t\tString tomatch_";
  protected final String TEXT_32 = " = FormatterUtils.format_Date(";
  protected final String TEXT_33 = ".";
  protected final String TEXT_34 = ", ";
  protected final String TEXT_35 = ")";
  protected final String TEXT_36 = ".toLowerCase()";
  protected final String TEXT_37 = ";" + NL + "\t\t\t\t\t\t\tString lookUpValue_";
  protected final String TEXT_38 = " = FormatterUtils.format_Date(";
  protected final String TEXT_39 = "_tmp_";
  protected final String TEXT_40 = ".";
  protected final String TEXT_41 = ", ";
  protected final String TEXT_42 = ")";
  protected final String TEXT_43 = ".toLowerCase()";
  protected final String TEXT_44 = ";";
  protected final String TEXT_45 = NL + "\t\t\t\t\t\t\tString tomatch_";
  protected final String TEXT_46 = " = FormatterUtils.format_Date(";
  protected final String TEXT_47 = ".";
  protected final String TEXT_48 = ", \"dd-MM-yyyy\")";
  protected final String TEXT_49 = ".toLowerCase()";
  protected final String TEXT_50 = ";" + NL + "\t\t\t\t\t\t\tString lookUpValue_";
  protected final String TEXT_51 = " = FormatterUtils.format_Date(";
  protected final String TEXT_52 = "_tmp_";
  protected final String TEXT_53 = ".";
  protected final String TEXT_54 = ", \"dd-MM-yyyy\")";
  protected final String TEXT_55 = ".toLowerCase()";
  protected final String TEXT_56 = ";";
  protected final String TEXT_57 = NL + "\t\t\t\t\t\tString tomatch_";
  protected final String TEXT_58 = " = (\"\" + ";
  protected final String TEXT_59 = ".";
  protected final String TEXT_60 = ")";
  protected final String TEXT_61 = ".toLowerCase()";
  protected final String TEXT_62 = ";" + NL + "\t\t\t\t\t\tString lookUpValue_";
  protected final String TEXT_63 = " = (\"\" + ";
  protected final String TEXT_64 = "_tmp_";
  protected final String TEXT_65 = ".";
  protected final String TEXT_66 = ")";
  protected final String TEXT_67 = ".toLowerCase()";
  protected final String TEXT_68 = ";";
  protected final String TEXT_69 = NL + "\t\t\t\t\tif(fuzzyMatch_";
  protected final String TEXT_70 = ".match(tomatch_";
  protected final String TEXT_71 = ",lookUpValue_";
  protected final String TEXT_72 = ",";
  protected final String TEXT_73 = ",";
  protected final String TEXT_74 = ",\"";
  protected final String TEXT_75 = "\")){";
  protected final String TEXT_76 = NL + "\t\t\t\t\t\tisMatch_";
  protected final String TEXT_77 = " = true;" + NL + "\t\t\t\t\t\tbreak;";
  protected final String TEXT_78 = NL + "\t\t\t\t\t}";
  protected final String TEXT_79 = NL + "\t\t\t}" + NL + "" + NL + "\t\t\tif(isMatch_";
  protected final String TEXT_80 = "==true){";
  protected final String TEXT_81 = NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_82 = ".";
  protected final String TEXT_83 = " = ";
  protected final String TEXT_84 = "_tmp_";
  protected final String TEXT_85 = ".";
  protected final String TEXT_86 = ";";
  protected final String TEXT_87 = NL + "\t\t\t\t\t";
  protected final String TEXT_88 = " = null; ";
  protected final String TEXT_89 = NL + "\t\t\t} ";
  protected final String TEXT_90 = "\t" + NL + "\t\t\t\telse{";
  protected final String TEXT_91 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_92 = " = null; ";
  protected final String TEXT_93 = NL + "\t\t\t\t}";
  protected final String TEXT_94 = " " + NL + "\t\t\t\t\t";
  protected final String TEXT_95 = " = null; ";
  protected final String TEXT_96 = NL + "\t\t\t\t";
  protected final String TEXT_97 = " = new ";
  protected final String TEXT_98 = "Struct();\t";
  protected final String TEXT_99 = NL + "\t\t\t\t\t";
  protected final String TEXT_100 = ".";
  protected final String TEXT_101 = " = ";
  protected final String TEXT_102 = ".";
  protected final String TEXT_103 = ";\t\t\t";
  protected final String TEXT_104 = NL + "////////////";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas!=null)&&(metadatas.size()>0)) {//11
    IMetadataTable metadata = metadatas.get(0);
    if (metadata!=null) {//22
        String cid = node.getUniqueName();
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
    		}
        }//3
        
        if(inMainCon == null){
        	return "";
        }
        
        IMetadataTable preMetadata = inMainCon.getMetadataTable();
        String incomingName = inMainCon.getName();
        
    	List<IMetadataColumn> columns = metadata.getListColumns();
    	int columnSize = columns.size();
    	List<IMetadataColumn> preColumns = preMetadata.getListColumns();
    	int preColumnSize = preColumns.size();
    	int minSize = Math.min(columnSize, preColumnSize);
        
        List<Map<String, String>> lookupCols = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__LOOKUP_COLS__");
        List<Map<String, String>> joinKeys = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__JOIN_KEY__");
        boolean useLookup = ElementParameterParser.getValue(node, "__USE_LOOKUP_COLS__").equals("true");
        boolean useReject = ElementParameterParser.getValue(node, "__USE_INNER_JOIN__").equals("true");
        
    	List< ? extends IConnection> outConns = node.getOutgoingConnections();
		List<? extends IConnection> connsFilter = node.getOutgoingConnections("FLOW");
		List<? extends IConnection> connsReject = node.getOutgoingConnections("REJECT");
    	
    	for (IConnection conn : outConns) {
    		
    		if(!conn.isActivate() || !conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) continue;
    		String outputConnName = conn.getName();    		

    stringBuffer.append(TEXT_2);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_3);
    
		}
		if(inRefCon != null){//exist lookup
    		for (IConnection conn : outConns) {//3
    			if(!conn.isActivate() || !conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) continue;
    			String outputConnName = conn.getName();

    stringBuffer.append(TEXT_4);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_6);
    
				//first, iterate with minSize
				for (int i = 0; i < minSize; i++) {//4
					IMetadataColumn column = columns.get(i);
					IMetadataColumn preColumn = preColumns.get(i);
			
					//find the lookup column
					boolean isLookup = false;
					for (int j = 0; j < lookupCols.size(); j++) {
		                 Map<String, String> lineValue = lookupCols.get(j);
		                 if (column.getLabel().equals(lineValue.get("OUTPUT_COLUMN"))) {
							isLookup = true;
							break;
		                 }
		            }
            
            		if(!isLookup){

    stringBuffer.append(TEXT_7);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(incomingName );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(preColumn.getLabel() );
    stringBuffer.append(TEXT_11);
    
					}
				} //4
			}//3

    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
    stringBuffer.append(incomingName );
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(inRefCon.getName());
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_26);
    stringBuffer.append(inRefCon.getName());
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_30);
    
				int keyColSize = joinKeys.size();
				int intkeyCol = 0;
				for(Map<String, String> keyColumn : joinKeys){
					String inColName = keyColumn.get("INPUT_COLUMN");
					String lookupColName = keyColumn.get("LOOKUP_COLUMN");
					String matchingType = keyColumn.get("MATCHING_TYPE");
					Boolean isCaseSens = keyColumn.get("CASE_SENSITIVE").equals("true");
					String minDistance = keyColumn.get("MIN_DISTANCE");
					String maxDistance = keyColumn.get("MAX_DISTANCE");
					JavaType javaType = null;
					String pattern = null;
					//compute the type of the input column
					for(IMetadataColumn column : metadata.getListColumns()){
						if(column.getLabel().equals(inColName)){
							javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
							pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
							break;
						}
					}
					if (javaType == JavaTypesManager.DATE) {
						if(pattern != null && pattern.trim().length() != 0){

    stringBuffer.append(TEXT_31);
    stringBuffer.append(intkeyCol);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(inMainCon.getName() );
    stringBuffer.append(TEXT_33);
    stringBuffer.append(inColName );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(pattern );
    stringBuffer.append(TEXT_35);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_36);
    }
    stringBuffer.append(TEXT_37);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(lookupColName );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(pattern );
    stringBuffer.append(TEXT_42);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_43);
    }
    stringBuffer.append(TEXT_44);
    
						}else{

    stringBuffer.append(TEXT_45);
    stringBuffer.append(intkeyCol);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(inMainCon.getName() );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(inColName );
    stringBuffer.append(TEXT_48);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_49);
    }
    stringBuffer.append(TEXT_50);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_53);
    stringBuffer.append(lookupColName );
    stringBuffer.append(TEXT_54);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_55);
    }
    stringBuffer.append(TEXT_56);
    
						}
					}else{

    stringBuffer.append(TEXT_57);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_58);
    stringBuffer.append(inMainCon.getName() );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(inColName );
    stringBuffer.append(TEXT_60);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_61);
    }
    stringBuffer.append(TEXT_62);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_63);
    stringBuffer.append(inRefCon.getName() );
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(lookupColName );
    stringBuffer.append(TEXT_66);
    if(!isCaseSens){ 
    stringBuffer.append(TEXT_67);
    }
    stringBuffer.append(TEXT_68);
    
					}

    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_71);
    stringBuffer.append(intkeyCol );
    stringBuffer.append(TEXT_72);
    stringBuffer.append(minDistance);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(maxDistance);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(matchingType);
    stringBuffer.append(TEXT_75);
    
					intkeyCol ++;
					if (intkeyCol == keyColSize) { //3

    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_77);
    
					}
				}
				
				for(Map<String, String> keyColumn : joinKeys){

    stringBuffer.append(TEXT_78);
    
				}

    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_80);
    
				if(useLookup) {//2
					for (IConnection conn : connsFilter) {//3
						if(!conn.isActivate()) continue;
						String outputConnName = conn.getName();
						//second, iterate with columnSize
						for (int i = 0; i < columnSize; i++) {//4
							IMetadataColumn column = columns.get(i);
							//find the lookup column
							boolean isLookup = false;
							String lookupName = null;
							for (int j = 0; j < lookupCols.size(); j++) {
				                 Map<String, String> lineValue = lookupCols.get(j);
				                 if (column.getLabel().equals(lineValue.get("OUTPUT_COLUMN"))) {
									isLookup = true;
									lookupName = lineValue.get("LOOKUP_COLUMN");
									break;
				                 }
				            }
							if(isLookup) {

    stringBuffer.append(TEXT_81);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_82);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_83);
    stringBuffer.append(inRefCon.getName());
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_85);
    stringBuffer.append(lookupName );
    stringBuffer.append(TEXT_86);
    
							}
						} //4
		  			}//3
				}//2
				for (IConnection conn : connsReject) { 
					if(!conn.isActivate()) continue; 

    stringBuffer.append(TEXT_87);
    stringBuffer.append(conn.getName());
    stringBuffer.append(TEXT_88);
    
				}

    stringBuffer.append(TEXT_89);
    
			if(useReject){

    stringBuffer.append(TEXT_90);
    
					for (IConnection conn : connsFilter) { 
						if(!conn.isActivate()) continue; 

    stringBuffer.append(TEXT_91);
    stringBuffer.append(conn.getName());
    stringBuffer.append(TEXT_92);
    
					}

    stringBuffer.append(TEXT_93);
    
			}else{
				for (IConnection conn : connsReject) { 
					if(!conn.isActivate()) continue;

    stringBuffer.append(TEXT_94);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_95);
    
				}
			}
		}else{//exist lookup
			for (IConnection conn : connsFilter) {
				if(!conn.isActivate()) continue;
    			String outputConnName = conn.getName();

    stringBuffer.append(TEXT_96);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_97);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_98);
    
				//first, iterate with minSize
				for (int i = 0; i < minSize; i++) {//4
					IMetadataColumn column = columns.get(i);
					IMetadataColumn preColumn = preColumns.get(i);

    stringBuffer.append(TEXT_99);
    stringBuffer.append(outputConnName );
    stringBuffer.append(TEXT_100);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_101);
    stringBuffer.append(incomingName );
    stringBuffer.append(TEXT_102);
    stringBuffer.append(preColumn.getLabel() );
    stringBuffer.append(TEXT_103);
    
				} //4
			}
		}

    stringBuffer.append(TEXT_104);
    
    }//22
}//11

    return stringBuffer.toString();
  }
}
