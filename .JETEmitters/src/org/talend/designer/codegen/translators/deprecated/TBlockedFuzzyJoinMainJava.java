package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.EConnectionType;
import java.util.List;
import java.util.Map;

public class TBlockedFuzzyJoinMainJava
{
  protected static String nl;
  public static synchronized TBlockedFuzzyJoinMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TBlockedFuzzyJoinMainJava result = new TBlockedFuzzyJoinMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "class FuzzyMatch {" + NL + "  public boolean exactMatch(String comp, String ref, boolean bIgnoreCase){" + NL + "    if(comp != null && ref != null){" + NL + "      if (bIgnoreCase) { " + NL + "        return comp.equals(ref);" + NL + "      } else{" + NL + "        return comp.equalsIgnoreCase(ref);" + NL + "      }" + NL + "    }else{" + NL + "      return comp == ref;" + NL + "    }" + NL + "  }  " + NL + "  public boolean metaphoneMatch(String comp, String ref) {" + NL + "    org.apache.commons.codec.language.Metaphone metaphone = new org.apache.commons.codec.language.Metaphone();" + NL + "        " + NL + "    if (comp == null && ref == null){" + NL + "      return true;" + NL + "    } else if (comp == null || ref == null){" + NL + "      return false;" + NL + "    } else if (\"\".equals(comp.trim()) || \"\".equals(ref.trim())){" + NL + "      return comp.equals(ref);" + NL + "    } else{" + NL + "      return metaphone.metaphone(comp).equals(metaphone.metaphone(ref));" + NL + "    }    " + NL + "  }" + NL + "    " + NL + "  public boolean doublemetaphoneMatch(String comp, String ref) {" + NL + "    org.apache.commons.codec.language.DoubleMetaphone doublemetaphone = new org.apache.commons.codec.language.DoubleMetaphone();" + NL + "    " + NL + "    if (comp == null && ref == null){" + NL + "      return true;" + NL + "    } else if (comp == null || ref == null){" + NL + "      return false;" + NL + "    } else if (\"\".equals(comp.trim()) || \"\".equals(ref.trim())){" + NL + "      return comp.equals(ref);" + NL + "    } else{" + NL + "      return doublemetaphone.doubleMetaphone(comp).equals(doublemetaphone.doubleMetaphone(ref));" + NL + "    }" + NL + "  }" + NL + "  " + NL + "  public int levenshteinNum(String comp, String ref, boolean bIgnoreCase){" + NL + "    if(comp != null && ref != null){" + NL + "      if (bIgnoreCase){" + NL + "        comp = comp.toLowerCase();" + NL + "        ref = ref.toLowerCase();" + NL + "      }" + NL + "      return org.apache.commons.lang.StringUtils.getLevenshteinDistance(comp, ref);      " + NL + "    }else if (comp != null){" + NL + "      return comp.length();" + NL + "    }else if (ref != null){" + NL + "      return ref.length();" + NL + "    }else{" + NL + "      return 0;" + NL + "    }" + NL + "  }" + NL + "}";
  protected final String TEXT_2 = "    ";
  protected final String TEXT_3 = NL + "        ";
  protected final String TEXT_4 = " = null;";
  protected final String TEXT_5 = NL + "          String field_main_";
  protected final String TEXT_6 = " = FormatterUtils.format_Date(";
  protected final String TEXT_7 = ".";
  protected final String TEXT_8 = ", ";
  protected final String TEXT_9 = ");" + NL + "          String field_lookup_";
  protected final String TEXT_10 = " = FormatterUtils.format_Date(entry.getKey().";
  protected final String TEXT_11 = ", ";
  protected final String TEXT_12 = ");";
  protected final String TEXT_13 = NL + "          String field_main_";
  protected final String TEXT_14 = " = String.valueOf(";
  protected final String TEXT_15 = ".";
  protected final String TEXT_16 = ");" + NL + "          String field_lookup_";
  protected final String TEXT_17 = " = String.valueOf(entry.getKey().";
  protected final String TEXT_18 = ");";
  protected final String TEXT_19 = NL + "        ";
  protected final String TEXT_20 = " = new ";
  protected final String TEXT_21 = "Struct();";
  protected final String TEXT_22 = NL + "            ";
  protected final String TEXT_23 = ".";
  protected final String TEXT_24 = " = ";
  protected final String TEXT_25 = ";";
  protected final String TEXT_26 = NL + "                ";
  protected final String TEXT_27 = ".";
  protected final String TEXT_28 = " = ";
  protected final String TEXT_29 = ".";
  protected final String TEXT_30 = ";";
  protected final String TEXT_31 = NL + "                ";
  protected final String TEXT_32 = ".";
  protected final String TEXT_33 = " = ";
  protected final String TEXT_34 = ".";
  protected final String TEXT_35 = ";";
  protected final String TEXT_36 = NL + "                ";
  protected final String TEXT_37 = ".";
  protected final String TEXT_38 = " = ";
  protected final String TEXT_39 = ".";
  protected final String TEXT_40 = ";";
  protected final String TEXT_41 = NL + "                ";
  protected final String TEXT_42 = ".";
  protected final String TEXT_43 = " = null;\t";
  protected final String TEXT_44 = NL + "              ";
  protected final String TEXT_45 = ".";
  protected final String TEXT_46 = " = ";
  protected final String TEXT_47 = ".";
  protected final String TEXT_48 = ";";
  protected final String TEXT_49 = NL + "              ";
  protected final String TEXT_50 = ".";
  protected final String TEXT_51 = " = ";
  protected final String TEXT_52 = ".";
  protected final String TEXT_53 = ";";
  protected final String TEXT_54 = NL + "              ";
  protected final String TEXT_55 = ".";
  protected final String TEXT_56 = " = null;\t";
  protected final String TEXT_57 = NL + "    boolean bMatches_";
  protected final String TEXT_58 = " = false, bPossibleMatches_";
  protected final String TEXT_59 = " = false, bPossibleMatches_flag_";
  protected final String TEXT_60 = " = false;" + NL + "    String s_MATCHING_DISTANCES_";
  protected final String TEXT_61 = " = null;";
  protected final String TEXT_62 = NL + "      FuzzyMatch fm_";
  protected final String TEXT_63 = " = new FuzzyMatch();" + NL + "      bMatches_";
  protected final String TEXT_64 = " = true;";
  protected final String TEXT_65 = NL + "      ";
  protected final String TEXT_66 = "Struct ";
  protected final String TEXT_67 = " = null, firstPossibleMatch_";
  protected final String TEXT_68 = " = null;" + NL + "      String[] array_MATCHING_DISTANCES_";
  protected final String TEXT_69 = " = new String[";
  protected final String TEXT_70 = "];" + NL + "      String sAllDistance_";
  protected final String TEXT_71 = " = null;" + NL + "    " + NL + "      for (java.util.Map.Entry<";
  protected final String TEXT_72 = "Struct, ";
  protected final String TEXT_73 = "Struct> entry : tHash_";
  protected final String TEXT_74 = ".entrySet()){";
  protected final String TEXT_75 = NL + "        ";
  protected final String TEXT_76 = " = entry.getKey();" + NL + "  ";
  protected final String TEXT_77 = "// exact match";
  protected final String TEXT_78 = "          ";
  protected final String TEXT_79 = NL + "            if (!(bMatches_";
  protected final String TEXT_80 = " = fm_";
  protected final String TEXT_81 = ".exactMatch(field_lookup_";
  protected final String TEXT_82 = ", field_main_";
  protected final String TEXT_83 = ", ";
  protected final String TEXT_84 = "))) continue;          ";
  protected final String TEXT_85 = "      " + NL + "        ";
  protected final String TEXT_86 = "// metaphoneMatch";
  protected final String TEXT_87 = NL + "            if (!(bMatches_";
  protected final String TEXT_88 = " = fm_";
  protected final String TEXT_89 = ".metaphoneMatch(field_main_";
  protected final String TEXT_90 = ", field_lookup_";
  protected final String TEXT_91 = "))) continue;";
  protected final String TEXT_92 = "        " + NL + "      ";
  protected final String TEXT_93 = "// doublemetaphoneMatch";
  protected final String TEXT_94 = NL + "            if (!(bMatches_";
  protected final String TEXT_95 = " = fm_";
  protected final String TEXT_96 = ".doublemetaphoneMatch(field_main_";
  protected final String TEXT_97 = ", field_lookup_";
  protected final String TEXT_98 = "))) continue;";
  protected final String TEXT_99 = NL + "      ";
  protected final String TEXT_100 = NL + "          // levenshteinMatch" + NL + "          bMatches_";
  protected final String TEXT_101 = " = true;" + NL + "          bPossibleMatches_";
  protected final String TEXT_102 = " = true;           ";
  protected final String TEXT_103 = NL + "            int iDistance_";
  protected final String TEXT_104 = " = fm_";
  protected final String TEXT_105 = ".levenshteinNum(field_main_";
  protected final String TEXT_106 = ", field_lookup_";
  protected final String TEXT_107 = ", ";
  protected final String TEXT_108 = ");" + NL + "          " + NL + "            array_MATCHING_DISTANCES_";
  protected final String TEXT_109 = "[";
  protected final String TEXT_110 = "] = Integer.toString(iDistance_";
  protected final String TEXT_111 = ");          " + NL + "            if ((0 <= iDistance_";
  protected final String TEXT_112 = ") && (iDistance_";
  protected final String TEXT_113 = " <= ";
  protected final String TEXT_114 = ")){" + NL + "              // matches" + NL + "            }else if ((";
  protected final String TEXT_115 = " < iDistance_";
  protected final String TEXT_116 = ") && (iDistance_";
  protected final String TEXT_117 = " <= ";
  protected final String TEXT_118 = ")){" + NL + "              // possible matches" + NL + "              bMatches_";
  protected final String TEXT_119 = " = false;" + NL + "            } else if (";
  protected final String TEXT_120 = " < iDistance_";
  protected final String TEXT_121 = "){" + NL + "              // none matches" + NL + "              bPossibleMatches_";
  protected final String TEXT_122 = " = bMatches_";
  protected final String TEXT_123 = " = false;" + NL + "            }";
  protected final String TEXT_124 = NL + "      " + NL + "        StringBuffer sb_";
  protected final String TEXT_125 = " = new StringBuffer(10);" + NL + "        for(String s_";
  protected final String TEXT_126 = " : array_MATCHING_DISTANCES_";
  protected final String TEXT_127 = ")" + NL + "\t\t  sb_";
  protected final String TEXT_128 = ".append(s_";
  protected final String TEXT_129 = " == null ? \"\" : s_";
  protected final String TEXT_130 = ").append(\"|\");" + NL + "        sb_";
  protected final String TEXT_131 = ".deleteCharAt(sb_";
  protected final String TEXT_132 = ".length() - 1);" + NL + "        s_MATCHING_DISTANCES_";
  protected final String TEXT_133 = " = new String(sb_";
  protected final String TEXT_134 = ");" + NL + "          " + NL + "        if (bMatches_";
  protected final String TEXT_135 = "){" + NL + "          break;" + NL + "        } else if (bPossibleMatches_";
  protected final String TEXT_136 = "){" + NL + "          // if all levenshtein column is possible matches, open flag and loop goes on." + NL + "          bPossibleMatches_flag_";
  protected final String TEXT_137 = " = true;" + NL + "          // save the first possible match row from look up flow          " + NL + "          if(firstPossibleMatch_";
  protected final String TEXT_138 = " == null)" + NL + "            firstPossibleMatch_";
  protected final String TEXT_139 = " = entry.getKey();" + NL + "          // save the first MATCHING_DISTANCES" + NL + "          if (sAllDistance_";
  protected final String TEXT_140 = " == null)" + NL + "            sAllDistance_";
  protected final String TEXT_141 = " = s_MATCHING_DISTANCES_";
  protected final String TEXT_142 = ";" + NL + "        }" + NL + "      }" + NL;
  protected final String TEXT_143 = NL + "        // matches output" + NL + "        if (bMatches_";
  protected final String TEXT_144 = "){";
  protected final String TEXT_145 = NL + "        }";
  protected final String TEXT_146 = NL + "    ";
  protected final String TEXT_147 = NL + "        // possible matches output" + NL + "        if ((!bMatches_";
  protected final String TEXT_148 = ") && bPossibleMatches_flag_";
  protected final String TEXT_149 = "){ ";
  protected final String TEXT_150 = NL + "        }";
  protected final String TEXT_151 = NL + "    ";
  protected final String TEXT_152 = NL + "      // none matches output" + NL + "      if ((!bMatches_";
  protected final String TEXT_153 = ") && (!bPossibleMatches_flag_";
  protected final String TEXT_154 = ")){      ";
  protected final String TEXT_155 = NL + "      }";
  protected final String TEXT_156 = NL;
  protected final String TEXT_157 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
     
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();    

List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas != null) && (metadatas.size() > 0)) {
  IMetadataTable metadata = metadatas.get(0);
  if (metadata != null){
    int index = 0;

    IConnection lookupConn = null, mainConn = null;
    String connNameLookUp = "", connNameMain = "";

    List< ? extends IConnection> connsIn = node.getIncomingConnections();
    for (IConnection connIn: connsIn){
      if (connIn.getLineStyle().equals(EConnectionType.FLOW_REF)){
        lookupConn = connIn;
        connNameLookUp = connIn.getName();
      } else if (connIn.getLineStyle().equals(EConnectionType.FLOW_MAIN)){
        mainConn = connIn;
        connNameMain = connIn.getName();
      }
    }
    
    if ((mainConn == null) || (!mainConn.isActivate())) return "";
    if ((lookupConn == null) || (!lookupConn.isActivate())) return "";
    List< ? extends IConnection> connsOut = node.getOutgoingConnections();
    if (connsOut == null || connsOut.size() == 0) return "";
    
    List<String> listMainColsName = new java.util.ArrayList<String>(); // save main flow columns name.
    for(IMetadataColumn mainColumn : mainConn.getMetadataTable().getListColumns())
	  listMainColsName.add(mainColumn.getLabel());
		 
	List<String> listLUColsName = new java.util.ArrayList<String>(); // save lookup flow columns name.
    for(IMetadataColumn luColumn : lookupConn.getMetadataTable().getListColumns())
	  listLUColsName.add(luColumn.getLabel());
    
    List<String> listOutColsName = new java.util.ArrayList<String>(); // save out flow columns name.
    for(IMetadataColumn outColumn : metadata.getListColumns())
      listOutColsName.add(outColumn.getLabel());
      
    String connNameMatchesOut = null, connNamePosMatchOut = null, connNameNonMatchOut = null;
    
    for (IConnection connOut: connsOut){
      if (connOut.isActivate() && connOut.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){
        String connOutCntorName = connOut.getConnectorName();
        String connOutFlowName = connOut.getName();
        
    stringBuffer.append(TEXT_2);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connOutFlowName);
    stringBuffer.append(TEXT_4);
    
        if ("MATCHES".equals(connOutCntorName)){
          connNameMatchesOut = connOutFlowName;
        } else if ("POSSIBLE_MATCHES".equals(connOutCntorName)){
          connNamePosMatchOut = connOutFlowName;
        } else if ("NON_MATCHES".equals(connOutCntorName)){
          connNameNonMatchOut = connOutFlowName;
        }
      }
    }
    
    // read columns mapping, the defined columns data from main flow will be replaced by lookup flow if row matches(or possible matches)
    boolean bReplacedByLookupRow  = "true".equals(ElementParameterParser.getValue(node, "__REPLACED_BY_LOOKUPCOL__"));
    List<Map<String, String>> listReplacedColsMapping = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__REPLACED_COLS__");
    
    // split Map(join keys) into 4 pieces
    List<Map<String, String>> listMapJoinCols = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__JOIN_KEY__");
    String sJoinSourceColName = "", sJoinTargetColName = "", sJoinMatchType = "", sJoinDistanceMin = "", sJoinDistanceMax = "";
    
    List<Map<String, String>> listExactMatch = null, listLevenshtein = null, listMetaphone = null, listDoubleMetaphone = null;
    int iJoinSize = 0; // the LEVENSHTEIN row index (also it returns the size of listMapJoinCols)
    for (Map<String, String> mapJoinCol : listMapJoinCols){
      sJoinMatchType = mapJoinCol.get("MATCHING_TYPE");
      if ("EXACT_MATCH".equals(sJoinMatchType)){
        if (listExactMatch == null) listExactMatch = new java.util.ArrayList();
        listExactMatch.add(mapJoinCol);
      } else if ("LEVENSHTEIN".equals(sJoinMatchType)){
        mapJoinCol.put("INDEX", Integer.toString(iJoinSize));
        if (listLevenshtein == null) listLevenshtein = new java.util.ArrayList();
        listLevenshtein.add(mapJoinCol);
      } else if ("METAPHONE".equals(sJoinMatchType)){
        if (listMetaphone == null) listMetaphone = new java.util.ArrayList();
        listMetaphone.add(mapJoinCol);
      } else if ("DOUBLE_METAPHONE".equals(sJoinMatchType)){
        if (listDoubleMetaphone == null) listDoubleMetaphone = new java.util.ArrayList();
        listDoubleMetaphone.add(mapJoinCol);
      }
      iJoinSize++;
    }
    
    class CodeGenerator{
      public void initCompareCols(IMetadataTable meta, String connName, java.util.Map<String, String> mapCondition, int index){
        IMetadataColumn col = meta.getColumn(mapCondition.get("INPUT_COLUMN"));
        JavaType colType = JavaTypesManager.getJavaTypeFromId(col.getTalendType());
        
        if (colType == JavaTypesManager.DATE){
          String pattern = col.getPattern();
          pattern = (pattern == null || "".equals(pattern.trim())) ? "\"dd-MM-yyyy\"" : pattern;
          
    stringBuffer.append(TEXT_5);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(mapCondition.get("INPUT_COLUMN"));
    stringBuffer.append(TEXT_8);
    stringBuffer.append(pattern );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(mapCondition.get("LOOKUP_COLUMN"));
    stringBuffer.append(TEXT_11);
    stringBuffer.append(pattern );
    stringBuffer.append(TEXT_12);
    
        } else{
        
    stringBuffer.append(TEXT_13);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(mapCondition.get("INPUT_COLUMN"));
    stringBuffer.append(TEXT_16);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(mapCondition.get("LOOKUP_COLUMN"));
    stringBuffer.append(TEXT_18);
    
        }
      }
      
      public void generateOutput(String _outConnName, String _connNameLookUp, String _connNameMain
        , List<String> _listMainColsName, List<String> _listLUColsName, List<String> _listOutColsName, List<Map<String, String>> _listReplacedColsMapping
        , boolean _bReplacedByLookupRow, String _MATCHING_DISTANCES, String _cid){
      
    stringBuffer.append(TEXT_19);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_21);
    
        for (String currentOutputCol : _listOutColsName){
          if("MATCHING_DISTANCES".equals(currentOutputCol)){
          
    stringBuffer.append(TEXT_22);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(_MATCHING_DISTANCES);
    stringBuffer.append(TEXT_25);
    // fetch next column
            continue;
          }
         
          if (_bReplacedByLookupRow && (_listReplacedColsMapping.size() > 0)) {
            boolean bReadyReplaced = false;
            for (Map<String, String> colMapping : _listReplacedColsMapping){
              
              String toReplaceCol = colMapping.get("OUTPUT_COLUMN");
              String lookupCol = colMapping.get("LOOKUP_COLUMN");
              if (_listLUColsName != null && toReplaceCol.equals(currentOutputCol)){
              
    stringBuffer.append(TEXT_26);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(_connNameLookUp);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(lookupCol);
    stringBuffer.append(TEXT_30);
    
                bReadyReplaced = true;
                break;
              }
            }
            // current column not be filled in replaced table
            if (!bReadyReplaced){
              // main flow schema has current column, output its
              if (_listMainColsName.contains(currentOutputCol)){
              
    stringBuffer.append(TEXT_31);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(_connNameMain);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_35);
    } else if (_listLUColsName != null && _listLUColsName.contains(currentOutputCol)){ //  otherwise output its using lookup flow column
    stringBuffer.append(TEXT_36);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(_connNameLookUp);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_40);
    } else{ // current column not in main schema and lookup schema, output NULL
    stringBuffer.append(TEXT_41);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_43);
    
              }
            }
          } else {// no column defined in replaced table
            if (_listMainColsName.contains(currentOutputCol)){
            // main flow schema has current column, output its
            
    stringBuffer.append(TEXT_44);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(_connNameMain);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_48);
    } else if (_listLUColsName != null && _listLUColsName.contains(currentOutputCol)){ //  otherwise output its using lookup flow
    stringBuffer.append(TEXT_49);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(_connNameLookUp);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_53);
    } else{ // current column not in main schema and lookup schema, output NULL
    stringBuffer.append(TEXT_54);
    stringBuffer.append(_outConnName);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(currentOutputCol);
    stringBuffer.append(TEXT_56);
    
            }
          }
        }
      }
    }
    
    CodeGenerator codeGenerator = new CodeGenerator();
    
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
     
    if (iJoinSize > 0){
    
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(connNameLookUp);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(connNameLookUp);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(iJoinSize);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(connNameLookUp);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(connNameLookUp);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_74);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(connNameLookUp);
    stringBuffer.append(TEXT_76);
    
        if (listExactMatch != null){
          
    stringBuffer.append(TEXT_77);
    
          for (java.util.Map<String, String> exactCondition : listExactMatch){
            index ++;
        
    stringBuffer.append(TEXT_78);
    codeGenerator.initCompareCols(metadata, connNameMain, exactCondition, index);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_83);
    stringBuffer.append("false".equals(exactCondition.get("CASE_SENSITIVE")));
    stringBuffer.append(TEXT_84);
    
          }
        }
        
    stringBuffer.append(TEXT_85);
    
        if (listMetaphone != null){
          
    stringBuffer.append(TEXT_86);
    
          for (Map<String, String> metaphoneCodition : listMetaphone){
            index ++;
        
    codeGenerator.initCompareCols(metadata, connNameMain, metaphoneCodition, index);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_91);
    
          }
        }
        
    stringBuffer.append(TEXT_92);
    
        if (listDoubleMetaphone != null){
          
    stringBuffer.append(TEXT_93);
    
          for (Map<String, String> doubleMetaphoneCodition : listDoubleMetaphone){
            index ++;
        
    codeGenerator.initCompareCols(metadata, connNameMain, doubleMetaphoneCodition, index);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_98);
    
          }
        }
        
    stringBuffer.append(TEXT_99);
    
        if (listLevenshtein != null){
        
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
            
          for (Map<String, String> levenshteinCodition : listLevenshtein){
            index ++;
            int iDistanceMin = Integer.parseInt(levenshteinCodition.get("MIN_DISTANCE"));
            int iDistanceMax = Integer.parseInt(levenshteinCodition.get("MAX_DISTANCE"));
          
    codeGenerator.initCompareCols(metadata, connNameMain, levenshteinCodition, index);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_107);
    stringBuffer.append("false".equals(levenshteinCodition.get("CASE_SENSITIVE")));
    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(levenshteinCodition.get("INDEX"));
    stringBuffer.append(TEXT_110);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(iDistanceMin);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(iDistanceMin);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(iDistanceMax);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(iDistanceMax);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    
          }
        }
        
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    if (connNameMatchesOut != null){
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    codeGenerator.generateOutput(connNameMatchesOut, connNameLookUp, connNameMain, listMainColsName, listLUColsName, listOutColsName, listReplacedColsMapping, bReplacedByLookupRow, "s_MATCHING_DISTANCES_".concat(cid), cid);
    stringBuffer.append(TEXT_145);
    }
    stringBuffer.append(TEXT_146);
    if (connNamePosMatchOut != null){
    stringBuffer.append(TEXT_147);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    codeGenerator.generateOutput(connNamePosMatchOut, "firstPossibleMatch_".concat(cid), connNameMain, listMainColsName, listLUColsName, listOutColsName, listReplacedColsMapping, bReplacedByLookupRow, "sAllDistance_".concat(cid), cid);
    stringBuffer.append(TEXT_150);
    }
    } // end (iJoinSize > 0)
    
    stringBuffer.append(TEXT_151);
    if (connNameNonMatchOut != null){
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    codeGenerator.generateOutput(connNameNonMatchOut, connNameLookUp, connNameMain, listMainColsName, null, listOutColsName, listReplacedColsMapping, bReplacedByLookupRow, "null", cid);
    stringBuffer.append(TEXT_155);
    }
    
  }
}

    stringBuffer.append(TEXT_156);
    stringBuffer.append(TEXT_157);
    return stringBuffer.toString();
  }
}
