package org.talend.designer.codegen.translators.data_quality.survivorship;

import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import java.util.List;
import java.util.Map;
import org.talend.core.model.utils.NodeUtil;

public class TRuleSurvivorshipMainJava
{
  protected static String nl;
  public static synchronized TRuleSurvivorshipMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TRuleSurvivorshipMainJava result = new TRuleSurvivorshipMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t    \t\t\tif(groupSize_";
  protected final String TEXT_3 = " == 0){" + NL + "\t    \t\t\t\tgroupID_";
  protected final String TEXT_4 = " = ";
  protected final String TEXT_5 = ".";
  protected final String TEXT_6 = ";" + NL + "\t    \t\t\t\tgroupSize_";
  protected final String TEXT_7 = " = ";
  protected final String TEXT_8 = ".";
  protected final String TEXT_9 = ";" + NL + "\t    \t\t\t\tgroupCount_";
  protected final String TEXT_10 = " = 0;" + NL + "\t    \t\t\t\tgroupValues_";
  protected final String TEXT_11 = " = new Object[groupSize_";
  protected final String TEXT_12 = "][";
  protected final String TEXT_13 = "];" + NL + "\t    \t\t\t}" + NL + "\t    \t\t\tgroupCount_";
  protected final String TEXT_14 = "++;" + NL + "\t    \t\t\t";
  protected final String TEXT_15 = NL + "\t\t    \t\t\tgroupValues_";
  protected final String TEXT_16 = "[groupCount_";
  protected final String TEXT_17 = "-1][";
  protected final String TEXT_18 = "] = ";
  protected final String TEXT_19 = ".";
  protected final String TEXT_20 = ";" + NL + "\t    \t\t\t";
  protected final String TEXT_21 = NL + "\t    \t\t\tif(groupID_";
  protected final String TEXT_22 = " != null && !groupID_";
  protected final String TEXT_23 = ".equals(";
  protected final String TEXT_24 = ".";
  protected final String TEXT_25 = ")){" + NL + "\t    \t\t\t\tthrow new RuntimeException(\"the record [\"" + NL + "\t    \t\t\t\t\t\t\t\t\t";
  protected final String TEXT_26 = NL + "\t    \t\t\t\t\t\t\t\t\t\t+ ";
  protected final String TEXT_27 = ".";
  protected final String TEXT_28 = NL + "\t    \t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_29 = NL + "\t    \t\t\t\t\t\t\t\t\t\t+ \",\"" + NL + "\t    \t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_30 = NL + "\t    \t\t\t\t\t\t\t\t\t+ \"] does not belong to this group (group identifier=\" " + NL + "\t    \t\t\t\t\t\t\t\t\t+ groupID_";
  protected final String TEXT_31 = " " + NL + "\t    \t\t\t\t\t\t\t\t\t+ \"). Please, make sure the records are sorted according to the group identifier \" " + NL + "\t    \t\t\t\t\t\t\t\t\t+ \"in the input of the tRuleSurvivorship component.\");" + NL + "\t    \t\t\t}" + NL + "\t    \t\t\t" + NL + "\t    \t\t\tint recordsCount_";
  protected final String TEXT_32 = " = 0;" + NL + "\t    \t\t\tif(groupCount_";
  protected final String TEXT_33 = " == groupSize_";
  protected final String TEXT_34 = "){" + NL + "\t    \t\t\t\tmanager_";
  protected final String TEXT_35 = ".runSession(groupValues_";
  protected final String TEXT_36 = ");" + NL + "\t    \t\t\t\trecordsCount_";
  protected final String TEXT_37 = " = groupValues_";
  protected final String TEXT_38 = ".length + 1;" + NL + "\t    \t\t\t\tsurvivors_";
  protected final String TEXT_39 = " = manager_";
  protected final String TEXT_40 = ".getSurvivorMap();" + NL + "\t    \t\t\t\tconflicts_";
  protected final String TEXT_41 = " = manager_";
  protected final String TEXT_42 = ".getConflictList();" + NL + "\t    \t\t\t\tgroupSize_";
  protected final String TEXT_43 = " = 0;" + NL + "\t    \t\t\t}" + NL + "\t    \t\t\tfor(int i_";
  protected final String TEXT_44 = " = 0; groupSize_";
  protected final String TEXT_45 = " == 0 && i_";
  protected final String TEXT_46 = " < recordsCount_";
  protected final String TEXT_47 = "; i_";
  protected final String TEXT_48 = "++){" + NL + "" + NL + "    \t\t\t\t\t";
  protected final String TEXT_49 = NL + "\t\t\t\t\t\t\tif(i_";
  protected final String TEXT_50 = " != recordsCount_";
  protected final String TEXT_51 = " - 1){" + NL + "\t\t\t\t\t\t\t\ttmpValue_";
  protected final String TEXT_52 = " = groupValues_";
  protected final String TEXT_53 = "[i_";
  protected final String TEXT_54 = "][";
  protected final String TEXT_55 = "];" + NL + "\t\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t\ttmpValue_";
  protected final String TEXT_56 = " = survivors_";
  protected final String TEXT_57 = ".get(\"";
  protected final String TEXT_58 = "\");" + NL + "\t\t\t\t\t\t\t}" + NL + "" + NL + "\t\t\t\t\t\t\tif(tmpValue_";
  protected final String TEXT_59 = " != null){" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_60 = "\t\t\t\t" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_61 = ".";
  protected final String TEXT_62 = " = tmpValue_";
  protected final String TEXT_63 = ".toString();" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_64 = NL + "\t\t\t\t\t\t\t\t\tif(tmpValue_";
  protected final String TEXT_65 = " instanceof java.util.Date){" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_66 = ".";
  protected final String TEXT_67 = " = (java.util.Date)tmpValue_";
  protected final String TEXT_68 = ";" + NL + "\t\t\t\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_69 = ".";
  protected final String TEXT_70 = " = ParserUtils.parseTo_Date(tmpValue_";
  protected final String TEXT_71 = ".toString(), ";
  protected final String TEXT_72 = ");" + NL + "\t\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_73 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_74 = ".";
  protected final String TEXT_75 = " = tmpValue_";
  protected final String TEXT_76 = ".toString().getBytes();" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_77 = "\t\t\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_78 = ".";
  protected final String TEXT_79 = " = ParserUtils.parseTo_";
  protected final String TEXT_80 = "(tmpValue_";
  protected final String TEXT_81 = ".toString());\t" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_82 = NL + "\t\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_83 = ".";
  protected final String TEXT_84 = " = ";
  protected final String TEXT_85 = ";" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_86 = NL + "\t\t\t\t\t\t\t\t" + NL + "\t    \t\t\t\tif(i_";
  protected final String TEXT_87 = " != recordsCount_";
  protected final String TEXT_88 = " - 1){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_89 = ".CONFLICT = conflicts_";
  protected final String TEXT_90 = ".get(i_";
  protected final String TEXT_91 = ").toString();" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_92 = ".SURVIVOR = false;" + NL + "\t\t\t\t\t\t}else{ // master record" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_93 = ".CONFLICT = manager_";
  protected final String TEXT_94 = ".getConflictsOfSurvivor().toString();" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_95 = ".SURVIVOR = true;";
  protected final String TEXT_96 = NL + "                            ";
  protected final String TEXT_97 = ".";
  protected final String TEXT_98 = " = ParserUtils.parseTo_";
  protected final String TEXT_99 = "(groupID_";
  protected final String TEXT_100 = ".toString()); ";
  protected final String TEXT_101 = NL + "                            ";
  protected final String TEXT_102 = ".";
  protected final String TEXT_103 = " = groupValues_";
  protected final String TEXT_104 = ".length;" + NL + "\t\t\t\t\t\t}" + NL + "\t\t    " + NL + "\t \t\t\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	List<IMetadataTable> metadatas = node.getMetadataList();
	
	if ((metadatas!=null) && (metadatas.size() > 0)) {//1
	    IMetadataTable metadata = metadatas.get(0);
	    if (metadata != null) {//2
	    	List< ? extends IConnection> conns = node.getIncomingConnections();
	    	for (IConnection conn : conns) {//3
	    		if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {//4
	    		
					String incomingConnName = null;
        			String outgoingConnName = null;
        			
					java.util.List<IMetadataColumn> inputColumns = null;
		        
				  	List< ? extends IConnection> inConns = node.getIncomingConnections();
			        if (inConns != null && !inConns.isEmpty()) {
			        	for (IConnection inConn : inConns) {
							IMetadataTable inputMetadataTable = inConn.getMetadataTable();
							inputColumns = inputMetadataTable.getListColumns();
				            incomingConnName = inConn.getName();
							break;
						}
			        }
			        
			        List< ? extends IConnection> outConns = NodeUtil.getOutgoingConnections(node, IConnectionCategory.DATA);
			        if (outConns != null && !outConns.isEmpty()) {
			            IConnection outConn = outConns.get(0);
			            outgoingConnName = outConn.getName();
			        } 
			        int nbMetadataColumns = inputColumns.size();
			        
			        String grpSizeColumnName = ElementParameterParser.getValue(node, "__GRP_SIZE__");
			        String grpIDColumnName = ElementParameterParser.getValue(node, "__GRP_ID__");
	    			
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(grpSizeColumnName);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
	    			String grpIDColumnType = "";
	    			for( int i = 0; i < nbMetadataColumns; i++) {
		    			IMetadataColumn metadataColumn = inputColumns.get(i);
		    			if(grpIDColumnName.equals(metadataColumn.getLabel())){
		    			    grpIDColumnType = JavaTypesManager.getTypeToGenerate(metadataColumn.getTalendType(), metadataColumn.isNullable());                            
		    			}
	    			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(metadataColumn.getLabel());
    stringBuffer.append(TEXT_20);
    
					}
	    			
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_25);
    for( int i = 0; i < nbMetadataColumns; i++) {
	    										IMetadataColumn metadataColumn = inputColumns.get(i);
	    										
    stringBuffer.append(TEXT_26);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(metadataColumn.getLabel());
    stringBuffer.append(TEXT_28);
    
	    										if(i != nbMetadataColumns - 1){
	    										
    stringBuffer.append(TEXT_29);
    
	    										}
	    									}
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
     
    					for( int i = 0; i < nbMetadataColumns; i++) {
							
			    			IMetadataColumn column = inputColumns.get(i);
			    						
			    			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			    			String defaultValue =JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
			    			JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
			    							
			    			String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
							
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    							
								if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
								
    stringBuffer.append(TEXT_60);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    
								} else if(javaType == JavaTypesManager.DATE) { // Date
								
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(patternValue);
    stringBuffer.append(TEXT_72);
    
								} else if(javaType == JavaTypesManager.BYTE_ARRAY) { //byte[]
								
    stringBuffer.append(TEXT_73);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    
								} else {
								
    stringBuffer.append(TEXT_77);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_79);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    
								} 
								
    stringBuffer.append(TEXT_82);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_84);
    stringBuffer.append(defaultValue);
    stringBuffer.append(TEXT_85);
    
						}
						
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(grpIDColumnType);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(grpSizeColumnName);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    
	 			}//4
	 		}//3
		}//2
	}//1

    return stringBuffer.toString();
  }
}
