package org.talend.designer.codegen.translators.technical;

import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.INode;

public class TSurviveInMainJava
{
  protected static String nl;
  public static synchronized TSurviveInMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSurviveInMainJava result = new TSurviveInMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "            \t\t\t\t    ";
  protected final String TEXT_3 = ".";
  protected final String TEXT_4 = " = aggregated_row_";
  protected final String TEXT_5 = ".";
  protected final String TEXT_6 = ";" + NL + "            \t\t\t\t    ";
  protected final String TEXT_7 = NL + "            \t\t\t\t    String s_";
  protected final String TEXT_8 = "_";
  protected final String TEXT_9 = "_";
  protected final String TEXT_10 = " = String.valueOf(aggregated_row_";
  protected final String TEXT_11 = ".";
  protected final String TEXT_12 = ");" + NL + "            \t\t\t\t    ";
  protected final String TEXT_13 = ".";
  protected final String TEXT_14 = " = ";
  protected final String TEXT_15 = "s_";
  protected final String TEXT_16 = "_";
  protected final String TEXT_17 = "_";
  protected final String TEXT_18 = "s_";
  protected final String TEXT_19 = "_";
  protected final String TEXT_20 = "_";
  protected final String TEXT_21 = ".getBytes()";
  protected final String TEXT_22 = "(\"null\").equals(s_";
  protected final String TEXT_23 = "_";
  protected final String TEXT_24 = "_";
  protected final String TEXT_25 = ") ? null : ParserUtils.parseTo_Date(s_";
  protected final String TEXT_26 = "_";
  protected final String TEXT_27 = "_";
  protected final String TEXT_28 = ", ";
  protected final String TEXT_29 = ")";
  protected final String TEXT_30 = "ParserUtils.parseTo_";
  protected final String TEXT_31 = "(s_";
  protected final String TEXT_32 = "_";
  protected final String TEXT_33 = "_";
  protected final String TEXT_34 = ")";
  protected final String TEXT_35 = ";" + NL + "            \t\t\t\t    ";
  protected final String TEXT_36 = NL + "                                if(aggregated_row_";
  protected final String TEXT_37 = ".";
  protected final String TEXT_38 = "_count > 0){" + NL + "                                \t";
  protected final String TEXT_39 = NL + "\t    \t\t\t\t\t\t\t\t";
  protected final String TEXT_40 = ".";
  protected final String TEXT_41 = " = aggregated_row_";
  protected final String TEXT_42 = ".";
  protected final String TEXT_43 = "_sum.divide(new BigDecimal(String.valueOf(aggregated_row_";
  protected final String TEXT_44 = ".";
  protected final String TEXT_45 = "_count)), ";
  protected final String TEXT_46 = ", BigDecimal.ROUND_HALF_UP)" + NL + "\t    \t\t\t\t\t\t\t\t";
  protected final String TEXT_47 = NL + "\t    \t\t\t\t\t\t\t\t\t.";
  protected final String TEXT_48 = "Value()" + NL + "\t    \t\t\t\t\t\t\t\t";
  protected final String TEXT_49 = NL + "\t    \t\t\t\t\t\t\t\t;" + NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_50 = NL + "\t    \t\t\t\t\t\t\t\tdouble ";
  protected final String TEXT_51 = "_";
  protected final String TEXT_52 = "_temp = (double) aggregated_row_";
  protected final String TEXT_53 = ".";
  protected final String TEXT_54 = "_sum / (double) aggregated_row_";
  protected final String TEXT_55 = ".";
  protected final String TEXT_56 = "_count;" + NL + "\t    \t\t\t\t\t\t\t\t" + NL + "\t    \t\t\t\t\t\t\t\t";
  protected final String TEXT_57 = ".";
  protected final String TEXT_58 = " = String.valueOf(";
  protected final String TEXT_59 = "_";
  protected final String TEXT_60 = "_temp);" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_61 = ".";
  protected final String TEXT_62 = " = (";
  protected final String TEXT_63 = ") ";
  protected final String TEXT_64 = "_";
  protected final String TEXT_65 = "_temp;" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_66 = NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_67 = NL + "                                }";
  protected final String TEXT_68 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_69 = ".";
  protected final String TEXT_70 = " = new BigDecimal(aggregated_row_";
  protected final String TEXT_71 = ".count);" + NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_72 = ".";
  protected final String TEXT_73 = " = new BigDecimal(aggregated_row_";
  protected final String TEXT_74 = ".";
  protected final String TEXT_75 = "_clmCount);" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_76 = ".";
  protected final String TEXT_77 = " = String.valueOf(aggregated_row_";
  protected final String TEXT_78 = ".count);" + NL + "\t    \t\t\t\t\t\t\t\t  ";
  protected final String TEXT_79 = ".";
  protected final String TEXT_80 = " = String.valueOf(aggregated_row_";
  protected final String TEXT_81 = ".";
  protected final String TEXT_82 = "_clmCount);" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_83 = ".";
  protected final String TEXT_84 = " = (";
  protected final String TEXT_85 = ") aggregated_row_";
  protected final String TEXT_86 = ".count;" + NL + "\t                                \t";
  protected final String TEXT_87 = ".";
  protected final String TEXT_88 = " = (";
  protected final String TEXT_89 = ") aggregated_row_";
  protected final String TEXT_90 = ".";
  protected final String TEXT_91 = "_clmCount;" + NL + "\t                                \t";
  protected final String TEXT_92 = NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_93 = ".";
  protected final String TEXT_94 = " = new BigDecimal(aggregated_row_";
  protected final String TEXT_95 = ".distinctValues.size());" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_96 = ".";
  protected final String TEXT_97 = " = String.valueOf(aggregated_row_";
  protected final String TEXT_98 = ".distinctValues.size());" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_99 = ".";
  protected final String TEXT_100 = " = (";
  protected final String TEXT_101 = ") aggregated_row_";
  protected final String TEXT_102 = ".distinctValues.size();" + NL + "\t                                \t";
  protected final String TEXT_103 = ".";
  protected final String TEXT_104 = " = String.valueOf(aggregated_row_";
  protected final String TEXT_105 = ".";
  protected final String TEXT_106 = "_";
  protected final String TEXT_107 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_108 = NL + "    \t\t\t\t\t\t\t\tif(aggregated_row_";
  protected final String TEXT_109 = ".";
  protected final String TEXT_110 = "_";
  protected final String TEXT_111 = " != null) {" + NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_112 = ".";
  protected final String TEXT_113 = " = aggregated_row_";
  protected final String TEXT_114 = ".";
  protected final String TEXT_115 = "_";
  protected final String TEXT_116 = ".";
  protected final String TEXT_117 = "Value();" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_118 = NL + "    \t\t\t\t\t\t\t\t}" + NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_119 = ".";
  protected final String TEXT_120 = " = aggregated_row_";
  protected final String TEXT_121 = ".";
  protected final String TEXT_122 = "_";
  protected final String TEXT_123 = ";" + NL + "                                \t";
  protected final String TEXT_124 = NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_125 = ".";
  protected final String TEXT_126 = " = aggregated_row_";
  protected final String TEXT_127 = ".";
  protected final String TEXT_128 = "_";
  protected final String TEXT_129 = ".toString();" + NL + "\t    \t\t\t\t\t\t";
  protected final String TEXT_130 = NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_131 = ".";
  protected final String TEXT_132 = " = aggregated_row_";
  protected final String TEXT_133 = ".";
  protected final String TEXT_134 = "_";
  protected final String TEXT_135 = ";" + NL + "    \t\t\t\t\t\t\t";
  protected final String TEXT_136 = "double result_";
  protected final String TEXT_137 = "_";
  protected final String TEXT_138 = "_";
  protected final String TEXT_139 = " = utilClass_";
  protected final String TEXT_140 = ".sd(aggregated_row_";
  protected final String TEXT_141 = ".";
  protected final String TEXT_142 = "_";
  protected final String TEXT_143 = ".toArray(new Double[0]));" + NL + "\t\t    \t\t\t\t\t\t\tif(((Double)result_";
  protected final String TEXT_144 = "_";
  protected final String TEXT_145 = "_";
  protected final String TEXT_146 = ").equals((Double)Double.NaN)) {" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_147 = ".";
  protected final String TEXT_148 = " = null;" + NL + "\t\t    \t\t\t\t\t\t\t} else {" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_149 = ".";
  protected final String TEXT_150 = " = new BigDecimal(result_";
  protected final String TEXT_151 = "_";
  protected final String TEXT_152 = "_";
  protected final String TEXT_153 = ");" + NL + "\t\t    \t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_154 = ".";
  protected final String TEXT_155 = " = (";
  protected final String TEXT_156 = ") utilClass_";
  protected final String TEXT_157 = ".sd(aggregated_row_";
  protected final String TEXT_158 = ".";
  protected final String TEXT_159 = "_";
  protected final String TEXT_160 = ".toArray(new Double[0]));" + NL + "\t    \t\t\t\t\t\t\t\t";
  protected final String TEXT_161 = ".";
  protected final String TEXT_162 = " = String.valueOf(utilClass_";
  protected final String TEXT_163 = ".sd(aggregated_row_";
  protected final String TEXT_164 = ".";
  protected final String TEXT_165 = "_";
  protected final String TEXT_166 = ".toArray(new Double[0])));" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_167 = NL + "    \t\t\t\t\t\t\t\t// MOD scorreia 2010-02-17 " + NL + "    \t\t\t\t\t\t\t\t";
  protected final String TEXT_168 = ".";
  protected final String TEXT_169 = " = aggregated_row_";
  protected final String TEXT_170 = ".";
  protected final String TEXT_171 = "_";
  protected final String TEXT_172 = " == null ? null : aggregated_row_";
  protected final String TEXT_173 = ".";
  protected final String TEXT_174 = "_";
  protected final String TEXT_175 = ".toString();" + NL + "\t    \t\t\t\t\t\t";
  protected final String TEXT_176 = NL + "                                ";
  protected final String TEXT_177 = ".";
  protected final String TEXT_178 = " = aggregated_row_";
  protected final String TEXT_179 = ".";
  protected final String TEXT_180 = "_";
  protected final String TEXT_181 = ";";
  protected final String TEXT_182 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String origin = ElementParameterParser.getValue(node, "__ORIGIN__");
String cid = origin;

boolean useFinancialPrecision = "true".equals(ElementParameterParser.getValue(node, "__USE_FINANCIAL_PRECISION__"));

String SUM = "sum";
String COUNT = "count";
String MAX = "max";
String MIN = "min";
String FIRST = "first";
String LAST = "last";
String AVG = "avg";
String COUNT_DISTINCT = "distinct";
String LIST = "list";
String LIST_OBJECT = "list_object";
String STD_DEV = "std_dev";
String MAX_LENGTH = "max_length";
String MAX_WEIGHT = "max_weight";

List<Map<String, String>> operations = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__OPERATIONS__");
int sizeOperations = operations.size();
java.util.Map<String,IMetadataColumn> inputValuesColumns = new java.util.HashMap<String,IMetadataColumn>();
java.util.Map<String,IMetadataColumn> weightValuesColumns = new java.util.HashMap<String,IMetadataColumn>();

IConnection inputConn = null;
IMetadataTable inputMetadataTable = null;
java.util.List<IMetadataColumn> inputColumns = null;



String searchedComponentName = cid + "_AGGOUT";
List<? extends IConnection> incomingConnections = null;
List<? extends INode> generatedNodes = node.getProcess().getGeneratingNodes();
for(INode loopNode : generatedNodes) {
	if(loopNode.getUniqueName().equals(searchedComponentName)) {
		incomingConnections = (List<IConnection>) loopNode.getIncomingConnections();
		break;
	}
}

if (incomingConnections != null && !incomingConnections.isEmpty()) {
	for (IConnection conn : incomingConnections) {
		inputConn = conn;
		inputMetadataTable = conn.getMetadataTable();
		inputColumns = inputMetadataTable.getListColumns();
		break;
	}
}
if(inputColumns != null) { // T_AggR_144
	for (IMetadataColumn column: inputColumns) { // T_AggR_145

		for(int i = 0; i < sizeOperations; i++){ // T_AggR_713
			String columnname = operations.get(i).get("INPUT_COLUMN");
        	if(column.getLabel().equals(columnname)){ // T_AggR_714
       			inputValuesColumns.put(columnname, column);
				break;
       		} // T_AggR_714
		} // T_AggR_713

		/* MOD scorreia */ 
		for(int i = 0; i < sizeOperations; i++){ // T_AggR_913
			String columnname = operations.get(i).get("WEIGHT_COLUMN");
        	if(column.getLabel().equals(columnname)){ // T_AggR_914
       			weightValuesColumns.put(columnname, column);
				break;
       		} // T_AggR_914
		} // T_AggR_913					
				
	} // T_AggR_145
} // T_AggR_144



List<IMetadataTable> mestadataTableListOut = node.getMetadataList();

if (mestadataTableListOut!=null && mestadataTableListOut.size()>0) { // T_InMain_AggR_600
    IMetadataTable metadataTableOutput = mestadataTableListOut.get(0);
    if (metadataTableOutput!=null) { // T_InMain_AggR_601
        List<Map<String, String>> groupbys = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__GROUPBYS__");
		int sizeGroupbys = groupbys.size();
		
		IConnection outputConn = null;
		List< ? extends IConnection> outConns = node.getOutgoingSortedConnections();
		if (outConns!=null) {
			if (outConns.size()>0) {
				IConnection conn = outConns.get(0);
				if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
					outputConn = conn;    					
				}
			}
		}


		List<IMetadataColumn> outputColumns = metadataTableOutput.getListColumns();
		int sizeColumns = outputColumns.size();
		if(sizeOperations > 0 || sizeGroupbys > 0){ // T_InMain_AggR_603

    			next_column:
    			for(int c = 0; c < sizeColumns; c++){ // T_InMain_AggR_604
    				IMetadataColumn outputColumn = outputColumns.get(c);
    				String outputColumnName = outputColumn.getLabel();
    				String outputTypeToGenerate = JavaTypesManager.getTypeToGenerate(outputColumn.getTalendType(), outputColumn.isNullable());
    				String primitiveOutputType = JavaTypesManager.getTypeToGenerate(outputColumn.getTalendType(), false);
    				JavaType outputJavaType = JavaTypesManager.getJavaTypeFromId(outputColumn.getTalendType());
    				String patternValue = outputColumn.getPattern() == null || outputColumn.getPattern().trim().length() == 0 ? null : outputColumn.getPattern();
    				
    				JavaType inputJavaType = null;
    				
    				for(int g = 0; g < sizeGroupbys; g++){ // T_InMain_AggR_605
    					Map<String, String> groupby = groupbys.get(g);
    					String inputColumn = groupby.get("INPUT_COLUMN");
    					String outputGroupColumn = groupby.get("OUTPUT_COLUMN");
    					if(outputGroupColumn.equals(outputColumnName)){ // T_InMain_AggR_606
    						Boolean sameType = false;
                			
            				if (incomingConnections != null && !incomingConnections.isEmpty()) {
            					loop:
            					for (IConnection conn : incomingConnections) {
            						IMetadataTable inMetadata = conn.getMetadataTable();
            						for (IMetadataColumn inColumn: inMetadata.getListColumns()) {
            							if(inColumn.getLabel().equals(inputColumn)){
            								inputJavaType = JavaTypesManager.getJavaTypeFromId(inColumn.getTalendType());
            								sameType = (inputJavaType == outputJavaType);
            								break loop;
            							}
            						}
                				}
            				}
            				if(sameType){
            				    
    stringBuffer.append(TEXT_2);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_6);
      					
            				}else{
            				    
    stringBuffer.append(TEXT_7);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_12);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_13);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_14);
    
    							if(outputJavaType == JavaTypesManager.STRING || outputJavaType == JavaTypesManager.OBJECT) {
    								
    stringBuffer.append(TEXT_15);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid );
    
    							}else if(outputJavaType == JavaTypesManager.BYTE_ARRAY){
    								
    stringBuffer.append(TEXT_18);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    
    							} else if(outputJavaType == JavaTypesManager.DATE) {
    								
    stringBuffer.append(TEXT_22);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_23);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_26);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_29);
    
    							} else {
    								
    stringBuffer.append(TEXT_30);
    stringBuffer.append( outputTypeToGenerate );
    stringBuffer.append(TEXT_31);
    stringBuffer.append(inputColumn );
    stringBuffer.append(TEXT_32);
    stringBuffer.append(outputGroupColumn );
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    
    							}
            				    
    stringBuffer.append(TEXT_35);
    
    						}
    						continue next_column;
    					} // T_InMain_AggR_606
    				} // T_InMain_AggR_605
				
    				for(int o = 0; o < sizeOperations; o++){ // T_InMain_AggR_615
    					Map<String, String> operation = operations.get(o);
                		String function = operation.get("FUNCTION");
                		String outOperation = operation.get("OUTPUT_COLUMN");
                		String inOperation = operation.get("INPUT_COLUMN");
                		String weightColumnName = operation.get("WEIGHT_COLUMN");
                		
						boolean ignoreNull = ("true").equals(operation.get("IGNORE_NULL"));

                		if(outOperation.equals(outputColumnName)){ // T_InMain_AggR_616

							IMetadataColumn inputColumn = inputValuesColumns.get(inOperation);
							IMetadataColumn weightColumn = weightValuesColumns.get(weightColumnName); // MOD scorreia

							inputJavaType = JavaTypesManager.getJavaTypeFromId(inputColumn.getTalendType());
							JavaType weightJavaType = JavaTypesManager.getJavaTypeFromId(weightColumn.getTalendType());

							boolean outputIsNumber = JavaTypesManager.isNumberType(outputJavaType, false);
							boolean outputIsObject = outputJavaType == JavaTypesManager.OBJECT;
							boolean outputIsList = outputJavaType == JavaTypesManager.LIST;
							boolean outputIsString = outputJavaType == JavaTypesManager.STRING;
							boolean outputIsBigDecimal = outputJavaType == JavaTypesManager.BIGDECIMAL;
							boolean outputIsDate = outputJavaType == JavaTypesManager.DATE;
							boolean outputIsDecimal = outputJavaType == JavaTypesManager.FLOAT || outputJavaType == JavaTypesManager.DOUBLE || outputIsBigDecimal;
							
							boolean inputIsNumber = JavaTypesManager.isNumberType(inputJavaType, false);
							boolean inputIsObject = inputJavaType == JavaTypesManager.OBJECT;
							boolean inputIsBoolean = inputJavaType == JavaTypesManager.BOOLEAN;
							boolean inputIsList = inputJavaType == JavaTypesManager.LIST;
							boolean inputIsString = inputJavaType == JavaTypesManager.STRING;
							boolean inputIsDate = inputJavaType == JavaTypesManager.DATE;
							boolean inputIsBigDecimal = inputJavaType == JavaTypesManager.BIGDECIMAL;
							boolean inputIsByteArray = inputJavaType == JavaTypesManager.BYTE_ARRAY;
							boolean inputIsDecimal = inputJavaType == JavaTypesManager.FLOAT || inputJavaType == JavaTypesManager.DOUBLE || inputIsBigDecimal;
				
							/* weight type */
							boolean weightIsNumberOrDate = JavaTypesManager.isNumberType(weightJavaType, false) || weightJavaType == JavaTypesManager.DATE;
							boolean weightIsPrimitiveType = JavaTypesManager.isJavaPrimitiveType(weightJavaType, weightColumn.isNullable());
				
							boolean forceUseBigDecimal = 
								(function.equals(SUM) || function.equals(AVG)) 
								&& inputIsDecimal
								&& outputIsDecimal
								&& useFinancialPrecision
							;
	
							boolean sameInOutType = outputJavaType == inputJavaType;
					
							boolean isValidTypeForOperation = 
								(function.equals(SUM) || function.equals(AVG)) && inputIsNumber && outputIsNumber
								|| function.equals(MIN) && sameInOutType && !inputIsList && !inputIsByteArray && !inputIsBoolean 
								|| function.equals(MAX) && sameInOutType && !inputIsList && !inputIsByteArray && !inputIsBoolean
								|| function.equals(FIRST) && sameInOutType
								|| function.equals(LAST) && sameInOutType
								|| function.equals(LIST) && outputIsString
								|| function.equals(LIST_OBJECT) && outputIsList
								|| function.equals(COUNT) && outputIsNumber
								|| function.equals(COUNT_DISTINCT) && outputIsNumber
								|| function.equals(STD_DEV) && inputIsNumber && outputIsNumber
								|| function.equals(MAX_LENGTH) && outputIsString /* MOD scorreia */			
								|| function.equals(MAX_WEIGHT) && sameInOutType && weightIsNumberOrDate
							;
	
							if(!isValidTypeForOperation) {
								continue;
							}
            				
                			if(function.equals(AVG)){ // T_InMain_AggR_617
                			
                				int pre = 10;
                       		 	Integer precision = outputColumn.getPrecision();
                           		 if(precision!=null && precision!=0) { 
                           		 	pre = precision;
                           		 }               				

                			    
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_38);
    
									if(outputIsBigDecimal || forceUseBigDecimal) {
									
    stringBuffer.append(TEXT_39);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_44);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(pre );
    stringBuffer.append(TEXT_46);
     if(!outputIsBigDecimal) {
    stringBuffer.append(TEXT_47);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_48);
     } 
    stringBuffer.append(TEXT_49);
    
									} else {
									
    stringBuffer.append(TEXT_50);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_53);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_56);
     
	    								if(outputIsString) {
	    								
		    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_58);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_60);
    
											
										} else {
		    							
		    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_61);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_62);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_63);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_64);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_65);
    
										
										}
										
    stringBuffer.append(TEXT_66);
    
    								}
    								
    stringBuffer.append(TEXT_67);
    
                			}  // T_InMain_AggR_617
                			else if(function.equals(COUNT)) { // T_InMain_AggR_638
                			
								if(outputIsBigDecimal) {
								
    stringBuffer.append(TEXT_68);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_69);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_71);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_72);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_74);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_75);
    
								} else {
            			
									if(outputIsString) {
									
	    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_76);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_78);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_79);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_81);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_82);
    
										
									} else {
	    							
	                                	
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_83);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_84);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_86);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_87);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_88);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_90);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_91);
    
									
									}
								}

							} // T_InMain_AggR__638
                			else if(function.equals(COUNT_DISTINCT)) { // T_InMain_AggR_658
                			
								if(outputIsBigDecimal) {
								
    stringBuffer.append(TEXT_92);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_93);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_95);
    
								} else {
            			
									if(outputIsString) {
									
	    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_96);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_98);
    
										
									} else {
	    							
	                                	
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_99);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_100);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_102);
    
									
									}
								}

							} // T_InMain_AggR__638
                			else if(function.equals(SUM)) { // T_InMain_AggR_618

								if(outputIsString) {
								
    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_103);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_105);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_106);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_107);
    
									
								} else if(forceUseBigDecimal && !outputIsBigDecimal) {

    								
    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_109);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_110);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_111);
    
    								
	    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_112);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_114);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_115);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_116);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_117);
    

    								
    stringBuffer.append(TEXT_118);
    
								
								} else {
    							
                                	
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_119);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_121);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_122);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_123);
    
								
								}
									
    						} // T_InMain_AggR_618 
    						else if(function.equals(LIST)) { // T_InMain_AggR_679
    							
    stringBuffer.append(TEXT_124);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_125);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_127);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_128);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_129);
     
    						}  // T_InMain_AggR_679
    						else if(("list_object").equals(function)) { // T_InMain_AggR_619
    							
    stringBuffer.append(TEXT_130);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_131);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_133);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_134);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_135);
     
    						}  // T_InMain_AggR_619
    						else if(function.equals(STD_DEV)) { // T_InMain_AggR_620
    						
    						
    							if(outputIsNumber || outputIsObject){ // T_InMain_AggR_621

									if(outputIsBigDecimal) {
										
		    							
    stringBuffer.append(TEXT_136);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_137);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_141);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_142);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_143);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_144);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_146);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_147);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_148);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_149);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_150);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_151);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_153);
    
										
									} else {
	            			
	    								
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_154);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_155);
    stringBuffer.append(primitiveOutputType);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_158);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_159);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_160);
    
    								 		
									}

    							} // T_InMain_AggR_621
    							
    							else if(outputIsString){ // T_InMain_AggR_622
								
									
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_161);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_164);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_165);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_166);
    
									
    							} // T_InMain_AggR_622
    							
    						} // T_InMain_AggR_620
    						
    						else if(function.equals(MAX_LENGTH)) { // T_InMain_AggR_680
    							
    stringBuffer.append(TEXT_167);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_168);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_169);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_170);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_171);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_172);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_173);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_174);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_175);
     
    						}  // T_InMain_AggR_680
    						else { // T_InMain_AggR_636
    							
								
    stringBuffer.append(TEXT_176);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_177);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_178);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_179);
    stringBuffer.append(outOperation );
    stringBuffer.append(TEXT_180);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_181);
    
                                
    							
    						} // T_InMain_AggR_636

                			continue next_column;
                			
                		} // T_InMain_AggR_616

                		
                } // T_InMain_AggR_615
    		
    		} // T_InMain_AggR_604

		} // T_InMain_AggR_603

	} // T_InMain_AggR_601
} // T_InMain_AggR_600

    stringBuffer.append(TEXT_182);
    return stringBuffer.toString();
  }
}
