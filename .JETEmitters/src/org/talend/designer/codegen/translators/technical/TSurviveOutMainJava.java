package org.talend.designer.codegen.translators.technical;

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

public class TSurviveOutMainJava
{
  protected static String nl;
  public static synchronized TSurviveOutMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSurviveOutMainJava result = new TSurviveOutMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = "operation_finder_";
  protected final String TEXT_3 = ".";
  protected final String TEXT_4 = " = ";
  protected final String TEXT_5 = ".";
  protected final String TEXT_6 = ";" + NL + "\t\t\t";
  protected final String TEXT_7 = NL + NL + "\toperation_finder_";
  protected final String TEXT_8 = ".hashCodeDirty = true;" + NL + "\t" + NL + "\toperation_result_";
  protected final String TEXT_9 = " = hash_";
  protected final String TEXT_10 = ".get(operation_finder_";
  protected final String TEXT_11 = ");" + NL + "" + NL + "\t";
  protected final String TEXT_12 = NL + "\t\tboolean isFirstAdd_";
  protected final String TEXT_13 = " = false;" + NL + "\t";
  protected final String TEXT_14 = NL + NL + "\tif(operation_result_";
  protected final String TEXT_15 = " == null) { // G_OutMain_AggR_001" + NL + "" + NL + "\t\toperation_result_";
  protected final String TEXT_16 = " = new AggOperationStruct_";
  protected final String TEXT_17 = "();" + NL + "" + NL + "\t\t";
  protected final String TEXT_18 = "operation_result_";
  protected final String TEXT_19 = ".";
  protected final String TEXT_20 = " = operation_finder_";
  protected final String TEXT_21 = ".";
  protected final String TEXT_22 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_23 = NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_24 = NL + "\t\t\tisFirstAdd_";
  protected final String TEXT_25 = " = true;" + NL + "\t\t";
  protected final String TEXT_26 = NL + NL + "\t\thash_";
  protected final String TEXT_27 = ".put(operation_result_";
  protected final String TEXT_28 = ", operation_result_";
  protected final String TEXT_29 = ");" + NL + "\t" + NL + "\t} // G_OutMain_AggR_001" + NL + "" + NL + "" + NL + "\t";
  protected final String TEXT_30 = NL + "\t\t\t\tif(";
  protected final String TEXT_31 = ".";
  protected final String TEXT_32 = " != null) { // G_OutMain_AggR_546" + NL + "\t\t\t\t";
  protected final String TEXT_33 = NL + "\t\t\t\t" + NL + "\t\t\t\tAggCountDistinctValuesStruct_";
  protected final String TEXT_34 = " countDistinctValues_";
  protected final String TEXT_35 = " = new AggCountDistinctValuesStruct_";
  protected final String TEXT_36 = "();" + NL + "\t\t\t" + NL + "\t\t\t\t";
  protected final String TEXT_37 = "countDistinctValues_";
  protected final String TEXT_38 = ".";
  protected final String TEXT_39 = " = ";
  protected final String TEXT_40 = ".";
  protected final String TEXT_41 = ";" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_42 = "operation_result_";
  protected final String TEXT_43 = ".distinctValues.add(countDistinctValues_";
  protected final String TEXT_44 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_45 = NL + "\t\t\t\toperation_result_";
  protected final String TEXT_46 = ".";
  protected final String TEXT_47 = "_clmCount++;";
  protected final String TEXT_48 = NL + "\t\t\t\toperation_result_";
  protected final String TEXT_49 = ".count++;" + NL + "\t\t\t\t";
  protected final String TEXT_50 = NL + "\t\t\t\toperation_result_";
  protected final String TEXT_51 = ".";
  protected final String TEXT_52 = "_count++;" + NL + "\t\t\t\t";
  protected final String TEXT_53 = NL + "\t\t\t\t\tif( " + NL + "\t\t\t\t\t\t";
  protected final String TEXT_54 = NL + "\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_55 = ".";
  protected final String TEXT_56 = "_";
  protected final String TEXT_57 = " == null || operation_result_";
  protected final String TEXT_58 = ".";
  protected final String TEXT_59 = "_";
  protected final String TEXT_60 = " instanceof java.lang.Comparable && " + NL + "\t\t\t\t\t\t\t((java.lang.Comparable) ";
  protected final String TEXT_61 = ".";
  protected final String TEXT_62 = ").compareTo(operation_result_";
  protected final String TEXT_63 = ".";
  protected final String TEXT_64 = "_";
  protected final String TEXT_65 = ") ";
  protected final String TEXT_66 = " 0" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_67 = NL + "\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_68 = ".";
  protected final String TEXT_69 = "_";
  protected final String TEXT_70 = " == null || ";
  protected final String TEXT_71 = ".";
  protected final String TEXT_72 = ".compareTo(operation_result_";
  protected final String TEXT_73 = ".";
  protected final String TEXT_74 = "_";
  protected final String TEXT_75 = ") ";
  protected final String TEXT_76 = " 0" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_77 = NL + "\t\t\t\t\t) {" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_78 = ".";
  protected final String TEXT_79 = "_";
  protected final String TEXT_80 = " = ";
  protected final String TEXT_81 = ".";
  protected final String TEXT_82 = ";" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_83 = NL + "\t\t\t\t\tif(" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_84 = "operation_result_";
  protected final String TEXT_85 = ".";
  protected final String TEXT_86 = "_";
  protected final String TEXT_87 = " == null || ";
  protected final String TEXT_88 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_89 = ".";
  protected final String TEXT_90 = " ";
  protected final String TEXT_91 = " operation_result_";
  protected final String TEXT_92 = ".";
  protected final String TEXT_93 = "_";
  protected final String TEXT_94 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_95 = " || isFirstAdd_";
  protected final String TEXT_96 = " ";
  protected final String TEXT_97 = NL + "\t\t\t\t\t) {" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_98 = ".";
  protected final String TEXT_99 = "_";
  protected final String TEXT_100 = " = ";
  protected final String TEXT_101 = ".";
  protected final String TEXT_102 = ";" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_103 = NL + "\t\t\t\t\tif(operation_result_";
  protected final String TEXT_104 = ".";
  protected final String TEXT_105 = "_";
  protected final String TEXT_106 = " == null) {" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_107 = ".";
  protected final String TEXT_108 = "_";
  protected final String TEXT_109 = " = (";
  protected final String TEXT_110 = ") 0;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_111 = NL + "\t\t\t\t\tif(operation_result_";
  protected final String TEXT_112 = ".";
  protected final String TEXT_113 = "_";
  protected final String TEXT_114 = " == null) {" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_115 = ".";
  protected final String TEXT_116 = "_";
  protected final String TEXT_117 = " = new BigDecimal(0);" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t\toperation_result_";
  protected final String TEXT_118 = ".";
  protected final String TEXT_119 = "_";
  protected final String TEXT_120 = " = operation_result_";
  protected final String TEXT_121 = ".";
  protected final String TEXT_122 = "_";
  protected final String TEXT_123 = ".add(" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_124 = "new BigDecimal(";
  protected final String TEXT_125 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_126 = "String.valueOf(";
  protected final String TEXT_127 = NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_128 = ".";
  protected final String TEXT_129 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_130 = ")";
  protected final String TEXT_131 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_132 = ")";
  protected final String TEXT_133 = NL + "\t\t\t\t\t);" + NL + "\t\t\t\t\t";
  protected final String TEXT_134 = "utilClass_";
  protected final String TEXT_135 = ".checkedIADD( (";
  protected final String TEXT_136 = ") operation_result_";
  protected final String TEXT_137 = ".";
  protected final String TEXT_138 = "_";
  protected final String TEXT_139 = ", (";
  protected final String TEXT_140 = ") ";
  protected final String TEXT_141 = ".";
  protected final String TEXT_142 = ", ";
  protected final String TEXT_143 = ".doubleValue(), ";
  protected final String TEXT_144 = ");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_145 = "operation_result_";
  protected final String TEXT_146 = ".";
  protected final String TEXT_147 = "_";
  protected final String TEXT_148 = " += ";
  protected final String TEXT_149 = ".";
  protected final String TEXT_150 = ".doubleValue();" + NL + "\t\t\t\t\t";
  protected final String TEXT_151 = "utilClass_";
  protected final String TEXT_152 = ".checkedIADD( (";
  protected final String TEXT_153 = ") operation_result_";
  protected final String TEXT_154 = ".";
  protected final String TEXT_155 = "_";
  protected final String TEXT_156 = ", (";
  protected final String TEXT_157 = ") ";
  protected final String TEXT_158 = ".";
  protected final String TEXT_159 = ", ";
  protected final String TEXT_160 = ", ";
  protected final String TEXT_161 = ");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_162 = "operation_result_";
  protected final String TEXT_163 = ".";
  protected final String TEXT_164 = "_";
  protected final String TEXT_165 = " = (";
  protected final String TEXT_166 = ")(operation_result_";
  protected final String TEXT_167 = ".";
  protected final String TEXT_168 = "_";
  protected final String TEXT_169 = ".";
  protected final String TEXT_170 = "Value() + ";
  protected final String TEXT_171 = ".";
  protected final String TEXT_172 = ".";
  protected final String TEXT_173 = "Value());" + NL + "\t\t\t\t\t";
  protected final String TEXT_174 = "operation_result_";
  protected final String TEXT_175 = ".";
  protected final String TEXT_176 = "_";
  protected final String TEXT_177 = " += ";
  protected final String TEXT_178 = ".";
  protected final String TEXT_179 = ";";
  protected final String TEXT_180 = NL + "\t\t\t\tif(isFirstAdd_";
  protected final String TEXT_181 = " ";
  protected final String TEXT_182 = " || operation_result_";
  protected final String TEXT_183 = ".";
  protected final String TEXT_184 = "_";
  protected final String TEXT_185 = " == null";
  protected final String TEXT_186 = ") {" + NL + "\t\t\t\t\toperation_result_";
  protected final String TEXT_187 = ".";
  protected final String TEXT_188 = "_";
  protected final String TEXT_189 = " = ";
  protected final String TEXT_190 = ".";
  protected final String TEXT_191 = ";" + NL + "\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_192 = NL + "\t\t\t\t\toperation_result_";
  protected final String TEXT_193 = ".";
  protected final String TEXT_194 = "_";
  protected final String TEXT_195 = " = ";
  protected final String TEXT_196 = ".";
  protected final String TEXT_197 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_198 = NL + "\t\t\t\t\t// MOD scorreia 2010-02-17 store the longest string" + NL + "\t\t\t\t\tif (";
  protected final String TEXT_199 = ".";
  protected final String TEXT_200 = " != null) {" + NL + "\t\t\t\t\t\tif (operation_result_";
  protected final String TEXT_201 = ".";
  protected final String TEXT_202 = "_";
  protected final String TEXT_203 = " == null) { " + NL + "\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_204 = ".";
  protected final String TEXT_205 = "_";
  protected final String TEXT_206 = " =  ";
  protected final String TEXT_207 = ".";
  protected final String TEXT_208 = ";" + NL + "\t\t\t\t\t\t} else if (";
  protected final String TEXT_209 = ".";
  protected final String TEXT_210 = ".length() > operation_result_";
  protected final String TEXT_211 = ".";
  protected final String TEXT_212 = "_";
  protected final String TEXT_213 = ".length()) {\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_214 = ".";
  protected final String TEXT_215 = "_";
  protected final String TEXT_216 = " =  ";
  protected final String TEXT_217 = ".";
  protected final String TEXT_218 = ";" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_219 = NL + "\t\t\t\t\t\t// MOD scorreia 2010-02-17 store the attribute with the highest weight" + NL + "\t\t\t\t\t\tif (";
  protected final String TEXT_220 = ".";
  protected final String TEXT_221 = " > operation_result_";
  protected final String TEXT_222 = ".";
  protected final String TEXT_223 = "_weight ) {" + NL + "\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_224 = ".";
  protected final String TEXT_225 = "_weight = ";
  protected final String TEXT_226 = ".";
  protected final String TEXT_227 = ";\t" + NL + "\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_228 = ".";
  protected final String TEXT_229 = "_";
  protected final String TEXT_230 = " =  ";
  protected final String TEXT_231 = ".";
  protected final String TEXT_232 = ";\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t " + NL + "\t\t\t\t\t\t";
  protected final String TEXT_233 = NL + "\t\t\t\t\t\t\t// MOD scorreia 2010-02-17 store the attribute with the highest weight" + NL + "\t\t\t\t\t\t\tif (operation_result_";
  protected final String TEXT_234 = ".";
  protected final String TEXT_235 = "_weight == null) {" + NL + "\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_236 = ".";
  protected final String TEXT_237 = "_weight = ";
  protected final String TEXT_238 = ".";
  protected final String TEXT_239 = ";\t" + NL + "\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_240 = ".";
  protected final String TEXT_241 = "_";
  protected final String TEXT_242 = " =  ";
  protected final String TEXT_243 = ".";
  protected final String TEXT_244 = ";\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t} else { " + NL + "\t\t\t\t\t\t\t\tif ((";
  protected final String TEXT_245 = ".";
  protected final String TEXT_246 = " != null) " + NL + "\t\t\t\t\t\t\t\t\t&& (";
  protected final String TEXT_247 = ".";
  protected final String TEXT_248 = ".compareTo(operation_result_";
  protected final String TEXT_249 = ".";
  protected final String TEXT_250 = "_weight) > 0 )) {" + NL + "\t\t\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_251 = ".";
  protected final String TEXT_252 = "_weight = ";
  protected final String TEXT_253 = ".";
  protected final String TEXT_254 = ";\t" + NL + "\t\t\t\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_255 = ".";
  protected final String TEXT_256 = "_";
  protected final String TEXT_257 = " =  ";
  protected final String TEXT_258 = ".";
  protected final String TEXT_259 = ";\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t " + NL + "\t\t\t\t\t\t";
  protected final String TEXT_260 = NL + "\t\t\t\tif(operation_result_";
  protected final String TEXT_261 = ".";
  protected final String TEXT_262 = "_";
  protected final String TEXT_263 = ".length() > 0) {" + NL + "\t\t\t\t\toperation_result_";
  protected final String TEXT_264 = ".";
  protected final String TEXT_265 = "_";
  protected final String TEXT_266 = ".append(";
  protected final String TEXT_267 = ");" + NL + "\t\t\t\t} " + NL + "\t\t\t\t";
  protected final String TEXT_268 = NL + "\t\t\t\t\tif(operation_result_";
  protected final String TEXT_269 = ".";
  protected final String TEXT_270 = "_";
  protected final String TEXT_271 = " != null) {" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_272 = ".";
  protected final String TEXT_273 = "_";
  protected final String TEXT_274 = " = operation_result_";
  protected final String TEXT_275 = ".";
  protected final String TEXT_276 = "_";
  protected final String TEXT_277 = ".append(java.util.Arrays.toString(";
  protected final String TEXT_278 = ".";
  protected final String TEXT_279 = "));" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_280 = "else if(operation_result_";
  protected final String TEXT_281 = ".";
  protected final String TEXT_282 = "_";
  protected final String TEXT_283 = "_firstEmpty){" + NL + "\t\t\t\t\toperation_result_";
  protected final String TEXT_284 = ".";
  protected final String TEXT_285 = "_";
  protected final String TEXT_286 = ".append(";
  protected final String TEXT_287 = ");" + NL + "\t\t\t\t}" + NL + "\t\t\t\t\tif(operation_result_";
  protected final String TEXT_288 = ".";
  protected final String TEXT_289 = "_";
  protected final String TEXT_290 = " != null) {" + NL + "\t\t\t\t\t\tif(operation_result_";
  protected final String TEXT_291 = ".";
  protected final String TEXT_292 = "_";
  protected final String TEXT_293 = "_firstEmpty==false && (\"\").equals(String.valueOf(";
  protected final String TEXT_294 = ".";
  protected final String TEXT_295 = "))){" + NL + "\t\t\t\t\t\t\toperation_result_";
  protected final String TEXT_296 = ".";
  protected final String TEXT_297 = "_";
  protected final String TEXT_298 = "_firstEmpty = true;" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\toperation_result_";
  protected final String TEXT_299 = ".";
  protected final String TEXT_300 = "_";
  protected final String TEXT_301 = " = operation_result_";
  protected final String TEXT_302 = ".";
  protected final String TEXT_303 = "_";
  protected final String TEXT_304 = ".append(String.valueOf(";
  protected final String TEXT_305 = ".";
  protected final String TEXT_306 = "));" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_307 = NL + "\t\t\t\toperation_result_";
  protected final String TEXT_308 = ".";
  protected final String TEXT_309 = "_";
  protected final String TEXT_310 = ".add(";
  protected final String TEXT_311 = ".";
  protected final String TEXT_312 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_313 = "operation_result_";
  protected final String TEXT_314 = ".";
  protected final String TEXT_315 = "_";
  protected final String TEXT_316 = ".add(";
  protected final String TEXT_317 = ".";
  protected final String TEXT_318 = ".doubleValue());" + NL + "\t\t\t\t\t";
  protected final String TEXT_319 = "operation_result_";
  protected final String TEXT_320 = ".";
  protected final String TEXT_321 = "_";
  protected final String TEXT_322 = ".add((double)";
  protected final String TEXT_323 = ".";
  protected final String TEXT_324 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_325 = NL + "\t\t\t\t} // G_OutMain_AggR_546" + NL + "\t\t\t\t";
  protected final String TEXT_326 = NL;
  protected final String TEXT_327 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String origin = ElementParameterParser.getValue(node, "__DESTINATION__");
String cid = origin;

boolean useFinancialPrecision = "true".equals(ElementParameterParser.getValue(node, "__USE_FINANCIAL_PRECISION__"));

boolean checkTypeOverflow = "true".equals(ElementParameterParser.getValue(node, "__CHECK_TYPE_OVERFLOW__"));
boolean checkUlp = "true".equals(ElementParameterParser.getValue(node, "__CHECK_ULP__"));
String listDelimiter = ElementParameterParser.getValue(node, "__LIST_DELIMITER__");

IConnection inputConn = null;
IMetadataTable inputMetadataTable = null;
IMetadataTable outputMetadataTable = null;
java.util.List<IMetadataColumn> inputColumns = null;
java.util.List<IMetadataColumn> outputColumns = null;


int FUNCTION = 0;
int INPUT_COLUMN = 1;
int IGNORE_NULL = 2;
int OUTPUT_COLUMN = 3;
int WEIGHT_COLUMN = 4;

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


List<? extends IConnection> incomingConnections = node.getIncomingConnections();
if (incomingConnections != null && !incomingConnections.isEmpty()) {
	for (IConnection conn : incomingConnections) {
		if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			inputConn = conn;
			inputMetadataTable = conn.getMetadataTable();
			inputColumns = inputMetadataTable.getListColumns();
			break;
		}
	}
}

List<IMetadataTable> mestadataTableListOut = node.getMetadataList();
if (mestadataTableListOut!=null && mestadataTableListOut.size()>0) { // T_AggR_600
    outputMetadataTable = mestadataTableListOut.get(0);
	if(outputMetadataTable != null) {
		outputColumns = outputMetadataTable.getListColumns();
	}
}

if(inputConn != null) { // T_OutMain_AggR_501

	List<Map<String, String>> operations = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__OPERATIONS__");
	List<Map<String, String>> groupbys = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__GROUPBYS__");
	
	java.util.Map<String,IMetadataColumn> inputKeysColumns = new java.util.HashMap<String,IMetadataColumn>();
	java.util.Map<String,IMetadataColumn> inputValuesColumns = new java.util.HashMap<String,IMetadataColumn>();
	java.util.Map<String,IMetadataColumn> outputValuesColumns = new java.util.HashMap<String,IMetadataColumn>();
	java.util.Map<String,IMetadataColumn> weightValuesColumns = new java.util.HashMap<String,IMetadataColumn>();
	
	int sizeOperations = operations.size();
	int sizeGroupbys = groupbys.size();
	String lastInputColumn = null;
	if(sizeGroupbys>0){
		lastInputColumn = groupbys.get(sizeGroupbys-1).get("INPUT_COLUMN");
	}


	if(inputColumns != null) { // T_AggR_144
		for (IMetadataColumn column: inputColumns) { // T_AggR_145
	
			for(int i = 0; i < sizeGroupbys; i++){ // T_AggR_113
				String columnname = groupbys.get(i).get("INPUT_COLUMN");
				if(column.getLabel().equals(columnname)){ // T_AggR_114
					inputKeysColumns.put(columnname, column);
					break;
	        	} // T_AggR_114
			} // T_AggR_113
					
			// MOD scorreia 
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
	
	if(outputColumns != null) { // T_AggR_744
		for (IMetadataColumn column: outputColumns) { // T_AggR_745
	
			for(int i = 0; i < sizeOperations; i++){ // T_AggR_713
				String columnname = operations.get(i).get("OUTPUT_COLUMN");
	        	if(column.getLabel().equals(columnname)){ // T_AggR_714
	       			outputValuesColumns.put(columnname, column);
					break;
	       		} // T_AggR_714
			} // T_AggR_713
	
		} // T_AggR_745
	} // T_AggR_744
	
	
	for (IMetadataColumn column : inputColumns) {
		if(inputKeysColumns.containsKey(column.getLabel())) {
	
			
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_4);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_6);
    
		}
	}
	
	
	List<String[]> funinOperations = new java.util.ArrayList<String[]>();
	boolean hasOperationFirst = false;
	next:
	for(int i=0; i<sizeOperations; i++){
		Map<String, String> operation = operations.get(i);
		String fun = operation.get("FUNCTION");
		if(FIRST.equals(fun) || LAST.equals(fun) || MIN.equals(fun) || MAX.equals(fun)) {
			hasOperationFirst = true;
		}
		
		String in = operation.get("INPUT_COLUMN");
		String out = operation.get("OUTPUT_COLUMN");
		String ignoreNull = operation.get("IGNORE_NULL");
		String weight = operation.get("WEIGHT_COLUMN");
		
		
		String[] funin = new String[5];
		funin[FUNCTION]=fun;
		funin[INPUT_COLUMN]=in;
		funin[OUTPUT_COLUMN]=out;
		funin[IGNORE_NULL]=ignoreNull;
		funin[WEIGHT_COLUMN]=weight;
		funinOperations.add(funin);
	}
	
	
	
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    
	if(hasOperationFirst) {
	
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
    
	}
	
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    
		for (IMetadataColumn column : inputColumns) {
			if(inputKeysColumns.containsKey(column.getLabel())) {
		
				
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_22);
    
			}
		}
		
    stringBuffer.append(TEXT_23);
    
		if(hasOperationFirst) {
		
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    
		}
		
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_29);
    
		
	
	
	
	int sizeOps = funinOperations.size();
	String tInputColumn =null;

	boolean hasAlreadyCountProperty = false;
	boolean hasAlreadyCountDistinctProperty = false;
	for(int j = 0; j < sizeOps; j++){ // T_OutMain_AggR_546
		String[] funin = funinOperations.get(j);
		
		String function = funin[FUNCTION];
		String inputColumnName = funin[INPUT_COLUMN];
		String outputColumnName = funin[OUTPUT_COLUMN];
		/* MOD scorreia */
		String weightColumnName = funin[WEIGHT_COLUMN];
		
		boolean ignoreNull = ("true").equals(funin[IGNORE_NULL]);

		IMetadataColumn outputColumn = outputValuesColumns.get(outputColumnName);
		IMetadataColumn inputColumn = inputValuesColumns.get(inputColumnName);
		IMetadataColumn weightColumn = weightValuesColumns.get(weightColumnName); // MOD scorreia
		
		JavaType outputJavaType = JavaTypesManager.getJavaTypeFromId(outputColumn.getTalendType());
		JavaType inputJavaType = JavaTypesManager.getJavaTypeFromId(inputColumn.getTalendType());
		JavaType weightJavaType = JavaTypesManager.getJavaTypeFromId(weightColumn.getTalendType());

		boolean isBasePrimitive = JavaTypesManager.isJavaPrimitiveType(outputJavaType, false);
		boolean isSelectedPrimitive = JavaTypesManager.isJavaPrimitiveType(outputJavaType, outputColumn.isNullable());
		boolean isInputColumnPrimitive = JavaTypesManager.isJavaPrimitiveType(inputJavaType, inputColumn.isNullable());
		String primitiveTypeToGenerate = JavaTypesManager.getTypeToGenerate(outputJavaType.getId(), false);

		boolean outputIsNumber = JavaTypesManager.isNumberType(outputJavaType, false);
		boolean outputIsObject = outputJavaType == JavaTypesManager.OBJECT;
		boolean outputIsList = outputJavaType == JavaTypesManager.LIST;
		boolean outputIsString = outputJavaType == JavaTypesManager.STRING;
		boolean outputIsBigDecimal = outputJavaType == JavaTypesManager.BIGDECIMAL;
		boolean outputIsDate = outputJavaType == JavaTypesManager.DATE;
		boolean outputIsDecimal = outputJavaType == JavaTypesManager.FLOAT || outputJavaType == JavaTypesManager.DOUBLE || outputIsBigDecimal;
		
		boolean outputIsByte = outputJavaType == JavaTypesManager.BYTE;
		boolean outputIsShort = outputJavaType == JavaTypesManager.SHORT;
		
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
			|| function.equals(MAX_LENGTH) && outputIsString			
			|| function.equals(MAX_WEIGHT) && sameInOutType && weightIsNumberOrDate
		;
		
		if(isValidTypeForOperation) { // T_OutMain_AggR_745
		
			if(ignoreNull && !isInputColumnPrimitive) { // T_OutMain_AggR_545
			
				
    stringBuffer.append(TEXT_30);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_31);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_32);
    
				
			} // T_OutMain_AggR_545
			

			if(!hasAlreadyCountDistinctProperty && function.equals(COUNT_DISTINCT)){
					hasAlreadyCountDistinctProperty = true;
				
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_36);
    
				for (IMetadataColumn column : inputColumns) {
					if(inputValuesColumns.containsKey(column.getLabel())) {
				
						
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_39);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_41);
    
						
					}
				}
				
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_44);
    
			}
			
			if(function.equals(COUNT)) {

    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_46);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_47);
    
			}

			if(!hasAlreadyCountProperty && function.equals(COUNT)) {
				hasAlreadyCountProperty = true;
				
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_49);
    
			}
			
			if(outputIsNumber && function.equals(AVG)){
					
				
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_52);
    
				
			}
			if(function.equals(MIN) || function.equals(MAX)){
			
				String operator = ">";
				if(function.equals(MIN)) {
					operator = "<";
				}
	
	
	
				if(inputIsString || inputIsDate || inputIsObject || inputIsBigDecimal) {
				
					
    stringBuffer.append(TEXT_53);
    if(inputIsObject) {
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_56);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_58);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_60);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_61);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_63);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_64);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(operator);
    stringBuffer.append(TEXT_66);
    } else {
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_68);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_69);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_71);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_73);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_74);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_75);
    stringBuffer.append(operator);
    stringBuffer.append(TEXT_76);
    }
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_78);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_79);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_80);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_81);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_82);
    
				
				} else {
				
					
    stringBuffer.append(TEXT_83);
     if(outputColumn.isNullable()) { 
							
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_85);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_86);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_87);
    
						}
    stringBuffer.append(TEXT_88);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_89);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_90);
    stringBuffer.append(operator);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_92);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_93);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_94);
     if(!outputColumn.isNullable()) { 
							
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_96);
    
						}
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_98);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_99);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_100);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_101);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_102);
    
				}
			
			} else if(function.equals(SUM) || function.equals(AVG)){
	
				if(!isSelectedPrimitive && isBasePrimitive && !forceUseBigDecimal) {
					
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_104);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_105);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_107);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_108);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_109);
    stringBuffer.append( primitiveTypeToGenerate );
    stringBuffer.append(TEXT_110);
    
				}
	
				if(outputIsBigDecimal || forceUseBigDecimal) {
	
					
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_112);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_113);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_115);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_116);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_118);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_119);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_121);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_122);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_123);
    if(!inputIsBigDecimal || forceUseBigDecimal) {
							
    stringBuffer.append(TEXT_124);
    
						}
    stringBuffer.append(TEXT_125);
    if(forceUseBigDecimal) {
								
    stringBuffer.append(TEXT_126);
    
							}
    stringBuffer.append(TEXT_127);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_128);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_129);
    if(forceUseBigDecimal) {
								
    stringBuffer.append(TEXT_130);
    
							}
    stringBuffer.append(TEXT_131);
    if(!inputIsBigDecimal || forceUseBigDecimal) {
							
    stringBuffer.append(TEXT_132);
    
						}
    stringBuffer.append(TEXT_133);
    
			
				} else if(inputIsBigDecimal && !outputIsBigDecimal) {
				
					if(checkTypeOverflow || checkUlp) {
					
						
    stringBuffer.append(TEXT_134);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_135);
    stringBuffer.append( primitiveTypeToGenerate);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_137);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_138);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_139);
    stringBuffer.append( primitiveTypeToGenerate);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_141);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_142);
    stringBuffer.append( checkTypeOverflow );
    stringBuffer.append(TEXT_143);
    stringBuffer.append( checkUlp );
    stringBuffer.append(TEXT_144);
    
					}
					
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_146);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_147);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_148);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_149);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_150);
    
				
				} else {
				
					if(checkTypeOverflow || checkUlp) {
					
						
    stringBuffer.append(TEXT_151);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_152);
    stringBuffer.append( primitiveTypeToGenerate);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_154);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_155);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_156);
    stringBuffer.append( primitiveTypeToGenerate);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_158);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_159);
    stringBuffer.append( checkTypeOverflow );
    stringBuffer.append(TEXT_160);
    stringBuffer.append( checkUlp );
    stringBuffer.append(TEXT_161);
    
					}
					if(outputColumn.isNullable() && (outputIsByte || outputIsShort)){
					
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_163);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_164);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_165);
    stringBuffer.append(primitiveTypeToGenerate);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_167);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_168);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_169);
    stringBuffer.append(primitiveTypeToGenerate);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_171);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_172);
    stringBuffer.append(primitiveTypeToGenerate);
    stringBuffer.append(TEXT_173);
    
					}else{
					
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_175);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_176);
    stringBuffer.append( SUM );
    stringBuffer.append(TEXT_177);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_178);
    stringBuffer.append(inputColumnName );
    stringBuffer.append(TEXT_179);
    
					}
				}
			
			} else if(function.equals(FIRST)){
					
				
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_181);
    if(ignoreNull) {
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_183);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_184);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_185);
    }
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_187);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_188);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_189);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_190);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_191);
    
				
			} else if(function.equals(LAST)){
					
				
    stringBuffer.append(TEXT_192);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_193);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_194);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_195);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_196);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_197);
    
				
			} else if(function.equals(MAX_LENGTH)){
					
				
    stringBuffer.append(TEXT_198);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_199);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_201);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_202);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_204);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_205);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_206);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_207);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_208);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_209);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_211);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_212);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_214);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_215);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_216);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_217);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_218);
    
				
			} else if(function.equals(MAX_WEIGHT)){
					if (weightIsPrimitiveType) { // T_mergeout_main 001
						
    stringBuffer.append(TEXT_219);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_220);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_221);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_222);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_223);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_224);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_225);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_226);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_227);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_228);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_229);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_230);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_231);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_232);
    
					
					} // T_mergeout_main 001
					else { // T_mergeout_main 002
						
    stringBuffer.append(TEXT_233);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_234);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_235);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_236);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_237);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_238);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_239);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_240);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_241);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_242);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_243);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_244);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_245);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_246);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_247);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_248);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_249);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_250);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_251);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_252);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_253);
    stringBuffer.append(weightColumnName  );
    stringBuffer.append(TEXT_254);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_255);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_256);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_257);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_258);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_259);
    
					} // T_mergeout_main 002
			} else if(function.equals(LIST)){
					
				
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_261);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_262);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_263);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_264);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_265);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_266);
    stringBuffer.append( listDelimiter );
    stringBuffer.append(TEXT_267);
    
				if(inputIsByteArray) {
				
    stringBuffer.append(TEXT_268);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_269);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_270);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_271);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_272);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_273);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_274);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_275);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_276);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_277);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_278);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_279);
    
				} else {
				
    stringBuffer.append(TEXT_280);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_281);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_282);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_283);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_284);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_285);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_286);
    stringBuffer.append( listDelimiter );
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_288);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_289);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_290);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_291);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_292);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_293);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_294);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_295);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_296);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_297);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_298);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_299);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_300);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_301);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_302);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_303);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_304);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_305);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_306);
    
				}
				
			} else if(function.equals(LIST_OBJECT)){
					
				
    stringBuffer.append(TEXT_307);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_308);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_309);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_310);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_311);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_312);
    
				
			} else if(function.equals(STD_DEV)){

				if(inputIsBigDecimal) {
	
					
    stringBuffer.append(TEXT_313);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_314);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_315);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_316);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_317);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_318);
    
			
				} else {
				
					
    stringBuffer.append(TEXT_319);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_320);
    stringBuffer.append(outputColumnName );
    stringBuffer.append(TEXT_321);
    stringBuffer.append(function );
    stringBuffer.append(TEXT_322);
    stringBuffer.append(inputConn.getName() );
    stringBuffer.append(TEXT_323);
    stringBuffer.append(inputColumnName  );
    stringBuffer.append(TEXT_324);
    
					
				}
				
			}
			
			if(ignoreNull && !isInputColumnPrimitive) { // T_OutMain_AggR_545
			
				
    stringBuffer.append(TEXT_325);
    
				
			} // T_OutMain_AggR_545

		} // T_OutMain_AggR_745
		
	} // T_OutMain_AggR_546

} // T_OutMain_AggR_501


    stringBuffer.append(TEXT_326);
    stringBuffer.append(TEXT_327);
    return stringBuffer.toString();
  }
}
