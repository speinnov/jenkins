package org.talend.designer.codegen.translators.data_quality.survivorship;

import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IBigDataNode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TRuleSurvivorshipSparkcodeJava
{
  protected static String nl;
  public static synchronized TRuleSurvivorshipSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TRuleSurvivorshipSparkcodeJava result = new TRuleSurvivorshipSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "            public static class ";
  protected final String TEXT_2 = " implements ";
  protected final String TEXT_3 = " {";
  protected final String TEXT_4 = NL + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_5 = "(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "\t            public ";
  protected final String TEXT_6 = " ";
  protected final String TEXT_7 = "(";
  protected final String TEXT_8 = ") ";
  protected final String TEXT_9 = " {" + NL + "\t            \t";
  protected final String TEXT_10 = NL + "\t            \t";
  protected final String TEXT_11 = NL + "\t                ";
  protected final String TEXT_12 = NL + "\t                return ";
  protected final String TEXT_13 = ";" + NL + "\t            }" + NL + "\t        }" + NL + "\t\t";
  protected final String TEXT_14 = NL + "            public static class ";
  protected final String TEXT_15 = " implements ";
  protected final String TEXT_16 = " {";
  protected final String TEXT_17 = NL + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_18 = "(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_19 = " ";
  protected final String TEXT_20 = "(";
  protected final String TEXT_21 = ") ";
  protected final String TEXT_22 = " {" + NL + "                \t";
  protected final String TEXT_23 = NL + "\t                 \treturn ";
  protected final String TEXT_24 = ";";
  protected final String TEXT_25 = NL + "                }" + NL + "            }";
  protected final String TEXT_26 = NL + "            public static class ";
  protected final String TEXT_27 = " implements ";
  protected final String TEXT_28 = " {";
  protected final String TEXT_29 = NL + NL + "                private ContextProperties context = null;" + NL + "                private java.util.function.Function<org.apache.avro.generic.IndexedRecord, org.apache.avro.generic.IndexedRecord> function = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_30 = "(JobConf job, java.util.function.Function<org.apache.avro.generic.IndexedRecord, org.apache.avro.generic.IndexedRecord> function) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                    this.function = function;" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_31 = " ";
  protected final String TEXT_32 = "(";
  protected final String TEXT_33 = ") ";
  protected final String TEXT_34 = " {";
  protected final String TEXT_35 = NL + "                    ";
  protected final String TEXT_36 = NL + "                    ";
  protected final String TEXT_37 = NL + "                    return ";
  protected final String TEXT_38 = ";" + NL + "                }" + NL + "            }";
  protected final String TEXT_39 = NL;
  protected final String TEXT_40 = NL + "            // No sparkcode generated for unnecessary ";
  protected final String TEXT_41 = NL;
  protected final String TEXT_42 = NL + "            public static class ";
  protected final String TEXT_43 = "TrueFilter implements org.apache.spark.api.java.function.Function<scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase>, Boolean> {" + NL + "" + NL + "                public Boolean call(scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> arg0)" + NL + "                        throws Exception {" + NL + "                    return arg0._1;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_44 = "FalseFilter implements org.apache.spark.api.java.function.Function<scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase>, Boolean> {" + NL + "" + NL + "                public Boolean call(scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> arg0)" + NL + "                        throws Exception {" + NL + "                    return !arg0._1;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_45 = "ToNullWritableMain implements ";
  protected final String TEXT_46 = " {" + NL + "" + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_47 = "ToNullWritableMain(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_48 = " call(" + NL + "                        scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> data){";
  protected final String TEXT_49 = NL + "                    ";
  protected final String TEXT_50 = " ";
  protected final String TEXT_51 = " = (";
  protected final String TEXT_52 = ")data._2;" + NL + "                    return ";
  protected final String TEXT_53 = ";" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_54 = "ToNullWritableReject implements ";
  protected final String TEXT_55 = " {" + NL + "" + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_56 = "ToNullWritableReject(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_57 = " call(" + NL + "                        scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> data){";
  protected final String TEXT_58 = NL + "                        ";
  protected final String TEXT_59 = " ";
  protected final String TEXT_60 = " = (";
  protected final String TEXT_61 = ")data._2;" + NL + "                    return ";
  protected final String TEXT_62 = ";" + NL + "                }" + NL + "            }";
  protected final String TEXT_63 = NL + "            // No sparkconfig generated for unnecessary ";
  protected final String TEXT_64 = NL;
  protected final String TEXT_65 = NL + "            // Extract data." + NL;
  protected final String TEXT_66 = NL + "            ";
  protected final String TEXT_67 = "<Boolean, org.apache.avro.specific.SpecificRecordBase> temporary_rdd_";
  protected final String TEXT_68 = " = rdd_";
  protected final String TEXT_69 = ".";
  protected final String TEXT_70 = "(new ";
  protected final String TEXT_71 = "(job));" + NL + "" + NL + "            // Main flow" + NL;
  protected final String TEXT_72 = NL + "            ";
  protected final String TEXT_73 = " rdd_";
  protected final String TEXT_74 = " = temporary_rdd_";
  protected final String TEXT_75 = NL + "                  .filter(new ";
  protected final String TEXT_76 = "TrueFilter())" + NL + "                    .";
  protected final String TEXT_77 = "(new ";
  protected final String TEXT_78 = "ToNullWritableMain(job));" + NL + "" + NL + "            // Reject flow";
  protected final String TEXT_79 = NL + "            ";
  protected final String TEXT_80 = " rdd_";
  protected final String TEXT_81 = " = temporary_rdd_";
  protected final String TEXT_82 = NL + "                    .filter(new ";
  protected final String TEXT_83 = "FalseFilter())" + NL + "                    .";
  protected final String TEXT_84 = "(new ";
  protected final String TEXT_85 = "ToNullWritableReject(job));";
  protected final String TEXT_86 = NL + "            ";
  protected final String TEXT_87 = " rdd_";
  protected final String TEXT_88 = " = rdd_";
  protected final String TEXT_89 = ".";
  protected final String TEXT_90 = "(new ";
  protected final String TEXT_91 = "(job));";
  protected final String TEXT_92 = NL;
  protected final String TEXT_93 = NL + "        org.talend.survivorship.SurvivorshipManager manager_";
  protected final String TEXT_94 = "=null;" + NL + "        Object outPutGroupId_";
  protected final String TEXT_95 = " = null;" + NL + "        int groupCount_";
  protected final String TEXT_96 = " = 0;" + NL + "        Object[][] groupValues_";
  protected final String TEXT_97 = " = new Object[1][1];" + NL + "        Object tmpValue_";
  protected final String TEXT_98 = " = null;" + NL + "        java.util.Map<String, Object> survivors_";
  protected final String TEXT_99 = " = null;" + NL + "        java.util.List<java.util.HashSet<String>> conflicts_";
  protected final String TEXT_100 = " = null;" + NL + "" + NL + "        // current row number" + NL + "        private long currentRow=0l;" + NL + "        Object lastGroupID_";
  protected final String TEXT_101 = " = null;" + NL + "        //a row with all column values" + NL + "        private Object[] columnValues_";
  protected final String TEXT_102 = " = new Object[1];" + NL + "        //Store a group records" + NL + "        java.util.List<Object[]> recordLists__";
  protected final String TEXT_103 = " =new java.util.ArrayList<Object[]>();" + NL + "        // a count of all rows from RDD" + NL + "        private long rowCount;" + NL + "        public void setRowCount(long count){" + NL + "            this.rowCount=count;" + NL + "        }" + NL + "        ";
  protected final String TEXT_104 = NL + NL + "        // init SurvivorshipManager." + NL + "        if(manager_";
  protected final String TEXT_105 = "==null){" + NL + "            //transmit the rule path only for useLocalMode,or else the String \"Real_spark_relative_path\" represent a real cluster." + NL + "            manager_";
  protected final String TEXT_106 = " = new org.talend.survivorship.SurvivorshipManager(\"true\".equals(\"";
  protected final String TEXT_107 = "\")?\"";
  protected final String TEXT_108 = "\":\"Real_spark_relative_path\", ";
  protected final String TEXT_109 = ");" + NL + "            manager_";
  protected final String TEXT_110 = ".setJobName( jobName);" + NL + "            manager_";
  protected final String TEXT_111 = ".setJobVersion(jobVersion);" + NL + "        ";
  protected final String TEXT_112 = NL + "            manager_";
  protected final String TEXT_113 = ".addColumn(\"";
  protected final String TEXT_114 = "\",\"";
  protected final String TEXT_115 = "\");" + NL + "            ";
  protected final String TEXT_116 = NL + "            manager_";
  protected final String TEXT_117 = ".addRuleDefinition(" + NL + "                new org.talend.survivorship.model.RuleDefinition(" + NL + "                    org.talend.survivorship.model.RuleDefinition.Order.";
  protected final String TEXT_118 = NL + "                    ,";
  protected final String TEXT_119 = NL + "                    ,\"";
  protected final String TEXT_120 = "\"                                " + NL + "                    ,";
  protected final String TEXT_121 = "null";
  protected final String TEXT_122 = "org.talend.survivorship.model.RuleDefinition.Function.";
  protected final String TEXT_123 = NL + "                    ,";
  protected final String TEXT_124 = NL + "                    ,\"";
  protected final String TEXT_125 = "\"" + NL + "                    ,";
  protected final String TEXT_126 = NL + "                )" + NL + "            );";
  protected final String TEXT_127 = NL + "            org.talend.survivorship.model.Column column=null;" + NL + "            org.talend.survivorship.model.ConflictRuleDefinition conflictRule=null;";
  protected final String TEXT_128 = NL + "                column=manager_";
  protected final String TEXT_129 = ".getColumnByName(\"";
  protected final String TEXT_130 = "\");" + NL + "                conflictRule=new org.talend.survivorship.model.ConflictRuleDefinition(" + NL + "                        null" + NL + "                        ,";
  protected final String TEXT_131 = NL + "                        ,\"";
  protected final String TEXT_132 = "\"                                " + NL + "                        ,";
  protected final String TEXT_133 = "null";
  protected final String TEXT_134 = "org.talend.survivorship.model.ConflictRuleDefinition.Function.";
  protected final String TEXT_135 = NL + "                        ,";
  protected final String TEXT_136 = NL + "                        ,\"";
  protected final String TEXT_137 = "\"" + NL + "                        ,";
  protected final String TEXT_138 = NL + "                        ,";
  protected final String TEXT_139 = NL + "                        ,";
  protected final String TEXT_140 = NL + "                    );" + NL + "                column.getConflictResolveList().add(conflictRule);";
  protected final String TEXT_141 = NL + "         manager_";
  protected final String TEXT_142 = ".initKnowledgeBase();" + NL + "         //check if the conflict rules are valid." + NL + "         java.util.Map<String,java.util.List<String>> errorMap=manager_";
  protected final String TEXT_143 = ".checkConflictRuleValid();" + NL + "         if(errorMap.size()>0){" + NL + "             java.util.Iterator<?> iter = errorMap.entrySet().iterator();" + NL + "             StringBuilder erroMessages=new StringBuilder();" + NL + "             while (iter.hasNext()){" + NL + "                java.util.Map.Entry<String,java.util.List<String>> entry = (java.util.Map.Entry) iter.next();" + NL + "                erroMessages.append(\"'\"+entry.getKey()+\"':\\n\");" + NL + "                for(String value:entry.getValue()){" + NL + "                    for(int index=0;index<(\"'\"+entry.getKey()+\"':\").length();index++){" + NL + "                        erroMessages.append(\" \");" + NL + "                    }" + NL + "                    erroMessages.append(value+\"\\n\");" + NL + "                }" + NL + "             }" + NL + "             throw new Exception(erroMessages.toString());" + NL + "         }";
  protected final String TEXT_144 = NL + "            manager_";
  protected final String TEXT_145 = ".initKnowledgeBase();";
  protected final String TEXT_146 = NL + "        }";
  protected final String TEXT_147 = NL + "          " + NL + "          if(currentRow == 0l&&lastGroupID_";
  protected final String TEXT_148 = "==null){" + NL + "              lastGroupID_";
  protected final String TEXT_149 = "=";
  protected final String TEXT_150 = ".";
  protected final String TEXT_151 = ";" + NL + "          }" + NL + "          currentRow++;" + NL + "          " + NL + "        " + NL + "          int recordsCount_";
  protected final String TEXT_152 = "=0;" + NL + "          boolean isNeedOutput=false;" + NL + "          //when it is the first Row in a group, need to set outPutGroupId." + NL + "          if(groupCount_";
  protected final String TEXT_153 = " == 0){" + NL + "              outPutGroupId_";
  protected final String TEXT_154 = " = ";
  protected final String TEXT_155 = ".";
  protected final String TEXT_156 = ";" + NL + "          }" + NL + "          //if current groupId is not equals last groupId,output last group and reset some varibles." + NL + "          if(!lastGroupID_";
  protected final String TEXT_157 = ".equals(";
  protected final String TEXT_158 = ".";
  protected final String TEXT_159 = ")&&recordLists__";
  protected final String TEXT_160 = ".size()>0){" + NL + "              isNeedOutput=true;" + NL + "              groupValues_";
  protected final String TEXT_161 = " = new Object[groupCount_";
  protected final String TEXT_162 = "][";
  protected final String TEXT_163 = "];" + NL + "              for(int i=0;i<groupCount_";
  protected final String TEXT_164 = ";i++){" + NL + "                  groupValues_";
  protected final String TEXT_165 = "[i]=recordLists__";
  protected final String TEXT_166 = ".get(i);" + NL + "              }" + NL + "              outPutGroupId_";
  protected final String TEXT_167 = " =lastGroupID_";
  protected final String TEXT_168 = ";" + NL + "              groupCount_";
  protected final String TEXT_169 = "=0;" + NL + "              recordLists__";
  protected final String TEXT_170 = ".clear();" + NL + "             " + NL + "          }" + NL + "          groupCount_";
  protected final String TEXT_171 = "++;" + NL + "          columnValues_";
  protected final String TEXT_172 = "=new Object[";
  protected final String TEXT_173 = "];";
  protected final String TEXT_174 = NL + "          columnValues_";
  protected final String TEXT_175 = "[";
  protected final String TEXT_176 = "] = ";
  protected final String TEXT_177 = ".";
  protected final String TEXT_178 = " ;" + NL + "          ";
  protected final String TEXT_179 = NL + "          recordLists__";
  protected final String TEXT_180 = ".add( columnValues_";
  protected final String TEXT_181 = ");" + NL + "          //do something when it is the last record form RDD." + NL + "          if(currentRow==this.rowCount){" + NL + "              isNeedOutput=true;" + NL + "              groupValues_";
  protected final String TEXT_182 = " = new Object[groupCount_";
  protected final String TEXT_183 = "][";
  protected final String TEXT_184 = "];" + NL + "              for(int i=0;i<groupCount_";
  protected final String TEXT_185 = ";i++){" + NL + "                  groupValues_";
  protected final String TEXT_186 = "[i]=recordLists__";
  protected final String TEXT_187 = ".get(i);" + NL + "              }" + NL + "              outPutGroupId_";
  protected final String TEXT_188 = " =";
  protected final String TEXT_189 = ".";
  protected final String TEXT_190 = ";" + NL + "          }" + NL + "          //call API when it needs to output a group." + NL + "          if(isNeedOutput){" + NL + "              manager_";
  protected final String TEXT_191 = ".runSession(groupValues_";
  protected final String TEXT_192 = ");" + NL + "              recordsCount_";
  protected final String TEXT_193 = " = groupValues_";
  protected final String TEXT_194 = ".length + 1;" + NL + "              survivors_";
  protected final String TEXT_195 = " = manager_";
  protected final String TEXT_196 = ".getSurvivorMap();" + NL + "              conflicts_";
  protected final String TEXT_197 = " = manager_";
  protected final String TEXT_198 = ".getConflictList();" + NL + "          }" + NL + "" + NL + "          //output previous or the last one group";
  protected final String TEXT_199 = NL + "          for(int i_";
  protected final String TEXT_200 = " = 0; isNeedOutput && i_";
  protected final String TEXT_201 = " < recordsCount_";
  protected final String TEXT_202 = "; i_";
  protected final String TEXT_203 = "++){" + NL + "              //because a group data is output in this loop, we need to create a new outputStructure instance here.";
  protected final String TEXT_204 = NL + "              ";
  protected final String TEXT_205 = "=new ";
  protected final String TEXT_206 = "();";
  protected final String TEXT_207 = NL + "                  if(i_";
  protected final String TEXT_208 = " != recordsCount_";
  protected final String TEXT_209 = " - 1){" + NL + "                      tmpValue_";
  protected final String TEXT_210 = " = groupValues_";
  protected final String TEXT_211 = "[i_";
  protected final String TEXT_212 = "][";
  protected final String TEXT_213 = "];" + NL + "                  }else{" + NL + "                      tmpValue_";
  protected final String TEXT_214 = " = survivors_";
  protected final String TEXT_215 = ".get(\"";
  protected final String TEXT_216 = "\");" + NL + "                  }" + NL + "" + NL + "                  if(tmpValue_";
  protected final String TEXT_217 = " != null){";
  protected final String TEXT_218 = "              ";
  protected final String TEXT_219 = NL + "                          ";
  protected final String TEXT_220 = ".";
  protected final String TEXT_221 = " = tmpValue_";
  protected final String TEXT_222 = ".toString();";
  protected final String TEXT_223 = NL + "                          if(tmpValue_";
  protected final String TEXT_224 = " instanceof java.util.Date){";
  protected final String TEXT_225 = NL + "                              ";
  protected final String TEXT_226 = ".";
  protected final String TEXT_227 = " = (java.util.Date)tmpValue_";
  protected final String TEXT_228 = ";" + NL + "                          }else{";
  protected final String TEXT_229 = NL + "                              ";
  protected final String TEXT_230 = ".";
  protected final String TEXT_231 = " = ParserUtils.parseTo_Date(tmpValue_";
  protected final String TEXT_232 = ".toString(), ";
  protected final String TEXT_233 = ");" + NL + "                          }";
  protected final String TEXT_234 = NL + "                          ";
  protected final String TEXT_235 = ".";
  protected final String TEXT_236 = " = tmpValue_";
  protected final String TEXT_237 = ".toString().getBytes();";
  protected final String TEXT_238 = "                                  ";
  protected final String TEXT_239 = NL + "                          ";
  protected final String TEXT_240 = ".";
  protected final String TEXT_241 = " = ParserUtils.parseTo_";
  protected final String TEXT_242 = "(tmpValue_";
  protected final String TEXT_243 = ".toString()); ";
  protected final String TEXT_244 = NL + "                  }else{";
  protected final String TEXT_245 = NL + "                      ";
  protected final String TEXT_246 = ".";
  protected final String TEXT_247 = " = ";
  protected final String TEXT_248 = ";" + NL + "                  }";
  protected final String TEXT_249 = NL + "                      " + NL + "              if(i_";
  protected final String TEXT_250 = " != recordsCount_";
  protected final String TEXT_251 = " - 1){";
  protected final String TEXT_252 = NL + "                  ";
  protected final String TEXT_253 = ".CONFLICT = conflicts_";
  protected final String TEXT_254 = ".get(i_";
  protected final String TEXT_255 = ").toString();";
  protected final String TEXT_256 = NL + "                  ";
  protected final String TEXT_257 = ".SURVIVOR = false;" + NL + "              }else{ // master record";
  protected final String TEXT_258 = NL + "                  ";
  protected final String TEXT_259 = ".CONFLICT = manager_";
  protected final String TEXT_260 = ".getConflictsOfSurvivor().toString();";
  protected final String TEXT_261 = NL + "                  ";
  protected final String TEXT_262 = ".SURVIVOR = true;";
  protected final String TEXT_263 = NL + "                  ";
  protected final String TEXT_264 = ".";
  protected final String TEXT_265 = " = ParserUtils.parseTo_";
  protected final String TEXT_266 = "(outPutGroupId_";
  protected final String TEXT_267 = ".toString()); " + NL + "              }" + NL + "           // Emit the parsed structure on the main output.";
  protected final String TEXT_268 = NL + "              ";
  protected final String TEXT_269 = NL + "              ";
  protected final String TEXT_270 = NL + "  " + NL + "           }" + NL + "          //~ output previous or the last one group" + NL + "          //reset the currentGroupId to lastGroupId" + NL + "          lastGroupID_";
  protected final String TEXT_271 = "=";
  protected final String TEXT_272 = ".";
  protected final String TEXT_273 = ";";
  protected final String TEXT_274 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
//Parse the inputs to this javajet generator.
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode) codeGenArgument.getArgument();
final IBigDataNode bigDataNode = (IBigDataNode) codeGenArgument.getArgument();
final String cid = node.getUniqueName();



    
	/**
	 * Code generated for component with single output
 	 */
    class SOFunctionGenerator extends org.talend.designer.spark.generator.FunctionGenerator{

    	SOFunctionGenerator(org.talend.designer.common.TransformerBase transformer) {
            super(transformer);
        }

    	SOFunctionGenerator(org.talend.designer.common.TransformerBase transformer, org.talend.designer.spark.generator.SparkFunction sparkFunction) {
    		super(transformer, sparkFunction);
    	}

    	@Override
        public void generate(){
		
    stringBuffer.append(TEXT_1);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_2);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementation(getOutValueClass(), getInValueClass()));
    stringBuffer.append(TEXT_3);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_4);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_5);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedType(this.outValueClass.toString()));
    stringBuffer.append(TEXT_6);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_7);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_8);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_9);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    stringBuffer.append(TEXT_10);
    stringBuffer.append(this.sparkFunction.getCodeKeyMapping(getInValue()));
    stringBuffer.append(TEXT_11);
    
	                this.transformer.generateTransformContextInitialization();
	                this.transformer.generateTransform(true);
	                
    stringBuffer.append(TEXT_12);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn(this.getOutValue(), this.getOutValueClass()));
    stringBuffer.append(TEXT_13);
    
        }
    }

	/**
	 * Code generated for component with multiple outputs
 	 */
    class MOFunctionGenerator extends org.talend.designer.spark.generator.FunctionGenerator{

        /** The single connection to pass along the output chain of Mappers
         *  instead of sending to a dedicated collector via MultipleOutputs. */
        String connectionToChain = null;

        MOFunctionGenerator(org.talend.designer.common.TransformerBase transformer) {
            super(transformer);
            defaultOutKeyClass = "Boolean";
        }

    	MOFunctionGenerator(org.talend.designer.common.TransformerBase transformer, org.talend.designer.spark.generator.SparkFunction sparkFunction) {
    		super(transformer, sparkFunction);
            defaultOutKeyClass = "Boolean";
    	}

    	@Override
        public void generate(){
        
    stringBuffer.append(TEXT_14);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_15);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementationOutputFixedType(getInValueClass(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_16);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_17);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_18);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedTypeFixedType((String)this.outKeyClass, "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_19);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_20);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_21);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_22);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    
                    this.transformer.generateTransformContextInitialization();
                    this.transformer.generateTransform(true);
	                if(this.sparkFunction.getCodeFunctionReturn()!=null) {
                
    stringBuffer.append(TEXT_23);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn());
    stringBuffer.append(TEXT_24);
    
	            	}
                
    stringBuffer.append(TEXT_25);
    
        }
    }
    
    /**
     * Code generated for tDataprepRun component
     */
    class DataprepFunctionGenerator extends org.talend.designer.spark.generator.FunctionGenerator {

        DataprepFunctionGenerator(org.talend.designer.common.TransformerBase transformer) {
            super(transformer);
        }

        DataprepFunctionGenerator(org.talend.designer.common.TransformerBase transformer, org.talend.designer.spark.generator.SparkFunction sparkFunction) {
            super(transformer, sparkFunction);
        }

        @Override
        public void generate(){
        
    stringBuffer.append(TEXT_26);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_27);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementation(getOutValueClass(), getInValueClass()));
    stringBuffer.append(TEXT_28);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_29);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_30);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedType(this.outValueClass.toString()));
    stringBuffer.append(TEXT_31);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_32);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_33);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_34);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    stringBuffer.append(TEXT_36);
    stringBuffer.append(this.sparkFunction.getCodeKeyMapping(getInValue()));
    
                        this.transformer.generateTransformContextInitialization();
                        this.transformer.generateTransform(true);
                     
    stringBuffer.append(TEXT_37);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn(this.getOutValue(), this.getOutValueClass()));
    stringBuffer.append(TEXT_38);
    
        }
    }

    stringBuffer.append(TEXT_39);
    

/**
 * A common, reusable utility that generates code in the context of a spark
 * component, for reading and writing to a spark RDD.
 */
class SparkRowTransformUtil extends org.talend.designer.common.CommonRowTransformUtil {

    private boolean isMultiOutput = false;

    private org.talend.designer.spark.generator.SparkFunction sparkFunction = null;

    private org.talend.designer.spark.generator.FunctionGenerator functionGenerator = null;

    public SparkRowTransformUtil() {

    }
    
    public SparkRowTransformUtil(org.talend.designer.spark.generator.SparkFunction sparkFunction) {
        this.sparkFunction = sparkFunction;
    }
    
    public void setFunctionGenerator(org.talend.designer.spark.generator.FunctionGenerator functionGenerator) {
        this.functionGenerator = functionGenerator;
    }

    public void setMultiOutput(boolean multiOutput) {
        isMultiOutput = multiOutput;
    }

    public String getCodeToGetInField(String columnName) {
        return functionGenerator.getInValue() + "." + columnName;
    }

    public String getInValue() {
        return functionGenerator.getInValue();
    }

    public String getOutValue() {
        return functionGenerator.getOutValue();
    }

    public String getInValueClass() {
        return functionGenerator.getInValueClass();
    }

    public String getOutValueClass() {
        return functionGenerator.getOutValueClass();
    }

    public String getCodeToGetOutField(boolean isReject, String columnName) {
        return functionGenerator.getOutValue(isReject ? "reject" : "main") + "." + columnName;
    }

    public String getCodeToInitOut(boolean isReject, Iterable<IMetadataColumn> columns) {
        if(!isReject && this.sparkFunction!=null && !isMultiOutput) {
            return this.sparkFunction.getCodeToInitOut(functionGenerator.getOutValue("main"), functionGenerator.getOutValueClass("main"));
        } else {
            return "";
        }
    }

    // Method to avoid using getCodeToInitOut that calls sparkFunction.getCodeToInitOut which creates unnecessary objects
    // Check getCodeToAddToOutput in SparkFunction and its implementation in FlatMapToPairFunction
    public String getCodeToAddToOutput(boolean isReject, Iterable<IMetadataColumn> columns) {
        if(this.sparkFunction!=null && !isMultiOutput) {
            return this.sparkFunction.getCodeToAddToOutput(false, false, functionGenerator.getOutValue(isReject ? "reject" : "main"), functionGenerator.getOutValueClass(isReject ? "reject" : "main"));
        }else if(this.sparkFunction!=null && isMultiOutput){
            if(isReject){
                return this.sparkFunction.getCodeToAddToOutput(true, false, functionGenerator.getOutValue("reject"), functionGenerator.getOutValueClass("reject"));
            }else{
                return this.sparkFunction.getCodeToAddToOutput(true, true, functionGenerator.getOutValue("main"), functionGenerator.getOutValueClass("main"));
            }
        }else {
            return "";
        }
    }

    public String getCodeToSetOutField(boolean isReject, String columnName, String codeValue) {
        return functionGenerator.getOutValue(isReject ? "reject" : "main") + "." + columnName + " = " + codeValue + ";";
    }

    public String getCodeToSetOutField(boolean isReject, String columnName, String codeValue, String operator) {
        return functionGenerator.getOutValue(isReject ? "reject" : "main") + "." + columnName + " " + operator + " " + codeValue + ";";
    }

    public String getCodeToEmit(boolean isReject) {
        if (this.sparkFunction != null && isMultiOutput) {
            if (isReject) {
                return this.sparkFunction.getCodeToEmit(false, functionGenerator.getOutValue("reject"), functionGenerator.getOutValueClass("reject"));
            } else {
                return this.sparkFunction.getCodeToEmit(true, functionGenerator.getOutValue("main"), functionGenerator.getOutValueClass("main"));
            }
        } else {
            if (isReject) {
                return this.sparkFunction.getCodeToInitOut(functionGenerator.getOutValue("reject"), functionGenerator.getOutValueClass("reject"));
            } else {
                return "";
            }
        }
    }

    public void generateSparkCode(final org.talend.designer.common.TransformerBase transformer, final org.talend.designer.spark.generator.SparkFunction sparkFunction) {
        if (transformer.isMultiOutput()) {
            setMultiOutput(true);
        }
        if (transformer.isUnnecessary()) {
            
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    
            return;
        }

        if (transformer.isMultiOutput()) {
            org.talend.designer.spark.generator.SparkFunction localSparkFunction = null;
            if ((sparkFunction instanceof org.talend.designer.spark.generator.FlatMapFunction)
                    || (sparkFunction instanceof org.talend.designer.spark.generator.FlatMapToPairFunction)) {
                localSparkFunction = new org.talend.designer.spark.generator.FlatMapToPairFunction(
                        sparkFunction.isInputPair(),
                        codeGenArgument.getSparkVersion(),
                        sparkFunction.getKeyList());
            } else {
                localSparkFunction = new org.talend.designer.spark.generator.MapToPairFunction(
                        sparkFunction.isInputPair(),
                        sparkFunction.getKeyList());
            }

            org.talend.designer.spark.generator.SparkFunction extractSparkFunction = null;
            if ((sparkFunction instanceof org.talend.designer.spark.generator.FlatMapFunction)
                    || (sparkFunction instanceof org.talend.designer.spark.generator.MapFunction)) {
                extractSparkFunction = new org.talend.designer.spark.generator.MapFunction(
                        sparkFunction.isInputPair(),
                        sparkFunction.getKeyList());
            } else {
                extractSparkFunction = new org.talend.designer.spark.generator.MapToPairFunction(
                        sparkFunction.isInputPair(),
                        sparkFunction.getKeyList());
            }
            this.sparkFunction = localSparkFunction;

            // The multi-output condition is slightly different, and the
            // MapperHelper is configured with internal names for the
            // connections.
            java.util.HashMap<String, String> names = new java.util.HashMap<String, String>();
            names.put("main", transformer.getOutConnMainName());
            names.put("reject", transformer.getOutConnRejectName());

            // Refactoring FunctionGenerator to java so we have to instaniate a MO or SO here
            functionGenerator = new MOFunctionGenerator(transformer, localSparkFunction);
            functionGenerator.init(node, cid, null, transformer.getInConnName(), null, names);

            
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(extractSparkFunction.getCodeFunctionImplementationInputFixedType(transformer.getOutConnMainTypeName(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturnedType(transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_48);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(transformer.getOutConnMainTypeName());
    stringBuffer.append(TEXT_50);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_51);
    stringBuffer.append(transformer.getOutConnMainTypeName());
    stringBuffer.append(TEXT_52);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturn(transformer.getOutConnMainName(), transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(extractSparkFunction.getCodeFunctionImplementationInputFixedType(transformer.getOutConnRejectTypeName(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturnedType(transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_57);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(transformer.getOutConnRejectTypeName());
    stringBuffer.append(TEXT_59);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_60);
    stringBuffer.append(transformer.getOutConnRejectTypeName());
    stringBuffer.append(TEXT_61);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturn(transformer.getOutConnRejectName(), transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_62);
    
        } else {
            // Refactoring FunctionGenerator to java so we have to instaniate a MO or SO here
            functionGenerator = new SOFunctionGenerator(transformer, sparkFunction);

            functionGenerator.init(node, cid, null, transformer.getInConnName(), null,
                    transformer.getOutConnMainName() != null
                        ? transformer.getOutConnMainName()
                                : transformer.getOutConnRejectName());
        }
        functionGenerator.generate();
    }

    public void generateSparkConfig(final org.talend.designer.common.TransformerBase transformer, final org.talend.designer.spark.generator.SparkFunction sparkFunction) {
        if (transformer.isUnnecessary()) {
            
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    
            return;
        }

        if (transformer.isMultiOutput()) {
            String localFunctionType = "mapToPair";
            if ((sparkFunction instanceof org.talend.designer.spark.generator.FlatMapFunction)
                    || (sparkFunction instanceof org.talend.designer.spark.generator.FlatMapToPairFunction)) {
                localFunctionType = "flatMapToPair";
            }

            String extractFunctionType = "mapToPair";
            if ((sparkFunction instanceof org.talend.designer.spark.generator.FlatMapFunction)
                    || (sparkFunction instanceof org.talend.designer.spark.generator.MapFunction)) {
                extractFunctionType = "map";
            }
            
    stringBuffer.append(TEXT_65);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(sparkFunction.isStreaming() ?"org.apache.spark.streaming.api.java.JavaPairDStream":"org.apache.spark.api.java.JavaPairRDD");
    stringBuffer.append(TEXT_67);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_68);
    stringBuffer.append(transformer.getInConnName());
    stringBuffer.append(TEXT_69);
    stringBuffer.append(localFunctionType);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_71);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_73);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_74);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(extractFunctionType);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_80);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_81);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(extractFunctionType);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    
        } else {
            functionGenerator = new SOFunctionGenerator(transformer, sparkFunction);

            functionGenerator.init(node, cid, null, transformer.getInConnName(), null,
                    transformer.getOutConnMainName() != null
                        ? transformer.getOutConnMainName()
                                : transformer.getOutConnRejectName());
            
    stringBuffer.append(TEXT_86);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnMainName() != null ? transformer.getOutConnMainTypeName() : transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_87);
    stringBuffer.append(transformer.getOutConnMainName() != null ? transformer.getOutConnMainName() : transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_88);
    stringBuffer.append(transformer.getInConnName());
    stringBuffer.append(TEXT_89);
    stringBuffer.append(sparkFunction.getConfigTransformation());
    stringBuffer.append(TEXT_90);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_91);
    
        }
    }
}


    stringBuffer.append(TEXT_92);
    

/**
 * Contains common processing for TRuleSurvivorshipUtil code generation.  This is
 * used in MapReduce and Storm components.
 *
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class TRuleSurvivorshipUtil extends org.talend.designer.common.TransformerBase {

    String packageName = ElementParameterParser.getValue(node, "__PACKAGE_NAME__");
    String projectDir = ElementParameterParser.getValue(node.getProcess(), "__TDQ_DEFAULT_PROJECT_DIR__");    
    String grpIDColumnName = ElementParameterParser.getValue(node, "__GRP_ID__");
    String isDefinedConflict = ElementParameterParser.getValue(node, "__DEFINE_CONFLICT__");
    java.util.List<java.util.Map<String, String>> conlictRules = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValue(node, "__CONFLICT_TABLE__");
    java.util.List<? extends IConnection> outConns = node.getOutgoingSortedConnections();
    java.util.List<java.util.Map<String, String>> operations = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValue(node, "__OPERATIONS__");
  //Find the tSparkConfiguration and define which Spark version is currently used.
    final java.util.List<? extends INode> sparkConfigs = node.getProcess().getNodesOfType("tSparkConfiguration");
    boolean useLocalMode = false;
    
    final private String incomingConnName;
    final private String outgoingConnName;
    
    /** The list of columns that should be copied directly from the input to
     *  the output schema (where they have the same column names). */
    final private Iterable<IMetadataColumn> copiedInColumns;
    final private java.util.List<IMetadataColumn> inputColumns;

    /** Columns in the output schema that are not copied directly from the
     *  input schema (excluding reject fields). */
    final private java.util.List<IMetadataColumn> newOutColumns;
    private int nbMetadataColumns=0;

    public TRuleSurvivorshipUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil) {
        super(node, argument, rowTransformUtil, "FLOW", "REJECT");

        if (null != getInConn() && null != getOutConnMain()) {
            copiedInColumns = getColumnsUnion(getInColumns(), getOutColumnsMain());
            newOutColumns = getColumnsDiff(getOutColumnsMain(), getInColumns());
            inputColumns=getInConn().getMetadataTable().getListColumns() ;
        } else {
            copiedInColumns = null;
            newOutColumns = null;
            inputColumns=new java.util.ArrayList<IMetadataColumn>();
        }
        incomingConnName=getInConn().getName();
        outgoingConnName=getOutConnMainName();
        nbMetadataColumns =inputColumns.size();
        INode sparkConfig = null;
        if(sparkConfigs != null && sparkConfigs.size() > 0) {
         sparkConfig = sparkConfigs.get(0);
        }
        if(sparkConfig != null) {
            useLocalMode = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SPARK_LOCAL_MODE__"));
        }
       
    }

    public void generateTransformContextDeclaration() {
        
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    
    }

    public void generateTransformContextInitialization(){
        String jobName = node.getProcess().getName();
        String jobVersion = node.getProcess().getVersion();
        
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(useLocalMode);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    
        for(IMetadataColumn column : inputColumns){
            String typeName = "";
            typeName = column.getTalendType().substring(column.getTalendType().indexOf("_")+1);
            if(typeName.equals("Date")){
                typeName = "java.util.Date";
            }
        
    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_114);
    stringBuffer.append(typeName);
    stringBuffer.append(TEXT_115);
    
        }
        
        for(int i=0; i<operations.size(); i++){
            java.util.Map<String, String> operation = operations.get(i);
            String relation = operation.get("RELATION");
            String name = operation.get("RULE_NAME");
            String reference = operation.get("INPUT_COLUMN");
            String function = operation.get("FUNCTION");
            String operationValue = operation.get("OPERATION");
            String target = operation.get("OUTPUT_COLUMN");//?
            String ignoreNull = operation.get("IGNORE_NULL");
            
    stringBuffer.append(TEXT_116);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(relation);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(reference);
    stringBuffer.append(TEXT_120);
    if("None".equals(function)){
    stringBuffer.append(TEXT_121);
    }else{
    stringBuffer.append(TEXT_122);
    stringBuffer.append(function);
    }
    stringBuffer.append(TEXT_123);
    stringBuffer.append("\"\"".equals(operationValue)?null:operationValue);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(target);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(ignoreNull);
    stringBuffer.append(TEXT_126);
    
        }
       
      //check if the conflict rules are valid
        if("true".equals(isDefinedConflict)){
        
    stringBuffer.append(TEXT_127);
    
            for(int i=0; i<conlictRules.size(); i++){
                java.util.Map<String, String> conlictRule = conlictRules.get(i);
                String name = conlictRule.get("CR_RULE_NAME");
                String conflictingCol = conlictRule.get("CR_CONFLICTING_COLUMN");
                String function = conlictRule.get("CR_FUNCTION");
                String operationValue = conlictRule.get("CR_VALUE");
                String referenceCol = conlictRule.get("CR_REF_COLUMN");
                String ignoreNull = conlictRule.get("CR_IGNORE_NULL");
                String disable = conlictRule.get("CR_DISABLE");
                String fillColumn = null;
                String removeDuplicate ="false";
         
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(conflictingCol);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(referenceCol);
    stringBuffer.append(TEXT_132);
    if("None".equals(function)){
    stringBuffer.append(TEXT_133);
    }else{
    stringBuffer.append(TEXT_134);
    stringBuffer.append(function);
    }
    stringBuffer.append(TEXT_135);
    stringBuffer.append("\"\"".equals(operationValue)?null:operationValue);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(conflictingCol);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(ignoreNull);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(removeDuplicate);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(disable);
    stringBuffer.append(TEXT_140);
    
            }
         
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    
        }else{
            
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    
        }
        
    stringBuffer.append(TEXT_146);
    

    }

    public void generateTransform() {
        generateTransform(false);
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow.
     */
    public void generateTransform(boolean hasAReturnedType) {
          
    stringBuffer.append(TEXT_147);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_173);
    
          for( int i = 0; i < nbMetadataColumns; i++) {
              IMetadataColumn metadataColumn = inputColumns.get(i);
          
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(metadataColumn.getLabel());
    stringBuffer.append(TEXT_178);
    
          }
          
    stringBuffer.append(TEXT_179);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_198);
     String grpIDColumnType = ""; 
    stringBuffer.append(TEXT_199);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(getCodeGenArgument().getRecordStructName(outgoingConnName));
    stringBuffer.append(TEXT_206);
     
              for( int i = 0; i < nbMetadataColumns; i++) {
                  
                  IMetadataColumn column = inputColumns.get(i);
                              
                  String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                  String defaultValue =JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
                  JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                                  
                  String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
                  if(grpIDColumnName.equals(column.getLabel())){
                      grpIDColumnType = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());                            
                  }
                  
    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_216);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_217);
                              
                      if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
                      
    stringBuffer.append(TEXT_218);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_221);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_222);
    
                      } else if(javaType == JavaTypesManager.DATE) { // Date
                      
    stringBuffer.append(TEXT_223);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_227);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_231);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_232);
    stringBuffer.append(patternValue);
    stringBuffer.append(TEXT_233);
    
                      } else if(javaType == JavaTypesManager.BYTE_ARRAY) { //byte[]
                      
    stringBuffer.append(TEXT_234);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_236);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_237);
    
                      } else {
                      
    stringBuffer.append(TEXT_238);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_241);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_242);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_243);
    
                      } 
                      
    stringBuffer.append(TEXT_244);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_247);
    stringBuffer.append(defaultValue);
    stringBuffer.append(TEXT_248);
    
              }
              
    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_251);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_259);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_260);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_264);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_265);
    stringBuffer.append(grpIDColumnType);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_267);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(getRowTransform().getCodeToEmit(false));
    stringBuffer.append(TEXT_269);
    stringBuffer.append(getRowTransform().getCodeToInitOut(null == getOutConnMain(), newOutColumns));
    stringBuffer.append(TEXT_270);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_271);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_273);
    
    }
}

    
org.talend.designer.spark.generator.SparkFunction sparkFunction = null;
String requiredInputType = bigDataNode.getRequiredInputType();
String requiredOutputType = bigDataNode.getRequiredOutputType();
String incomingType = bigDataNode.getIncomingType();
String outgoingType = bigDataNode.getOutgoingType();
boolean inputIsPair = requiredInputType != null ? "KEYVALUE".equals(requiredInputType) : "KEYVALUE".equals(incomingType);

String type = requiredOutputType == null ? outgoingType : requiredOutputType;
if("KEYVALUE".equals(type)) {
 sparkFunction = new org.talend.designer.spark.generator.FlatMapToPairFunction(inputIsPair, codeGenArgument.getSparkVersion());
} else if("VALUE".equals(type)) {
 sparkFunction = new org.talend.designer.spark.generator.FlatMapFunction(inputIsPair, codeGenArgument.getSparkVersion());
}

final SparkRowTransformUtil sparkTransformUtil = new SparkRowTransformUtil(sparkFunction);
final TRuleSurvivorshipUtil tRuleSurvivorshipUtil = new TRuleSurvivorshipUtil(
     node, codeGenArgument, sparkTransformUtil);

sparkTransformUtil.generateSparkCode(tRuleSurvivorshipUtil, sparkFunction);

    stringBuffer.append(TEXT_274);
    return stringBuffer.toString();
  }
}
