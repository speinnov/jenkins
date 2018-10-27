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
import org.talend.designer.spark.generator.utils.SparkFunctionUtil;

public class TRuleSurvivorshipSparkconfigJava
{
  protected static String nl;
  public static synchronized TRuleSurvivorshipSparkconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TRuleSurvivorshipSparkconfigJava result = new TRuleSurvivorshipSparkconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    ";
  protected final String TEXT_2 = NL + "            public static class ";
  protected final String TEXT_3 = " implements ";
  protected final String TEXT_4 = " {";
  protected final String TEXT_5 = NL + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_6 = "(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "\t            public ";
  protected final String TEXT_7 = " ";
  protected final String TEXT_8 = "(";
  protected final String TEXT_9 = ") ";
  protected final String TEXT_10 = " {" + NL + "\t            \t";
  protected final String TEXT_11 = NL + "\t            \t";
  protected final String TEXT_12 = NL + "\t                ";
  protected final String TEXT_13 = NL + "\t                return ";
  protected final String TEXT_14 = ";" + NL + "\t            }" + NL + "\t        }" + NL + "\t\t";
  protected final String TEXT_15 = NL + "            public static class ";
  protected final String TEXT_16 = " implements ";
  protected final String TEXT_17 = " {";
  protected final String TEXT_18 = NL + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_19 = "(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_20 = " ";
  protected final String TEXT_21 = "(";
  protected final String TEXT_22 = ") ";
  protected final String TEXT_23 = " {" + NL + "                \t";
  protected final String TEXT_24 = NL + "\t                 \treturn ";
  protected final String TEXT_25 = ";";
  protected final String TEXT_26 = NL + "                }" + NL + "            }";
  protected final String TEXT_27 = NL + "            public static class ";
  protected final String TEXT_28 = " implements ";
  protected final String TEXT_29 = " {";
  protected final String TEXT_30 = NL + NL + "                private ContextProperties context = null;" + NL + "                private java.util.function.Function<org.apache.avro.generic.IndexedRecord, org.apache.avro.generic.IndexedRecord> function = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_31 = "(JobConf job, java.util.function.Function<org.apache.avro.generic.IndexedRecord, org.apache.avro.generic.IndexedRecord> function) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                    this.function = function;" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_32 = " ";
  protected final String TEXT_33 = "(";
  protected final String TEXT_34 = ") ";
  protected final String TEXT_35 = " {";
  protected final String TEXT_36 = NL + "                    ";
  protected final String TEXT_37 = NL + "                    ";
  protected final String TEXT_38 = NL + "                    return ";
  protected final String TEXT_39 = ";" + NL + "                }" + NL + "            }";
  protected final String TEXT_40 = NL;
  protected final String TEXT_41 = NL + "            // No sparkcode generated for unnecessary ";
  protected final String TEXT_42 = NL;
  protected final String TEXT_43 = NL + "            public static class ";
  protected final String TEXT_44 = "TrueFilter implements org.apache.spark.api.java.function.Function<scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase>, Boolean> {" + NL + "" + NL + "                public Boolean call(scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> arg0)" + NL + "                        throws Exception {" + NL + "                    return arg0._1;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_45 = "FalseFilter implements org.apache.spark.api.java.function.Function<scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase>, Boolean> {" + NL + "" + NL + "                public Boolean call(scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> arg0)" + NL + "                        throws Exception {" + NL + "                    return !arg0._1;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_46 = "ToNullWritableMain implements ";
  protected final String TEXT_47 = " {" + NL + "" + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_48 = "ToNullWritableMain(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_49 = " call(" + NL + "                        scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> data){";
  protected final String TEXT_50 = NL + "                    ";
  protected final String TEXT_51 = " ";
  protected final String TEXT_52 = " = (";
  protected final String TEXT_53 = ")data._2;" + NL + "                    return ";
  protected final String TEXT_54 = ";" + NL + "                }" + NL + "            }" + NL + "" + NL + "            public static class ";
  protected final String TEXT_55 = "ToNullWritableReject implements ";
  protected final String TEXT_56 = " {" + NL + "" + NL + "                private ContextProperties context = null;" + NL + "" + NL + "                public ";
  protected final String TEXT_57 = "ToNullWritableReject(JobConf job) {" + NL + "                    this.context = new ContextProperties(job);" + NL + "                }" + NL + "" + NL + "                public ";
  protected final String TEXT_58 = " call(" + NL + "                        scala.Tuple2<Boolean, org.apache.avro.specific.SpecificRecordBase> data){";
  protected final String TEXT_59 = NL + "                        ";
  protected final String TEXT_60 = " ";
  protected final String TEXT_61 = " = (";
  protected final String TEXT_62 = ")data._2;" + NL + "                    return ";
  protected final String TEXT_63 = ";" + NL + "                }" + NL + "            }";
  protected final String TEXT_64 = NL + "            // No sparkconfig generated for unnecessary ";
  protected final String TEXT_65 = NL;
  protected final String TEXT_66 = NL + "            // Extract data." + NL;
  protected final String TEXT_67 = NL + "            ";
  protected final String TEXT_68 = "<Boolean, org.apache.avro.specific.SpecificRecordBase> temporary_rdd_";
  protected final String TEXT_69 = " = rdd_";
  protected final String TEXT_70 = ".";
  protected final String TEXT_71 = "(new ";
  protected final String TEXT_72 = "(job));" + NL + "" + NL + "            // Main flow" + NL;
  protected final String TEXT_73 = NL + "            ";
  protected final String TEXT_74 = " rdd_";
  protected final String TEXT_75 = " = temporary_rdd_";
  protected final String TEXT_76 = NL + "                  .filter(new ";
  protected final String TEXT_77 = "TrueFilter())" + NL + "                    .";
  protected final String TEXT_78 = "(new ";
  protected final String TEXT_79 = "ToNullWritableMain(job));" + NL + "" + NL + "            // Reject flow";
  protected final String TEXT_80 = NL + "            ";
  protected final String TEXT_81 = " rdd_";
  protected final String TEXT_82 = " = temporary_rdd_";
  protected final String TEXT_83 = NL + "                    .filter(new ";
  protected final String TEXT_84 = "FalseFilter())" + NL + "                    .";
  protected final String TEXT_85 = "(new ";
  protected final String TEXT_86 = "ToNullWritableReject(job));";
  protected final String TEXT_87 = NL + "            ";
  protected final String TEXT_88 = " rdd_";
  protected final String TEXT_89 = " = rdd_";
  protected final String TEXT_90 = ".";
  protected final String TEXT_91 = "(new ";
  protected final String TEXT_92 = "(job));";
  protected final String TEXT_93 = NL;
  protected final String TEXT_94 = NL + "        org.talend.survivorship.SurvivorshipManager manager_";
  protected final String TEXT_95 = "=null;" + NL + "        Object outPutGroupId_";
  protected final String TEXT_96 = " = null;" + NL + "        int groupCount_";
  protected final String TEXT_97 = " = 0;" + NL + "        Object[][] groupValues_";
  protected final String TEXT_98 = " = new Object[1][1];" + NL + "        Object tmpValue_";
  protected final String TEXT_99 = " = null;" + NL + "        java.util.Map<String, Object> survivors_";
  protected final String TEXT_100 = " = null;" + NL + "        java.util.List<java.util.HashSet<String>> conflicts_";
  protected final String TEXT_101 = " = null;" + NL + "" + NL + "        // current row number" + NL + "        private long currentRow=0l;" + NL + "        Object lastGroupID_";
  protected final String TEXT_102 = " = null;" + NL + "        //a row with all column values" + NL + "        private Object[] columnValues_";
  protected final String TEXT_103 = " = new Object[1];" + NL + "        //Store a group records" + NL + "        java.util.List<Object[]> recordLists__";
  protected final String TEXT_104 = " =new java.util.ArrayList<Object[]>();" + NL + "        // a count of all rows from RDD" + NL + "        private long rowCount;" + NL + "        public void setRowCount(long count){" + NL + "            this.rowCount=count;" + NL + "        }" + NL + "        ";
  protected final String TEXT_105 = NL + NL + "        // init SurvivorshipManager." + NL + "        if(manager_";
  protected final String TEXT_106 = "==null){" + NL + "            //transmit the rule path only for useLocalMode,or else the String \"Real_spark_relative_path\" represent a real cluster." + NL + "            manager_";
  protected final String TEXT_107 = " = new org.talend.survivorship.SurvivorshipManager(\"true\".equals(\"";
  protected final String TEXT_108 = "\")?\"";
  protected final String TEXT_109 = "\":\"Real_spark_relative_path\", ";
  protected final String TEXT_110 = ");" + NL + "            manager_";
  protected final String TEXT_111 = ".setJobName( jobName);" + NL + "            manager_";
  protected final String TEXT_112 = ".setJobVersion(jobVersion);" + NL + "        ";
  protected final String TEXT_113 = NL + "            manager_";
  protected final String TEXT_114 = ".addColumn(\"";
  protected final String TEXT_115 = "\",\"";
  protected final String TEXT_116 = "\");" + NL + "            ";
  protected final String TEXT_117 = NL + "            manager_";
  protected final String TEXT_118 = ".addRuleDefinition(" + NL + "                new org.talend.survivorship.model.RuleDefinition(" + NL + "                    org.talend.survivorship.model.RuleDefinition.Order.";
  protected final String TEXT_119 = NL + "                    ,";
  protected final String TEXT_120 = NL + "                    ,\"";
  protected final String TEXT_121 = "\"                                " + NL + "                    ,";
  protected final String TEXT_122 = "null";
  protected final String TEXT_123 = "org.talend.survivorship.model.RuleDefinition.Function.";
  protected final String TEXT_124 = NL + "                    ,";
  protected final String TEXT_125 = NL + "                    ,\"";
  protected final String TEXT_126 = "\"" + NL + "                    ,";
  protected final String TEXT_127 = NL + "                )" + NL + "            );";
  protected final String TEXT_128 = NL + "            org.talend.survivorship.model.Column column=null;" + NL + "            org.talend.survivorship.model.ConflictRuleDefinition conflictRule=null;";
  protected final String TEXT_129 = NL + "                column=manager_";
  protected final String TEXT_130 = ".getColumnByName(\"";
  protected final String TEXT_131 = "\");" + NL + "                conflictRule=new org.talend.survivorship.model.ConflictRuleDefinition(" + NL + "                        null" + NL + "                        ,";
  protected final String TEXT_132 = NL + "                        ,\"";
  protected final String TEXT_133 = "\"                                " + NL + "                        ,";
  protected final String TEXT_134 = "null";
  protected final String TEXT_135 = "org.talend.survivorship.model.ConflictRuleDefinition.Function.";
  protected final String TEXT_136 = NL + "                        ,";
  protected final String TEXT_137 = NL + "                        ,\"";
  protected final String TEXT_138 = "\"" + NL + "                        ,";
  protected final String TEXT_139 = NL + "                        ,";
  protected final String TEXT_140 = NL + "                        ,";
  protected final String TEXT_141 = NL + "                    );" + NL + "                column.getConflictResolveList().add(conflictRule);";
  protected final String TEXT_142 = NL + "         manager_";
  protected final String TEXT_143 = ".initKnowledgeBase();" + NL + "         //check if the conflict rules are valid." + NL + "         java.util.Map<String,java.util.List<String>> errorMap=manager_";
  protected final String TEXT_144 = ".checkConflictRuleValid();" + NL + "         if(errorMap.size()>0){" + NL + "             java.util.Iterator<?> iter = errorMap.entrySet().iterator();" + NL + "             StringBuilder erroMessages=new StringBuilder();" + NL + "             while (iter.hasNext()){" + NL + "                java.util.Map.Entry<String,java.util.List<String>> entry = (java.util.Map.Entry) iter.next();" + NL + "                erroMessages.append(\"'\"+entry.getKey()+\"':\\n\");" + NL + "                for(String value:entry.getValue()){" + NL + "                    for(int index=0;index<(\"'\"+entry.getKey()+\"':\").length();index++){" + NL + "                        erroMessages.append(\" \");" + NL + "                    }" + NL + "                    erroMessages.append(value+\"\\n\");" + NL + "                }" + NL + "             }" + NL + "             throw new Exception(erroMessages.toString());" + NL + "         }";
  protected final String TEXT_145 = NL + "            manager_";
  protected final String TEXT_146 = ".initKnowledgeBase();";
  protected final String TEXT_147 = NL + "        }";
  protected final String TEXT_148 = NL + "          " + NL + "          if(currentRow == 0l&&lastGroupID_";
  protected final String TEXT_149 = "==null){" + NL + "              lastGroupID_";
  protected final String TEXT_150 = "=";
  protected final String TEXT_151 = ".";
  protected final String TEXT_152 = ";" + NL + "          }" + NL + "          currentRow++;" + NL + "          " + NL + "        " + NL + "          int recordsCount_";
  protected final String TEXT_153 = "=0;" + NL + "          boolean isNeedOutput=false;" + NL + "          //when it is the first Row in a group, need to set outPutGroupId." + NL + "          if(groupCount_";
  protected final String TEXT_154 = " == 0){" + NL + "              outPutGroupId_";
  protected final String TEXT_155 = " = ";
  protected final String TEXT_156 = ".";
  protected final String TEXT_157 = ";" + NL + "          }" + NL + "          //if current groupId is not equals last groupId,output last group and reset some varibles." + NL + "          if(!lastGroupID_";
  protected final String TEXT_158 = ".equals(";
  protected final String TEXT_159 = ".";
  protected final String TEXT_160 = ")&&recordLists__";
  protected final String TEXT_161 = ".size()>0){" + NL + "              isNeedOutput=true;" + NL + "              groupValues_";
  protected final String TEXT_162 = " = new Object[groupCount_";
  protected final String TEXT_163 = "][";
  protected final String TEXT_164 = "];" + NL + "              for(int i=0;i<groupCount_";
  protected final String TEXT_165 = ";i++){" + NL + "                  groupValues_";
  protected final String TEXT_166 = "[i]=recordLists__";
  protected final String TEXT_167 = ".get(i);" + NL + "              }" + NL + "              outPutGroupId_";
  protected final String TEXT_168 = " =lastGroupID_";
  protected final String TEXT_169 = ";" + NL + "              groupCount_";
  protected final String TEXT_170 = "=0;" + NL + "              recordLists__";
  protected final String TEXT_171 = ".clear();" + NL + "             " + NL + "          }" + NL + "          groupCount_";
  protected final String TEXT_172 = "++;" + NL + "          columnValues_";
  protected final String TEXT_173 = "=new Object[";
  protected final String TEXT_174 = "];";
  protected final String TEXT_175 = NL + "          columnValues_";
  protected final String TEXT_176 = "[";
  protected final String TEXT_177 = "] = ";
  protected final String TEXT_178 = ".";
  protected final String TEXT_179 = " ;" + NL + "          ";
  protected final String TEXT_180 = NL + "          recordLists__";
  protected final String TEXT_181 = ".add( columnValues_";
  protected final String TEXT_182 = ");" + NL + "          //do something when it is the last record form RDD." + NL + "          if(currentRow==this.rowCount){" + NL + "              isNeedOutput=true;" + NL + "              groupValues_";
  protected final String TEXT_183 = " = new Object[groupCount_";
  protected final String TEXT_184 = "][";
  protected final String TEXT_185 = "];" + NL + "              for(int i=0;i<groupCount_";
  protected final String TEXT_186 = ";i++){" + NL + "                  groupValues_";
  protected final String TEXT_187 = "[i]=recordLists__";
  protected final String TEXT_188 = ".get(i);" + NL + "              }" + NL + "              outPutGroupId_";
  protected final String TEXT_189 = " =";
  protected final String TEXT_190 = ".";
  protected final String TEXT_191 = ";" + NL + "          }" + NL + "          //call API when it needs to output a group." + NL + "          if(isNeedOutput){" + NL + "              manager_";
  protected final String TEXT_192 = ".runSession(groupValues_";
  protected final String TEXT_193 = ");" + NL + "              recordsCount_";
  protected final String TEXT_194 = " = groupValues_";
  protected final String TEXT_195 = ".length + 1;" + NL + "              survivors_";
  protected final String TEXT_196 = " = manager_";
  protected final String TEXT_197 = ".getSurvivorMap();" + NL + "              conflicts_";
  protected final String TEXT_198 = " = manager_";
  protected final String TEXT_199 = ".getConflictList();" + NL + "          }" + NL + "" + NL + "          //output previous or the last one group";
  protected final String TEXT_200 = NL + "          for(int i_";
  protected final String TEXT_201 = " = 0; isNeedOutput && i_";
  protected final String TEXT_202 = " < recordsCount_";
  protected final String TEXT_203 = "; i_";
  protected final String TEXT_204 = "++){" + NL + "              //because a group data is output in this loop, we need to create a new outputStructure instance here.";
  protected final String TEXT_205 = NL + "              ";
  protected final String TEXT_206 = "=new ";
  protected final String TEXT_207 = "();";
  protected final String TEXT_208 = NL + "                  if(i_";
  protected final String TEXT_209 = " != recordsCount_";
  protected final String TEXT_210 = " - 1){" + NL + "                      tmpValue_";
  protected final String TEXT_211 = " = groupValues_";
  protected final String TEXT_212 = "[i_";
  protected final String TEXT_213 = "][";
  protected final String TEXT_214 = "];" + NL + "                  }else{" + NL + "                      tmpValue_";
  protected final String TEXT_215 = " = survivors_";
  protected final String TEXT_216 = ".get(\"";
  protected final String TEXT_217 = "\");" + NL + "                  }" + NL + "" + NL + "                  if(tmpValue_";
  protected final String TEXT_218 = " != null){";
  protected final String TEXT_219 = "              ";
  protected final String TEXT_220 = NL + "                          ";
  protected final String TEXT_221 = ".";
  protected final String TEXT_222 = " = tmpValue_";
  protected final String TEXT_223 = ".toString();";
  protected final String TEXT_224 = NL + "                          if(tmpValue_";
  protected final String TEXT_225 = " instanceof java.util.Date){";
  protected final String TEXT_226 = NL + "                              ";
  protected final String TEXT_227 = ".";
  protected final String TEXT_228 = " = (java.util.Date)tmpValue_";
  protected final String TEXT_229 = ";" + NL + "                          }else{";
  protected final String TEXT_230 = NL + "                              ";
  protected final String TEXT_231 = ".";
  protected final String TEXT_232 = " = ParserUtils.parseTo_Date(tmpValue_";
  protected final String TEXT_233 = ".toString(), ";
  protected final String TEXT_234 = ");" + NL + "                          }";
  protected final String TEXT_235 = NL + "                          ";
  protected final String TEXT_236 = ".";
  protected final String TEXT_237 = " = tmpValue_";
  protected final String TEXT_238 = ".toString().getBytes();";
  protected final String TEXT_239 = "                                  ";
  protected final String TEXT_240 = NL + "                          ";
  protected final String TEXT_241 = ".";
  protected final String TEXT_242 = " = ParserUtils.parseTo_";
  protected final String TEXT_243 = "(tmpValue_";
  protected final String TEXT_244 = ".toString()); ";
  protected final String TEXT_245 = NL + "                  }else{";
  protected final String TEXT_246 = NL + "                      ";
  protected final String TEXT_247 = ".";
  protected final String TEXT_248 = " = ";
  protected final String TEXT_249 = ";" + NL + "                  }";
  protected final String TEXT_250 = NL + "                      " + NL + "              if(i_";
  protected final String TEXT_251 = " != recordsCount_";
  protected final String TEXT_252 = " - 1){";
  protected final String TEXT_253 = NL + "                  ";
  protected final String TEXT_254 = ".CONFLICT = conflicts_";
  protected final String TEXT_255 = ".get(i_";
  protected final String TEXT_256 = ").toString();";
  protected final String TEXT_257 = NL + "                  ";
  protected final String TEXT_258 = ".SURVIVOR = false;" + NL + "              }else{ // master record";
  protected final String TEXT_259 = NL + "                  ";
  protected final String TEXT_260 = ".CONFLICT = manager_";
  protected final String TEXT_261 = ".getConflictsOfSurvivor().toString();";
  protected final String TEXT_262 = NL + "                  ";
  protected final String TEXT_263 = ".SURVIVOR = true;";
  protected final String TEXT_264 = NL + "                  ";
  protected final String TEXT_265 = ".";
  protected final String TEXT_266 = " = ParserUtils.parseTo_";
  protected final String TEXT_267 = "(outPutGroupId_";
  protected final String TEXT_268 = ".toString()); " + NL + "              }" + NL + "           // Emit the parsed structure on the main output.";
  protected final String TEXT_269 = NL + "              ";
  protected final String TEXT_270 = NL + "              ";
  protected final String TEXT_271 = NL + "  " + NL + "           }" + NL + "          //~ output previous or the last one group" + NL + "          //reset the currentGroupId to lastGroupId" + NL + "          lastGroupID_";
  protected final String TEXT_272 = "=";
  protected final String TEXT_273 = ".";
  protected final String TEXT_274 = ";";
  protected final String TEXT_275 = NL + "   int pos_";
  protected final String TEXT_276 = " = \"";
  protected final String TEXT_277 = "\".lastIndexOf('/');" + NL + "   String projectName = \"\";" + NL + "   if (pos_";
  protected final String TEXT_278 = " > 0 && pos_";
  protected final String TEXT_279 = " < \"";
  protected final String TEXT_280 = "\".length() + 1) {" + NL + "       projectName = \"";
  protected final String TEXT_281 = "\".substring(pos_";
  protected final String TEXT_282 = " + 1);" + NL + "   }" + NL + "   String rulePath=\"\";" + NL + "   String itemsFolder=\"items\";" + NL + "   java.io.File studioFolder=new java.io.File(\"";
  protected final String TEXT_283 = "\"+\"/metadata/survivorship/\"+";
  protected final String TEXT_284 = "+\"/\");" + NL + "   java.io.File rulesFolder=null;" + NL + "   if(studioFolder.exists()){//studio" + NL + "       rulePath=studioFolder.getPath();" + NL + "       rulesFolder=new java.io.File(rulePath); " + NL + "   }else if (new java.io.File(itemsFolder).exists()){//items foder for export job" + NL + "       rulePath=\"items/\"+projectName.toLowerCase()+\"/metadata/survivorship/\"+";
  protected final String TEXT_285 = "+\"/\";" + NL + "       rulesFolder=new java.io.File(rulePath); " + NL + "   }else{ //items folder for export in classpath(TIC job)" + NL + "       rulePath=\"items/\"+projectName.toLowerCase()+\"/metadata/survivorship/\"+";
  protected final String TEXT_286 = "+\"/\";" + NL + "       java.net.URL resource = this.getClass().getClassLoader().getResource(rulePath);" + NL + "       if(resource!=null){" + NL + "           rulesFolder=new java.io.File(resource.toURI());" + NL + "       }" + NL + "   }" + NL + "   //java.net.URL url = this.getClass().getClassLoader().getResource(\"metadata/survivorship/\"+";
  protected final String TEXT_287 = "+\"/\" );" + NL + "   if(rulesFolder==null||!rulesFolder.exists()){";
  protected final String TEXT_288 = NL + "          log.error(\"can not find the rule files in \"+rulePath+\".Please re-generate rules!\");";
  protected final String TEXT_289 = NL + "      return ;" + NL + "   }" + NL + "      " + NL + "   java.io.File[] listFiles = rulesFolder.listFiles();" + NL + "   for(java.io.File droolsFile:listFiles){";
  protected final String TEXT_290 = NL + "            ctx.sc().addFile(droolsFile.toURI().toString());  ";
  protected final String TEXT_291 = NL + "            ctx.addFile(droolsFile.toURI().toString());   ";
  protected final String TEXT_292 = NL + "            " + NL + "   }" + NL + "      ";
  protected final String TEXT_293 = NL;
  protected final String TEXT_294 = " rdd_";
  protected final String TEXT_295 = " = rdd_";
  protected final String TEXT_296 = ".";
  protected final String TEXT_297 = "(new ";
  protected final String TEXT_298 = "(job));";
  protected final String TEXT_299 = NL + "    //re-partition by GID for batch mode.if the input components is partition the RDD,the survivorship will get error.Becasuse the survivorship logic only adapt to compute and output a group data.and the parttion by key is defined in XML \"PARTITIONING=\"GRP_ID\"" + NL + "    //partition numbers from previous rdd. if the number >1,need to re-partition at here." + NL + "    int parttionNum=rdd_";
  protected final String TEXT_300 = ".getNumPartitions();";
  protected final String TEXT_301 = NL + "    ";
  protected final String TEXT_302 = " rdd_";
  protected final String TEXT_303 = "_partition=null;" + NL + "    if(parttionNum>1){" + NL + "        rdd_";
  protected final String TEXT_304 = "_partition = rdd_";
  protected final String TEXT_305 = ".partitionBy(new org.apache.spark.HashPartitioner(parttionNum));" + NL + "    }else{" + NL + "        rdd_";
  protected final String TEXT_306 = "_partition=rdd_";
  protected final String TEXT_307 = ";" + NL + "    }";
  protected final String TEXT_308 = NL + "    ";
  protected final String TEXT_309 = " outputFunction_";
  protected final String TEXT_310 = "=new ";
  protected final String TEXT_311 = "(job);" + NL + "    outputFunction_";
  protected final String TEXT_312 = ".setRowCount(rdd_";
  protected final String TEXT_313 = "_partition.count());";
  protected final String TEXT_314 = NL + "    ";
  protected final String TEXT_315 = " rdd_";
  protected final String TEXT_316 = " =  rdd_";
  protected final String TEXT_317 = "_partition.";
  protected final String TEXT_318 = "(outputFunction_";
  protected final String TEXT_319 = ");";
  protected final String TEXT_320 = NL + "        ";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
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
		
    stringBuffer.append(TEXT_2);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_3);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementation(getOutValueClass(), getInValueClass()));
    stringBuffer.append(TEXT_4);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_5);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_6);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedType(this.outValueClass.toString()));
    stringBuffer.append(TEXT_7);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_8);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_9);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_10);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    stringBuffer.append(TEXT_11);
    stringBuffer.append(this.sparkFunction.getCodeKeyMapping(getInValue()));
    stringBuffer.append(TEXT_12);
    
	                this.transformer.generateTransformContextInitialization();
	                this.transformer.generateTransform(true);
	                
    stringBuffer.append(TEXT_13);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn(this.getOutValue(), this.getOutValueClass()));
    stringBuffer.append(TEXT_14);
    
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
        
    stringBuffer.append(TEXT_15);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_16);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementationOutputFixedType(getInValueClass(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_17);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_18);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_19);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedTypeFixedType((String)this.outKeyClass, "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_20);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_21);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_22);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_23);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    
                    this.transformer.generateTransformContextInitialization();
                    this.transformer.generateTransform(true);
	                if(this.sparkFunction.getCodeFunctionReturn()!=null) {
                
    stringBuffer.append(TEXT_24);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn());
    stringBuffer.append(TEXT_25);
    
	            	}
                
    stringBuffer.append(TEXT_26);
    
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
        
    stringBuffer.append(TEXT_27);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_28);
    stringBuffer.append(this.sparkFunction.getCodeFunctionImplementation(getOutValueClass(), getInValueClass()));
    stringBuffer.append(TEXT_29);
    
                this.transformer.generateHelperClasses(true);
                this.transformer.generateTransformContextDeclaration();
                
    stringBuffer.append(TEXT_30);
    stringBuffer.append(this.sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_31);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturnedType(this.outValueClass.toString()));
    stringBuffer.append(TEXT_32);
    stringBuffer.append(this.sparkFunction.getCodeImplementedMethod());
    stringBuffer.append(TEXT_33);
    stringBuffer.append(this.sparkFunction.getCodeFunctionArgument(getInValueClass()));
    stringBuffer.append(TEXT_34);
    stringBuffer.append(this.sparkFunction.getCodeThrowException());
    stringBuffer.append(TEXT_35);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(this.sparkFunction.getMethodHeader(this.outValueClass, this.outValue, this.inValueClass, this.inValue));
    stringBuffer.append(TEXT_37);
    stringBuffer.append(this.sparkFunction.getCodeKeyMapping(getInValue()));
    
                        this.transformer.generateTransformContextInitialization();
                        this.transformer.generateTransform(true);
                     
    stringBuffer.append(TEXT_38);
    stringBuffer.append(this.sparkFunction.getCodeFunctionReturn(this.getOutValue(), this.getOutValueClass()));
    stringBuffer.append(TEXT_39);
    
        }
    }

    stringBuffer.append(TEXT_40);
    

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
            
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    
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

            
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(extractSparkFunction.getCodeFunctionImplementationInputFixedType(transformer.getOutConnMainTypeName(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturnedType(transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_49);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(transformer.getOutConnMainTypeName());
    stringBuffer.append(TEXT_51);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_52);
    stringBuffer.append(transformer.getOutConnMainTypeName());
    stringBuffer.append(TEXT_53);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturn(transformer.getOutConnMainName(), transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(extractSparkFunction.getCodeFunctionImplementationInputFixedType(transformer.getOutConnRejectTypeName(), "Boolean", "org.apache.avro.specific.SpecificRecordBase"));
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturnedType(transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_58);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(transformer.getOutConnRejectTypeName());
    stringBuffer.append(TEXT_60);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_61);
    stringBuffer.append(transformer.getOutConnRejectTypeName());
    stringBuffer.append(TEXT_62);
    stringBuffer.append(extractSparkFunction.getCodeFunctionReturn(transformer.getOutConnRejectName(), transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_63);
    
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
            
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    
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
            
    stringBuffer.append(TEXT_66);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(sparkFunction.isStreaming() ?"org.apache.spark.streaming.api.java.JavaPairDStream":"org.apache.spark.api.java.JavaPairRDD");
    stringBuffer.append(TEXT_68);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_69);
    stringBuffer.append(transformer.getInConnName());
    stringBuffer.append(TEXT_70);
    stringBuffer.append(localFunctionType);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_72);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_74);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_75);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(extractFunctionType);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_81);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_82);
    stringBuffer.append(transformer.getOutConnMainName());
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(extractFunctionType);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    
        } else {
            functionGenerator = new SOFunctionGenerator(transformer, sparkFunction);

            functionGenerator.init(node, cid, null, transformer.getInConnName(), null,
                    transformer.getOutConnMainName() != null
                        ? transformer.getOutConnMainName()
                                : transformer.getOutConnRejectName());
            
    stringBuffer.append(TEXT_87);
    stringBuffer.append(sparkFunction.getConfigReturnedType(transformer.getOutConnMainName() != null ? transformer.getOutConnMainTypeName() : transformer.getOutConnRejectTypeName()));
    stringBuffer.append(TEXT_88);
    stringBuffer.append(transformer.getOutConnMainName() != null ? transformer.getOutConnMainName() : transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_89);
    stringBuffer.append(transformer.getInConnName());
    stringBuffer.append(TEXT_90);
    stringBuffer.append(sparkFunction.getConfigTransformation());
    stringBuffer.append(TEXT_91);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_92);
    
        }
    }
}


    stringBuffer.append(TEXT_93);
    

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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    
    }

    public void generateTransformContextInitialization(){
        String jobName = node.getProcess().getName();
        String jobVersion = node.getProcess().getVersion();
        
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(useLocalMode);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    
        for(IMetadataColumn column : inputColumns){
            String typeName = "";
            typeName = column.getTalendType().substring(column.getTalendType().indexOf("_")+1);
            if(typeName.equals("Date")){
                typeName = "java.util.Date";
            }
        
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_115);
    stringBuffer.append(typeName);
    stringBuffer.append(TEXT_116);
    
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
            
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(relation);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(reference);
    stringBuffer.append(TEXT_121);
    if("None".equals(function)){
    stringBuffer.append(TEXT_122);
    }else{
    stringBuffer.append(TEXT_123);
    stringBuffer.append(function);
    }
    stringBuffer.append(TEXT_124);
    stringBuffer.append("\"\"".equals(operationValue)?null:operationValue);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(target);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(ignoreNull);
    stringBuffer.append(TEXT_127);
    
        }
       
      //check if the conflict rules are valid
        if("true".equals(isDefinedConflict)){
        
    stringBuffer.append(TEXT_128);
    
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
         
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(conflictingCol);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(referenceCol);
    stringBuffer.append(TEXT_133);
    if("None".equals(function)){
    stringBuffer.append(TEXT_134);
    }else{
    stringBuffer.append(TEXT_135);
    stringBuffer.append(function);
    }
    stringBuffer.append(TEXT_136);
    stringBuffer.append("\"\"".equals(operationValue)?null:operationValue);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(conflictingCol);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(ignoreNull);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(removeDuplicate);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(disable);
    stringBuffer.append(TEXT_141);
    
            }
         
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    
        }else{
            
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    
        }
        
    stringBuffer.append(TEXT_147);
    

    }

    public void generateTransform() {
        generateTransform(false);
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow.
     */
    public void generateTransform(boolean hasAReturnedType) {
          
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(nbMetadataColumns);
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_174);
    
          for( int i = 0; i < nbMetadataColumns; i++) {
              IMetadataColumn metadataColumn = inputColumns.get(i);
          
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(metadataColumn.getLabel());
    stringBuffer.append(TEXT_179);
    
          }
          
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(nbMetadataColumns);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(grpIDColumnName);
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
     String grpIDColumnType = ""; 
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(getCodeGenArgument().getRecordStructName(outgoingConnName));
    stringBuffer.append(TEXT_207);
     
              for( int i = 0; i < nbMetadataColumns; i++) {
                  
                  IMetadataColumn column = inputColumns.get(i);
                              
                  String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                  String defaultValue =JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
                  JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                                  
                  String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
                  if(grpIDColumnName.equals(column.getLabel())){
                      grpIDColumnType = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());                            
                  }
                  
    stringBuffer.append(TEXT_208);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_217);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_218);
                              
                      if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
                      
    stringBuffer.append(TEXT_219);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_222);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_223);
    
                      } else if(javaType == JavaTypesManager.DATE) { // Date
                      
    stringBuffer.append(TEXT_224);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_227);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_228);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_232);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_233);
    stringBuffer.append(patternValue);
    stringBuffer.append(TEXT_234);
    
                      } else if(javaType == JavaTypesManager.BYTE_ARRAY) { //byte[]
                      
    stringBuffer.append(TEXT_235);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_236);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_237);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_238);
    
                      } else {
                      
    stringBuffer.append(TEXT_239);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_242);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_243);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_244);
    
                      } 
                      
    stringBuffer.append(TEXT_245);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_248);
    stringBuffer.append(defaultValue);
    stringBuffer.append(TEXT_249);
    
              }
              
    stringBuffer.append(TEXT_250);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_251);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(TEXT_259);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(TEXT_264);
    stringBuffer.append(outgoingConnName);
    stringBuffer.append(TEXT_265);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(grpIDColumnType);
    stringBuffer.append(TEXT_267);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(TEXT_269);
    stringBuffer.append(getRowTransform().getCodeToEmit(false));
    stringBuffer.append(TEXT_270);
    stringBuffer.append(getRowTransform().getCodeToInitOut(null == getOutConnMain(), newOutColumns));
    stringBuffer.append(TEXT_271);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(incomingConnName);
    stringBuffer.append(TEXT_273);
    stringBuffer.append(grpIDColumnName);
    stringBuffer.append(TEXT_274);
    
    }
}

    
org.talend.designer.spark.generator.SparkFunction sparkFunction = null;
String requiredInputType = bigDataNode.getRequiredInputType();
String requiredOutputType = bigDataNode.getRequiredOutputType();
String incomingType = bigDataNode.getIncomingType();
String outgoingType = bigDataNode.getOutgoingType();
String incomingConnName =  bigDataNode.getIncomingConnections().get(0).getName(); 
String outgoingConnName =bigDataNode.getOutgoingConnections().get(0).getName();
final String inConnTypeName = codeGenArgument.getRecordStructName(incomingConnName);
boolean inputIsPair = requiredInputType != null ? "KEYVALUE".equals(requiredInputType) : "KEYVALUE".equals(incomingType);


String packageName = ElementParameterParser.getValue(node, "__PACKAGE_NAME__");

String projectDir = ElementParameterParser.getValue(node.getProcess(), "__TDQ_DEFAULT_PROJECT_DIR__");  
final boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
//Find the tSparkConfiguration and define which Spark version is currently used.
final java.util.List<? extends INode> sparkConfigs = node.getProcess().getNodesOfType("tSparkConfiguration");
INode sparkConfig = null;
if(sparkConfigs != null && sparkConfigs.size() > 0) {
 sparkConfig = sparkConfigs.get(0);
}
boolean useLocalMode = false;
if(sparkConfig != null) {
    useLocalMode = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SPARK_LOCAL_MODE__"));
}

String type = requiredOutputType == null ? outgoingType : requiredOutputType;
if("KEYVALUE".equals(type)) {
 sparkFunction = new org.talend.designer.spark.generator.FlatMapToPairFunction(inputIsPair, codeGenArgument.getSparkVersion());
} else if("VALUE".equals(type)) {
 sparkFunction = new org.talend.designer.spark.generator.FlatMapFunction(inputIsPair, codeGenArgument.getSparkVersion());
}

boolean isNodeInBatchMode = org.talend.designer.common.tmap.LookupUtil.isNodeInBatchMode(node);
if("SPARKSTREAMING".equals(node.getComponent().getType()) && !isNodeInBatchMode) {
 sparkFunction.setStreaming(true);
}

final SparkRowTransformUtil sparkTransformUtil = new SparkRowTransformUtil(sparkFunction);
final TRuleSurvivorshipUtil tRuleSurvivorshipUtil = new TRuleSurvivorshipUtil(
     node, codeGenArgument, sparkTransformUtil);
//for a real spark cluster,upload the local rule files to Spark node
if(!useLocalMode){

    stringBuffer.append(TEXT_275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_281);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(projectDir);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_286);
    stringBuffer.append(packageName);
    stringBuffer.append(TEXT_287);
     if(isLog4jEnabled){
    stringBuffer.append(TEXT_288);
    }
    stringBuffer.append(TEXT_289);
    
        if("SPARKSTREAMING".equals(node.getComponent().getType())) {
    
    stringBuffer.append(TEXT_290);
    
        } else {
    
    stringBuffer.append(TEXT_291);
    
        }
     
    stringBuffer.append(TEXT_292);
    
}
//sparkTransformUtil.generateSparkConfig(tRuleSurvivorshipUtil, sparkFunction);
org.talend.designer.spark.generator.FunctionGenerator  functionGenerator = new SOFunctionGenerator(tRuleSurvivorshipUtil, sparkFunction);

functionGenerator = new SOFunctionGenerator(tRuleSurvivorshipUtil, sparkFunction);

functionGenerator.init(node, cid, null, tRuleSurvivorshipUtil.getInConnName(), null, tRuleSurvivorshipUtil.getOutConnMainName() != null? tRuleSurvivorshipUtil.getOutConnMainName(): tRuleSurvivorshipUtil.getOutConnRejectName());
if("SPARKSTREAMING".equals(node.getComponent().getType()) && !isNodeInBatchMode) {

    stringBuffer.append(TEXT_293);
    stringBuffer.append(sparkFunction.getConfigReturnedType(tRuleSurvivorshipUtil.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_294);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_295);
    stringBuffer.append(tRuleSurvivorshipUtil.getInConnName());
    stringBuffer.append(TEXT_296);
    stringBuffer.append(sparkFunction.getConfigTransformation());
    stringBuffer.append(TEXT_297);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_298);
    }else{
    stringBuffer.append(TEXT_299);
    stringBuffer.append(tRuleSurvivorshipUtil.getInConnName());
    stringBuffer.append(TEXT_300);
    stringBuffer.append(TEXT_301);
    stringBuffer.append(sparkFunction.getConfigReturnedType(inConnTypeName));
    stringBuffer.append(TEXT_302);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_303);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_304);
    stringBuffer.append(tRuleSurvivorshipUtil.getInConnName());
    stringBuffer.append(TEXT_305);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_306);
    stringBuffer.append(tRuleSurvivorshipUtil.getInConnName());
    stringBuffer.append(TEXT_307);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_309);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_310);
    stringBuffer.append(sparkFunction.getClassName(cid));
    stringBuffer.append(TEXT_311);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_313);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(sparkFunction.getConfigReturnedType(tRuleSurvivorshipUtil.getOutConnMainTypeName()));
    stringBuffer.append(TEXT_315);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_316);
    stringBuffer.append(tRuleSurvivorshipUtil.getOutConnMainName());
    stringBuffer.append(TEXT_317);
    stringBuffer.append(sparkFunction.getConfigTransformation());
    stringBuffer.append(TEXT_318);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_319);
    }
    stringBuffer.append(TEXT_320);
    return stringBuffer.toString();
  }
}
