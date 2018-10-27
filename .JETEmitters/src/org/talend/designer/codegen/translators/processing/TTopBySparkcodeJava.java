package org.talend.designer.codegen.translators.processing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IBigDataNode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TTopBySparkcodeJava
{
  protected static String nl;
  public static synchronized TTopBySparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TTopBySparkcodeJava result = new TTopBySparkcodeJava();
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
  protected final String TEXT_92 = NL + "        com.google.common.collect.TreeMultimap<";
  protected final String TEXT_93 = "_Comparator, ";
  protected final String TEXT_94 = "> topK " + NL + "            = com.google.common.collect.TreeMultimap.create();" + NL + "" + NL + "        // keep the topK" + NL + "        for (";
  protected final String TEXT_95 = " value : ";
  protected final String TEXT_96 = "){" + NL + "            topK.put(new ";
  protected final String TEXT_97 = "_Comparator(value), value);" + NL + "            if (topK.size() > ";
  protected final String TEXT_98 = ") {" + NL + "                java.util.Map.Entry<";
  protected final String TEXT_99 = "_Comparator, ";
  protected final String TEXT_100 = "> valueToDelete = topK.entries().iterator().next();" + NL + "                topK.remove(valueToDelete.getKey(), valueToDelete.getValue());" + NL + "            }" + NL + "        }" + NL + "" + NL + "        // Set put the topK into the output" + NL + "        for(";
  protected final String TEXT_101 = " topElement: topK.values()) {" + NL + "            outputs.addFirst(new scala.Tuple2(data._1(), topElement));" + NL + "        }";
  protected final String TEXT_102 = NL;
  protected final String TEXT_103 = NL + "                        public java.nio.ByteBuffer ";
  protected final String TEXT_104 = ";";
  protected final String TEXT_105 = NL + "                        public ";
  protected final String TEXT_106 = " ";
  protected final String TEXT_107 = ";";
  protected final String TEXT_108 = NL + "                    this.";
  protected final String TEXT_109 = " = input.";
  protected final String TEXT_110 = ";";
  protected final String TEXT_111 = NL + "        // the two objects are the same, but multimap implementation need to see them as different." + NL + "        return -1;";
  protected final String TEXT_112 = NL + "            return -1;";
  protected final String TEXT_113 = NL + "            return 1;";
  protected final String TEXT_114 = NL + "            return 1;";
  protected final String TEXT_115 = NL + "            return -1;";
  protected final String TEXT_116 = NL + "            if(this.";
  protected final String TEXT_117 = " == null && target.";
  protected final String TEXT_118 = " != null){";
  protected final String TEXT_119 = NL + "            }else if(this.";
  protected final String TEXT_120 = " != null && target.";
  protected final String TEXT_121 = " == null){";
  protected final String TEXT_122 = NL + "            }else if(this.";
  protected final String TEXT_123 = " == null && target.";
  protected final String TEXT_124 = " == null){" + NL + "                // ignore and will use the next criteria" + NL + "            }else{";
  protected final String TEXT_125 = NL + "                if(this.";
  protected final String TEXT_126 = " != target.";
  protected final String TEXT_127 = "){" + NL + "                    if (this.";
  protected final String TEXT_128 = "){";
  protected final String TEXT_129 = NL + "                    } else {";
  protected final String TEXT_130 = NL + "                    }" + NL + "                }";
  protected final String TEXT_131 = NL + "                if(this.";
  protected final String TEXT_132 = " > target.";
  protected final String TEXT_133 = "){";
  protected final String TEXT_134 = NL + "                }else if(this.";
  protected final String TEXT_135 = " < target.";
  protected final String TEXT_136 = "){";
  protected final String TEXT_137 = NL + "                }";
  protected final String TEXT_138 = NL + "                    if(!this.";
  protected final String TEXT_139 = ".equals(target.";
  protected final String TEXT_140 = ")) {" + NL + "                        if(this.";
  protected final String TEXT_141 = ".compareTo(target.";
  protected final String TEXT_142 = ") > 0){";
  protected final String TEXT_143 = NL + "                        }else{";
  protected final String TEXT_144 = NL + "                        }" + NL + "                    }";
  protected final String TEXT_145 = NL + "                    String s1_";
  protected final String TEXT_146 = " = new String(this.";
  protected final String TEXT_147 = ");" + NL + "                    String s2_";
  protected final String TEXT_148 = " = new String(target.";
  protected final String TEXT_149 = ");" + NL + "                    if(!s1_";
  protected final String TEXT_150 = ".equals(s2_";
  protected final String TEXT_151 = ")){" + NL + "                        if(s1_";
  protected final String TEXT_152 = ".compareTo(s2_";
  protected final String TEXT_153 = ") > 0){";
  protected final String TEXT_154 = NL + "                        }else{";
  protected final String TEXT_155 = NL + "                        }" + NL + "                    }";
  protected final String TEXT_156 = NL + "                if(this.";
  protected final String TEXT_157 = " - target.";
  protected final String TEXT_158 = " != 0){" + NL + "                    if(this.";
  protected final String TEXT_159 = " - target.";
  protected final String TEXT_160 = " > 0){";
  protected final String TEXT_161 = NL + "                    }else{";
  protected final String TEXT_162 = NL + "                    }" + NL + "                }";
  protected final String TEXT_163 = NL + "                    int cmp_";
  protected final String TEXT_164 = " = FormatterUtils.format_DateInUTC(this.";
  protected final String TEXT_165 = ", ";
  protected final String TEXT_166 = ").compareTo(FormatterUtils.format_DateInUTC(target.";
  protected final String TEXT_167 = ", ";
  protected final String TEXT_168 = "));" + NL + "                    if(cmp_";
  protected final String TEXT_169 = " > 0){";
  protected final String TEXT_170 = NL + "                    }else if(cmp_";
  protected final String TEXT_171 = " < 0){";
  protected final String TEXT_172 = NL + "                    }";
  protected final String TEXT_173 = NL + "                    if(!this.";
  protected final String TEXT_174 = ".equals(target.";
  protected final String TEXT_175 = ")){" + NL + "                        if(this.";
  protected final String TEXT_176 = ".compareTo(target.";
  protected final String TEXT_177 = ") > 0){";
  protected final String TEXT_178 = NL + "                        }else{";
  protected final String TEXT_179 = NL + "                        }" + NL + "                    }";
  protected final String TEXT_180 = NL + "                    int cmp_";
  protected final String TEXT_181 = " = String.valueOf(this.";
  protected final String TEXT_182 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_183 = "));" + NL + "                    if(cmp_";
  protected final String TEXT_184 = " > 0){";
  protected final String TEXT_185 = NL + "                    }else if(cmp_";
  protected final String TEXT_186 = " < 0){";
  protected final String TEXT_187 = NL + "                    }";
  protected final String TEXT_188 = NL + "                    if(this.";
  protected final String TEXT_189 = " > target.";
  protected final String TEXT_190 = "){";
  protected final String TEXT_191 = NL + "                    }else if(this.";
  protected final String TEXT_192 = " < target.";
  protected final String TEXT_193 = "){";
  protected final String TEXT_194 = NL + "                    }";
  protected final String TEXT_195 = NL + "                    int cmp_";
  protected final String TEXT_196 = " = String.valueOf(this.";
  protected final String TEXT_197 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_198 = "));" + NL + "                    if(cmp_";
  protected final String TEXT_199 = " > 0){";
  protected final String TEXT_200 = NL + "                    }else if(cmp_";
  protected final String TEXT_201 = " < 0){";
  protected final String TEXT_202 = NL + "                    }";
  protected final String TEXT_203 = NL + "                    if(this.";
  protected final String TEXT_204 = " > target.";
  protected final String TEXT_205 = "){";
  protected final String TEXT_206 = NL + "                    }else if(this.";
  protected final String TEXT_207 = " < target.";
  protected final String TEXT_208 = "){";
  protected final String TEXT_209 = NL + "                    }";
  protected final String TEXT_210 = NL + "                    int cmp_";
  protected final String TEXT_211 = " = String.valueOf(this.";
  protected final String TEXT_212 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_213 = "));";
  protected final String TEXT_214 = NL + "                    int cmp_";
  protected final String TEXT_215 = " = this.";
  protected final String TEXT_216 = ".compareTo(target.";
  protected final String TEXT_217 = ");";
  protected final String TEXT_218 = NL + "                if(cmp_";
  protected final String TEXT_219 = " > 0){";
  protected final String TEXT_220 = NL + "                }else if(cmp_";
  protected final String TEXT_221 = " < 0){";
  protected final String TEXT_222 = NL + "                }";
  protected final String TEXT_223 = NL + "                    int cmp_";
  protected final String TEXT_224 = " = String.valueOf(this.";
  protected final String TEXT_225 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_226 = "));" + NL + "                    if(cmp_";
  protected final String TEXT_227 = " > 0){";
  protected final String TEXT_228 = NL + "                    }else if(cmp_";
  protected final String TEXT_229 = " < 0){";
  protected final String TEXT_230 = NL + "                    }";
  protected final String TEXT_231 = NL + "                    if(this.";
  protected final String TEXT_232 = " > target.";
  protected final String TEXT_233 = "){";
  protected final String TEXT_234 = NL + "                    }else if(this.";
  protected final String TEXT_235 = " < target.";
  protected final String TEXT_236 = "){";
  protected final String TEXT_237 = NL + "                    }";
  protected final String TEXT_238 = NL + "                    int cmp_";
  protected final String TEXT_239 = " = String.valueOf(this.";
  protected final String TEXT_240 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_241 = "));" + NL + "                    if(cmp_";
  protected final String TEXT_242 = " > 0){";
  protected final String TEXT_243 = NL + "                    }else if(cmp_";
  protected final String TEXT_244 = " < 0){";
  protected final String TEXT_245 = NL + "                }";
  protected final String TEXT_246 = NL + "                if(this.";
  protected final String TEXT_247 = " > target.";
  protected final String TEXT_248 = "){";
  protected final String TEXT_249 = NL + "                }else if(this.";
  protected final String TEXT_250 = " < target.";
  protected final String TEXT_251 = "){";
  protected final String TEXT_252 = NL + "                }";
  protected final String TEXT_253 = NL + "            Don't support Object type: column--";
  protected final String TEXT_254 = NL + "                int cmp_";
  protected final String TEXT_255 = " = String.valueOf(this.";
  protected final String TEXT_256 = ").compareTo(String.valueOf(target.";
  protected final String TEXT_257 = "));" + NL + "                if(cmp_";
  protected final String TEXT_258 = " > 0){";
  protected final String TEXT_259 = NL + "                }else if(cmp_";
  protected final String TEXT_260 = " < 0){";
  protected final String TEXT_261 = NL + "                }";
  protected final String TEXT_262 = NL + "                if(this.";
  protected final String TEXT_263 = " > target.";
  protected final String TEXT_264 = "){";
  protected final String TEXT_265 = NL + "                }else if(this.";
  protected final String TEXT_266 = " < target.";
  protected final String TEXT_267 = "){";
  protected final String TEXT_268 = NL + "                }";
  protected final String TEXT_269 = NL + "            int comp_";
  protected final String TEXT_270 = " = this.";
  protected final String TEXT_271 = ".compareTo(target.";
  protected final String TEXT_272 = ");" + NL + "            if(comp_";
  protected final String TEXT_273 = " != 0){" + NL + "                if(comp_";
  protected final String TEXT_274 = " > 0){";
  protected final String TEXT_275 = NL + "                }else{";
  protected final String TEXT_276 = NL + "                }" + NL + "" + NL + "            }";
  protected final String TEXT_277 = NL + "            Don't support List type: column--";
  protected final String TEXT_278 = NL + "            Don't support Document type: column--";
  protected final String TEXT_279 = NL + "            Don't support Dynamic type: column--";
  protected final String TEXT_280 = NL + "            }";
  protected final String TEXT_281 = NL + "public static class ";
  protected final String TEXT_282 = "_Comparator  implements Comparable<";
  protected final String TEXT_283 = "_Comparator>{";
  protected final String TEXT_284 = NL + "    public ";
  protected final String TEXT_285 = "_Comparator (";
  protected final String TEXT_286 = " input) {";
  protected final String TEXT_287 = NL + "    }" + NL + "" + NL + "    @Override" + NL + "    public int compareTo(";
  protected final String TEXT_288 = "_Comparator target) {";
  protected final String TEXT_289 = NL + "    }" + NL + "}";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    

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


    

/**
 * Contains common processing for tExtractDelimitedFields code generation.  This is
 * used in MapReduce and Storm components.
 *
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class TTopByUtil extends org.talend.designer.common.TransformerBase {

    /** The list of columns that should be copied directly from the input to
     *  the output schema (where they have the same column names). */
    final private Iterable<IMetadataColumn> copiedInColumns;

    /** TODO: Used in DI, tied to CHECK_NUM */
    private int columnsSize = 0;

    java.util.List<java.util.Map<String, String>> criterias = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValue(node, "__CRITERIA__");
    String topValue = ElementParameterParser.getValue(node, "__TOP__");

    java.util.List<String> listCols = new java.util.ArrayList<String>();
    java.util.Map<String, Boolean> criteriasOrderType = new java.util.HashMap<String, Boolean>();
    java.util.Map<String, Integer> criteriasSortType = new java.util.HashMap<String, Integer>();
    java.util.Map<String, Boolean> sortTypes = new java.util.HashMap<String, Boolean>();
    final Integer SORT_NUM = 0;
    final Integer SORT_ALPHA = 1;
    final Integer SORT_DATE = 2;

    public TTopByUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil) {
        super(node, argument, rowTransformUtil, "FLOW", "REJECT");

        if (null != getInConn() && null != getOutConnMain()) {
            copiedInColumns = org.talend.designer.common.TransformerBaseUtil.getColumnsUnion(getInColumns(), getOutColumnsMain());
            columnsSize = getOutConnMain().getMetadataTable().getListColumns().size();
        } else {
            copiedInColumns = null;
        }

    }

    public void generateTransformContextDeclaration() {
        // nothing
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow.
     */
    public void generateTransform() {
        generateTransform(true);
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow. The boolean parameter is
     * used to define whether the transform method return type is void or something
     * else.
     */
    public void generateTransform(boolean hasAReturnedType) {
        
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(getOutConnMainTypeName());
    stringBuffer.append(TEXT_94);
    stringBuffer.append(getOutConnMainTypeName());
    stringBuffer.append(TEXT_95);
    stringBuffer.append(getInConnName());
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(topValue);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(getOutConnMainTypeName());
    stringBuffer.append(TEXT_100);
    stringBuffer.append(getOutConnMainTypeName());
    stringBuffer.append(TEXT_101);
    
    }

    /**
     * Generates code in the transform context to create reject output.
     *
     * @param die if this reject output should kill the job.  Normally, this is
     *    tied to a dieOnError parameter for the component, but can be
     *    explicitly set to false for non-fatal reject output.
     * @param codeException a variable in the transform scope that contains the
     *    variable with an exception.  If null, this will be constructed from
     *    the codeRejectMsg.
     * @param codeRejectMsg the error message to output with the reject output.
     */
    private void generateTransformReject(boolean die, String codeException, String codeRejectMsg) {

    }




}

    
final SparkRowTransformUtil sparkTransformUtil = new SparkRowTransformUtil();
final TTopByUtil tTopByUtil = new TTopByUtil(node, codeGenArgument, sparkTransformUtil);

java.util.Map<String, java.util.List<org.talend.core.model.metadata.IMetadataColumn>> keyList = bigDataNode.getKeyList();
org.talend.designer.spark.generator.SparkFunction sparkFunction = new org.talend.designer.spark.generator.GroupByKeyFlatMapToPairFunction(true, codeGenArgument.getSparkVersion(), keyList);

sparkTransformUtil.generateSparkCode(tTopByUtil, sparkFunction);

String inRowStruct = codeGenArgument.getRecordStructName(tTopByUtil.getInConnName());

    stringBuffer.append(TEXT_102);
    
/**
 * Contains common processing for tExtractDelimitedFields code generation.  This is
 * used in MapReduce and Storm components.
 *
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class TTopUtil {

    /** The list of columns that should be copied directly from the input to
     *  the output schema (where they have the same column names). */
    private Iterable<IMetadataColumn> copiedInColumns = null;

    INode node = null;

    java.util.List<java.util.Map<String, String>> criterias = null;
    java.util.List<String> listCols = new java.util.ArrayList<String>();
    java.util.Map<String, Boolean> criteriasOrderType = new java.util.HashMap<String, Boolean>();
    java.util.Map<String, Integer> criteriasSortType = new java.util.HashMap<String, Integer>();
    java.util.Map<String, Boolean> sortTypes = new java.util.HashMap<String, Boolean>();
    final Integer SORT_NUM = 0;
    final Integer SORT_ALPHA = 1;
    final Integer SORT_DATE = 2;

    public TTopUtil(INode node) {
        this.node = node;
        criterias = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValue(node, "__CRITERIA__");

        Iterable<? extends IConnection> inConns = node.getIncomingConnections();
        copiedInColumns = null;
        if (inConns != null) {
            for (IConnection conn : inConns) {
                if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                    copiedInColumns = conn.getMetadataTable().getListColumns();
                    break;
                }
            }
        }

        for(int i = 0; i < criterias.size(); i++){
            java.util.Map<String, String> line = criterias.get(i);
            String colname = line.get("COLNAME");
            if(listCols.contains(colname)){
                continue;//skip dipulicate
            }
            listCols.add(colname);

            if(("asc").equals(line.get("ORDER"))){
                criteriasOrderType.put(colname, true);
            }else{
                criteriasOrderType.put(colname, false);
            }
            if(("num").equals(line.get("SORT"))){
                criteriasSortType.put(colname, SORT_NUM);
                sortTypes.put(colname, true);
            }else if(("alpha").equals(line.get("SORT"))){
                sortTypes.put(colname, false);
            }else{
                criteriasSortType.put(colname, SORT_DATE);
                sortTypes.put(colname, true);
            }
        }
    }

    public void generateTransformContextDeclaration() {
        for(String col:listCols) {
            for(IMetadataColumn column : copiedInColumns){
                if(col.equals(column.getLabel())) {
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                    if ("byte[]".equals(typeToGenerate)
                        && (("SPARKSTREAMING".equals(node.getComponent().getType()))
                            || ("SPARK".equals(node.getComponent().getType())))) {
                        
    stringBuffer.append(TEXT_103);
    stringBuffer.append(col);
    stringBuffer.append(TEXT_104);
    
                    } else {
                        
    stringBuffer.append(TEXT_105);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(col);
    stringBuffer.append(TEXT_107);
    
                    }
                    break;
                }
            }
        }
    }
    

    public void generateConstructor() {
        for(String col:listCols) {
            for(IMetadataColumn column : copiedInColumns){
                if(col.equals(column.getLabel())) {
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                    
    stringBuffer.append(TEXT_108);
    stringBuffer.append(col);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(col);
    stringBuffer.append(TEXT_110);
    
                    break;
                }
            }
        }
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow. The boolean parameter is
     * used to define whether the transform method return type is void or something
     * else.
     */
    public void generateTransform() {
        int index = 0;
        for(String col:listCols) {
            for(IMetadataColumn column : copiedInColumns){
                if(col.equals(column.getLabel())) {
                    compareObjectColumn(column, ++index);
                    break;
                }
            }
        }
        
    stringBuffer.append(TEXT_111);
    
    }

    private void greater(String columnName){
            genGreater(columnName);
    }
    private void lesser(String columnName){
            genLesser(columnName);
    }
    private void genGreater(String columnName){
        if(criteriasOrderType.get(columnName)){
        
    stringBuffer.append(TEXT_112);
    
        }else{
        
    stringBuffer.append(TEXT_113);
    
        }
    }
    private void genLesser(String columnName){
        if(criteriasOrderType.get(columnName)){
        
    stringBuffer.append(TEXT_114);
    
        }else{
        
    stringBuffer.append(TEXT_115);
    
        }
    }

    private void compareObjectColumn(IMetadataColumn column, int columnIterator){
        String columnNameToGenerate = column.getLabel();
        boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
        String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
        String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
        String columnName = column.getLabel();
        if(nullable){
        
    stringBuffer.append(TEXT_116);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_118);
    lesser(columnName);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_121);
    greater(columnName);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_124);
    }
    
            if(typeToGenerate.equalsIgnoreCase("Boolean")){
            
    stringBuffer.append(TEXT_125);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_128);
    greater(columnName);
    stringBuffer.append(TEXT_129);
    lesser(columnName);
    stringBuffer.append(TEXT_130);
    
            }else if(typeToGenerate.equalsIgnoreCase("Byte")){
            
    stringBuffer.append(TEXT_131);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_133);
    greater(columnName);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_136);
    lesser(columnName);
    stringBuffer.append(TEXT_137);
    
            }else if(typeToGenerate.equals("byte[]")){
                if (("SPARKSTREAMING".equals(node.getComponent().getType()))
                        || ("SPARK".equals(node.getComponent().getType()))) {
                
    stringBuffer.append(TEXT_138);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_142);
    greater(columnName);
    stringBuffer.append(TEXT_143);
    lesser(columnName);
    stringBuffer.append(TEXT_144);
    
                } else { // MR
                    
    stringBuffer.append(TEXT_145);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_153);
    greater(columnName);
    stringBuffer.append(TEXT_154);
    lesser(columnName);
    stringBuffer.append(TEXT_155);
    
                }
            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
            
    stringBuffer.append(TEXT_156);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_160);
    greater(columnName);
    stringBuffer.append(TEXT_161);
    lesser(columnName);
    stringBuffer.append(TEXT_162);
    
            }else if(typeToGenerate.equals("java.util.Date")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_163);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_169);
    greater(columnName);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_171);
    lesser(columnName);
    stringBuffer.append(TEXT_172);
    
                }else{
                
    stringBuffer.append(TEXT_173);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_177);
    greater(columnName);
    stringBuffer.append(TEXT_178);
    lesser(columnName);
    stringBuffer.append(TEXT_179);
    
                }
            }else if(typeToGenerate.equalsIgnoreCase("Double")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_180);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_184);
    greater(columnName);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_186);
    lesser(columnName);
    stringBuffer.append(TEXT_187);
    
                }else{
                
    stringBuffer.append(TEXT_188);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_190);
    greater(columnName);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_193);
    lesser(columnName);
    stringBuffer.append(TEXT_194);
    
                }
            }else if(typeToGenerate.equalsIgnoreCase("Float")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_195);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_199);
    greater(columnName);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_201);
    lesser(columnName);
    stringBuffer.append(TEXT_202);
    
                }else{
                
    stringBuffer.append(TEXT_203);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_205);
    greater(columnName);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_208);
    lesser(columnName);
    stringBuffer.append(TEXT_209);
    
                }
            }else if(typeToGenerate.equals("BigDecimal")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_210);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_213);
    
                }else{
                
    stringBuffer.append(TEXT_214);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_217);
    }
    stringBuffer.append(TEXT_218);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_219);
    greater(columnName);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_221);
    lesser(columnName);
    stringBuffer.append(TEXT_222);
    
            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_223);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_227);
    greater(columnName);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_229);
    lesser(columnName);
    stringBuffer.append(TEXT_230);
    
                }else{
                
    stringBuffer.append(TEXT_231);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_232);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_233);
    greater(columnName);
    stringBuffer.append(TEXT_234);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_236);
    lesser(columnName);
    stringBuffer.append(TEXT_237);
    
                }
            }else if(typeToGenerate.equalsIgnoreCase("Long")){
            
    
                if(!sortTypes.get(columnName)){
                
    stringBuffer.append(TEXT_238);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_242);
    greater(columnName);
    stringBuffer.append(TEXT_243);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_244);
    lesser(columnName);
    stringBuffer.append(TEXT_245);
    
            }else{
            
    stringBuffer.append(TEXT_246);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_248);
    greater(columnName);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_251);
    lesser(columnName);
    stringBuffer.append(TEXT_252);
    
            }
        }else if(typeToGenerate.equals("Object")){
        
    stringBuffer.append(TEXT_253);
    stringBuffer.append(columnNameToGenerate);
    
        }else if(typeToGenerate.equalsIgnoreCase("Short")){
        
    
            if(!sortTypes.get(columnName)){
            
    stringBuffer.append(TEXT_254);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_258);
    greater(columnName);
    stringBuffer.append(TEXT_259);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_260);
    lesser(columnName);
    stringBuffer.append(TEXT_261);
    
            }else{
            
    stringBuffer.append(TEXT_262);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_264);
    greater(columnName);
    stringBuffer.append(TEXT_265);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_267);
    lesser(columnName);
    stringBuffer.append(TEXT_268);
    
            }
        }else if(typeToGenerate.equals("String")){
        
    stringBuffer.append(TEXT_269);
    stringBuffer.append(columnIterator);
    stringBuffer.append(TEXT_270);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_271);
    stringBuffer.append(columnNameToGenerate);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(columnIterator);
    stringBuffer.append(TEXT_273);
    stringBuffer.append(columnIterator);
    stringBuffer.append(TEXT_274);
    greater(columnName);
    stringBuffer.append(TEXT_275);
    lesser(columnName);
    stringBuffer.append(TEXT_276);
    
        }else if(typeToGenerate.equals("List")){
        
    stringBuffer.append(TEXT_277);
    stringBuffer.append(columnNameToGenerate);
    
        }else if(typeToGenerate.equals("Doucument")){
        
    stringBuffer.append(TEXT_278);
    stringBuffer.append(columnNameToGenerate);
    
        }else if(typeToGenerate.equals("Dynamic")){
        
    stringBuffer.append(TEXT_279);
    stringBuffer.append(columnNameToGenerate);
    
        }
        
    if(nullable){
    stringBuffer.append(TEXT_280);
    
        }
    }


}

    stringBuffer.append(TEXT_281);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_283);
    
    TTopUtil tTopUtil = new TTopUtil(node);
    tTopUtil.generateTransformContextDeclaration();
    
    stringBuffer.append(TEXT_284);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(inRowStruct);
    stringBuffer.append(TEXT_286);
    
        tTopUtil.generateConstructor();
        
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_288);
    
        tTopUtil.generateTransform();
        
    stringBuffer.append(TEXT_289);
    return stringBuffer.toString();
  }
}
