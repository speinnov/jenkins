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

public class TSortRowSparkcodeJava
{
  protected static String nl;
  public static synchronized TSortRowSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSortRowSparkcodeJava result = new TSortRowSparkcodeJava();
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
  protected final String TEXT_92 = NL + "            return 1;";
  protected final String TEXT_93 = NL + "            return -1;";
  protected final String TEXT_94 = NL + "            return -1;";
  protected final String TEXT_95 = NL + "            return 1;";
  protected final String TEXT_96 = NL + "            if (";
  protected final String TEXT_97 = " == null && ";
  protected final String TEXT_98 = " != null) {";
  protected final String TEXT_99 = NL + "            } else if (";
  protected final String TEXT_100 = " != null && ";
  protected final String TEXT_101 = " == null) {";
  protected final String TEXT_102 = NL + "            } else if (";
  protected final String TEXT_103 = " == null && ";
  protected final String TEXT_104 = " == null) {" + NL + "                //ignore" + NL + "            } else {";
  protected final String TEXT_105 = NL + "            if (";
  protected final String TEXT_106 = " != ";
  protected final String TEXT_107 = ") {" + NL + "                if (";
  protected final String TEXT_108 = ") {";
  protected final String TEXT_109 = NL + "                } else {";
  protected final String TEXT_110 = NL + "                }" + NL + "            }";
  protected final String TEXT_111 = NL + "            if (";
  protected final String TEXT_112 = " > ";
  protected final String TEXT_113 = ") {";
  protected final String TEXT_114 = NL + "            } else if (";
  protected final String TEXT_115 = " < ";
  protected final String TEXT_116 = ") {";
  protected final String TEXT_117 = NL + "            }";
  protected final String TEXT_118 = NL + "            int comp_";
  protected final String TEXT_119 = " = ";
  protected final String TEXT_120 = ".compareTo(";
  protected final String TEXT_121 = ");" + NL + "            if (comp_";
  protected final String TEXT_122 = " != 0) {" + NL + "                if (comp_";
  protected final String TEXT_123 = " > 0) {";
  protected final String TEXT_124 = NL + "                } else {";
  protected final String TEXT_125 = NL + "                }" + NL + "            }";
  protected final String TEXT_126 = NL + "            if (";
  protected final String TEXT_127 = " - ";
  protected final String TEXT_128 = " != 0) {" + NL + "                if (";
  protected final String TEXT_129 = " - ";
  protected final String TEXT_130 = " > 0) {";
  protected final String TEXT_131 = NL + "                } else {";
  protected final String TEXT_132 = NL + "                }" + NL + "            }";
  protected final String TEXT_133 = NL + "                int cmp_";
  protected final String TEXT_134 = " = FormatterUtils.format_DateInUTC(";
  protected final String TEXT_135 = ", ";
  protected final String TEXT_136 = ").compareTo(FormatterUtils.format_DateInUTC(";
  protected final String TEXT_137 = ", ";
  protected final String TEXT_138 = "));" + NL + "                if (cmp_";
  protected final String TEXT_139 = " > 0) {";
  protected final String TEXT_140 = NL + "                } else if (cmp_";
  protected final String TEXT_141 = " < 0) {";
  protected final String TEXT_142 = NL + "                }";
  protected final String TEXT_143 = NL + "                if (!";
  protected final String TEXT_144 = ".equals(";
  protected final String TEXT_145 = ")) {" + NL + "                    if (";
  protected final String TEXT_146 = ".compareTo(";
  protected final String TEXT_147 = ") > 0) {";
  protected final String TEXT_148 = NL + "                    } else {";
  protected final String TEXT_149 = NL + "                    }" + NL + "                }";
  protected final String TEXT_150 = NL + "                int cmp_";
  protected final String TEXT_151 = " = String.valueOf(";
  protected final String TEXT_152 = ").compareTo(String.valueOf(";
  protected final String TEXT_153 = "));" + NL + "                if (cmp_";
  protected final String TEXT_154 = " > 0) {";
  protected final String TEXT_155 = NL + "                } else if (cmp_";
  protected final String TEXT_156 = " < 0) {";
  protected final String TEXT_157 = NL + "                }";
  protected final String TEXT_158 = NL + "                if (";
  protected final String TEXT_159 = " > ";
  protected final String TEXT_160 = ") {";
  protected final String TEXT_161 = NL + "                } else if (";
  protected final String TEXT_162 = " < ";
  protected final String TEXT_163 = ") {";
  protected final String TEXT_164 = NL + "                }";
  protected final String TEXT_165 = NL + "                int cmp_";
  protected final String TEXT_166 = " = String.valueOf(";
  protected final String TEXT_167 = ").compareTo(String.valueOf(";
  protected final String TEXT_168 = "));" + NL + "                if (cmp_";
  protected final String TEXT_169 = " > 0) {";
  protected final String TEXT_170 = NL + "                } else if (cmp_";
  protected final String TEXT_171 = " < 0) {";
  protected final String TEXT_172 = NL + "                }";
  protected final String TEXT_173 = NL + "                if (";
  protected final String TEXT_174 = " > ";
  protected final String TEXT_175 = ") {";
  protected final String TEXT_176 = NL + "                } else if (";
  protected final String TEXT_177 = " < ";
  protected final String TEXT_178 = ") {";
  protected final String TEXT_179 = NL + "                }";
  protected final String TEXT_180 = NL + "                int cmp_";
  protected final String TEXT_181 = " = String.valueOf(";
  protected final String TEXT_182 = ").compareTo(String.valueOf(";
  protected final String TEXT_183 = "));";
  protected final String TEXT_184 = NL + "                int cmp_";
  protected final String TEXT_185 = " = ";
  protected final String TEXT_186 = ".compareTo(";
  protected final String TEXT_187 = ");";
  protected final String TEXT_188 = NL + "                if (cmp_";
  protected final String TEXT_189 = " > 0) {";
  protected final String TEXT_190 = NL + "                } else if (cmp_";
  protected final String TEXT_191 = " < 0) {";
  protected final String TEXT_192 = NL + "                }";
  protected final String TEXT_193 = NL + "                int cmp_";
  protected final String TEXT_194 = " = String.valueOf(";
  protected final String TEXT_195 = ").compareTo(String.valueOf(";
  protected final String TEXT_196 = "));" + NL + "                if (cmp_";
  protected final String TEXT_197 = " > 0) {";
  protected final String TEXT_198 = NL + "                } else if (cmp_";
  protected final String TEXT_199 = " < 0) {";
  protected final String TEXT_200 = NL + "                }";
  protected final String TEXT_201 = NL + "                if (";
  protected final String TEXT_202 = " > ";
  protected final String TEXT_203 = ") {";
  protected final String TEXT_204 = NL + "                } else if (";
  protected final String TEXT_205 = " < ";
  protected final String TEXT_206 = ") {";
  protected final String TEXT_207 = NL + "                }";
  protected final String TEXT_208 = NL + "                int cmp_";
  protected final String TEXT_209 = " = String.valueOf(";
  protected final String TEXT_210 = ").compareTo(String.valueOf(";
  protected final String TEXT_211 = "));" + NL + "                if (cmp_";
  protected final String TEXT_212 = " > 0) {";
  protected final String TEXT_213 = NL + "                } else if (cmp_";
  protected final String TEXT_214 = " < 0) {";
  protected final String TEXT_215 = NL + "                }";
  protected final String TEXT_216 = NL + "                if (";
  protected final String TEXT_217 = " > ";
  protected final String TEXT_218 = ") {";
  protected final String TEXT_219 = NL + "                } else if (";
  protected final String TEXT_220 = " < ";
  protected final String TEXT_221 = ") {";
  protected final String TEXT_222 = NL + "                }";
  protected final String TEXT_223 = NL + "                int cmp_";
  protected final String TEXT_224 = " = String.valueOf(";
  protected final String TEXT_225 = ").compareTo(String.valueOf(";
  protected final String TEXT_226 = "));" + NL + "                if (cmp_";
  protected final String TEXT_227 = " > 0) {";
  protected final String TEXT_228 = NL + "                } else if (cmp_";
  protected final String TEXT_229 = " < 0) {";
  protected final String TEXT_230 = NL + "                }";
  protected final String TEXT_231 = NL + "                if (";
  protected final String TEXT_232 = " > ";
  protected final String TEXT_233 = ") {";
  protected final String TEXT_234 = NL + "                } else if (";
  protected final String TEXT_235 = " < ";
  protected final String TEXT_236 = ") {";
  protected final String TEXT_237 = NL + "                }";
  protected final String TEXT_238 = NL + "            int comp_";
  protected final String TEXT_239 = " = ";
  protected final String TEXT_240 = ".compareTo(";
  protected final String TEXT_241 = ");" + NL + "            if (comp_";
  protected final String TEXT_242 = " != 0) {" + NL + "                if (comp_";
  protected final String TEXT_243 = " > 0) {";
  protected final String TEXT_244 = NL + "                } else {";
  protected final String TEXT_245 = NL + "                }" + NL + "" + NL + "            }";
  protected final String TEXT_246 = NL + "            throw new JobConfigurationError(\"The ";
  protected final String TEXT_247 = " type is not supported: column--";
  protected final String TEXT_248 = "\");";
  protected final String TEXT_249 = NL + "            }";

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
 * Contains common processing for tSortRow code generation.
 */
class TSortRowUtil extends org.talend.designer.common.TransformerBase {

    /** The list of columns that should be copied directly from the input to
     *  the output schema (where they have the same column names). */
    final private Iterable<IMetadataColumn> copiedInColumns;

    /** When generating a comparator, the code accessor for the left side. */
    private final String codeVarData1;

    /** When generating a comparator, the code accessor for the right side. */
    private final String codeVarData2;

    java.util.List<java.util.Map<String, String>> criterias = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValue(node, "__CRITERIA__");
    java.util.List<String> listCols = new java.util.ArrayList<String>();

    /** Contains ONLY those columns that are ascending. */
    java.util.Set<String> criteriasAscendingColumns = new java.util.HashSet<String>();

    java.util.Map<String, Integer> criteriasSortType = new java.util.HashMap<String, Integer>();
    java.util.Map<String, Boolean> sortTypes = new java.util.HashMap<String, Boolean>();

    final java.util.Map<String, java.util.List<org.talend.core.model.metadata.IMetadataColumn>> keyList;

    final Integer SORT_NUM = 0;
    final Integer SORT_ALPHA = 1;
    final Integer SORT_DATE = 2;

    public TSortRowUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil) {
        this(node, argument, rowTransformUtil, "data1", "data2", false);
    }

    /**
     * @param invert if true, changes all ascending columns to descending
     *        columns and vice versa.
     */
    protected TSortRowUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil,
            String codeVarData1, String codeVarData2, boolean invert) {
        super(node, argument, rowTransformUtil, "FLOW", "REJECT");

        keyList = ((IBigDataNode) node).getKeyList();
        this.codeVarData1 = codeVarData1;
        this.codeVarData2 = codeVarData2;

        if (null != getInConn() && null != getOutConnMain()) {
            copiedInColumns = org.talend.designer.common.TransformerBaseUtil.getColumnsUnion(getInColumns(), getOutColumnsMain());
        } else {
            copiedInColumns = null;
        }

        for(int i = 0; i < criterias.size(); i++) {
            java.util.Map<String, String> line = criterias.get(i);
            String colname = line.get("COLNAME");
            if (listCols.contains(colname)) {
                continue;//skip dipulicate
            }
            listCols.add(colname);

            if ("asc".equals(line.get("ORDER")) == !invert) {
                criteriasAscendingColumns.add(colname);
            }

            if (("num").equals(line.get("SORT"))) {
                criteriasSortType.put(colname, SORT_NUM);
                sortTypes.put(colname, true);
            } else if (("alpha").equals(line.get("SORT"))) {
                sortTypes.put(colname, false);
            } else {
                criteriasSortType.put(colname, SORT_DATE);
                sortTypes.put(colname, true);
            }
        }
    }

    public void generateTransformContextDeclaration() {
        // Nothing here
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
        int index = 0;
        for(String col:listCols) {
            for(IMetadataColumn column : copiedInColumns) {
                if (col.equals(column.getLabel())) {
                    generateComparatorSnippetForColumn(column, index,
                        org.talend.designer.spark.generator.utils.SparkFunctionUtil.getKeyValueAccessor(
                            keyList, "BOTH", codeVarData1, index),
                        org.talend.designer.spark.generator.utils.SparkFunctionUtil.getKeyValueAccessor(
                            keyList, "BOTH", codeVarData2, index));
                    index++;
                    break;
                }
            }
        }
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

    protected void greater(String columnName) {
        if (criteriasAscendingColumns.contains(columnName)) {
            
    stringBuffer.append(TEXT_92);
    
        } else {
            
    stringBuffer.append(TEXT_93);
    
        }
    }

    protected void lesser(String columnName) {
        if (criteriasAscendingColumns.contains(columnName)) {
            
    stringBuffer.append(TEXT_94);
    
        } else {
            
    stringBuffer.append(TEXT_95);
    
        }
    }

    private void generateComparatorSnippetForColumn(IMetadataColumn column,
                int i, String codeData1, String codeData2) {
        boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
        String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
        String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
        String columnName = column.getLabel();

        // For nullable columns, we always generate a comparison that sorts
        // nulls before non-null.
        if (nullable) {
            
    stringBuffer.append(TEXT_96);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_98);
    lesser(columnName);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_101);
    greater(columnName);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_104);
    
            // Note that the nullable else case is left open here and closed
            // at the end.
        }

        if (typeToGenerate.equalsIgnoreCase("Boolean")) {
            
    stringBuffer.append(TEXT_105);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_108);
    greater(columnName);
    stringBuffer.append(TEXT_109);
    lesser(columnName);
    stringBuffer.append(TEXT_110);
    
        } else if (typeToGenerate.equalsIgnoreCase("Byte")) {
            
    stringBuffer.append(TEXT_111);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_113);
    greater(columnName);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_116);
    lesser(columnName);
    stringBuffer.append(TEXT_117);
    
        } else if (typeToGenerate.equals("byte[]")) {
            
    stringBuffer.append(TEXT_118);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_123);
    greater(columnName);
    stringBuffer.append(TEXT_124);
    lesser(columnName);
    stringBuffer.append(TEXT_125);
    
        } else if (typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")) {
            
    stringBuffer.append(TEXT_126);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_130);
    greater(columnName);
    stringBuffer.append(TEXT_131);
    lesser(columnName);
    stringBuffer.append(TEXT_132);
    
        } else if (typeToGenerate.equals("java.util.Date")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_133);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_139);
    greater(columnName);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_141);
    lesser(columnName);
    stringBuffer.append(TEXT_142);
    
            } else {
                
    stringBuffer.append(TEXT_143);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_147);
    greater(columnName);
    stringBuffer.append(TEXT_148);
    lesser(columnName);
    stringBuffer.append(TEXT_149);
    
            }
        } else if (typeToGenerate.equalsIgnoreCase("Double")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_150);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_154);
    greater(columnName);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_156);
    lesser(columnName);
    stringBuffer.append(TEXT_157);
    
            } else {
                
    stringBuffer.append(TEXT_158);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_160);
    greater(columnName);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_163);
    lesser(columnName);
    stringBuffer.append(TEXT_164);
    
            }
        } else if (typeToGenerate.equalsIgnoreCase("Float")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_165);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_169);
    greater(columnName);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_171);
    lesser(columnName);
    stringBuffer.append(TEXT_172);
    
            } else {
                
    stringBuffer.append(TEXT_173);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_175);
    greater(columnName);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_178);
    lesser(columnName);
    stringBuffer.append(TEXT_179);
    
            }
        } else if (typeToGenerate.equals("BigDecimal")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_180);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_183);
    
            } else {
                
    stringBuffer.append(TEXT_184);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_187);
    }
    stringBuffer.append(TEXT_188);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_189);
    greater(columnName);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_191);
    lesser(columnName);
    stringBuffer.append(TEXT_192);
    
        } else if (typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_193);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_197);
    greater(columnName);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_199);
    lesser(columnName);
    stringBuffer.append(TEXT_200);
    
            } else {
                
    stringBuffer.append(TEXT_201);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_203);
    greater(columnName);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_206);
    lesser(columnName);
    stringBuffer.append(TEXT_207);
    
            }
        } else if (typeToGenerate.equalsIgnoreCase("Long")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_208);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_212);
    greater(columnName);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_214);
    lesser(columnName);
    stringBuffer.append(TEXT_215);
    
            } else {
                
    stringBuffer.append(TEXT_216);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_218);
    greater(columnName);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_221);
    lesser(columnName);
    stringBuffer.append(TEXT_222);
    
            }
        } else if (typeToGenerate.equalsIgnoreCase("Short")) {
            if (!sortTypes.get(columnName)) {
                
    stringBuffer.append(TEXT_223);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_227);
    greater(columnName);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_229);
    lesser(columnName);
    stringBuffer.append(TEXT_230);
    
            } else {
                
    stringBuffer.append(TEXT_231);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_232);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_233);
    greater(columnName);
    stringBuffer.append(TEXT_234);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_236);
    lesser(columnName);
    stringBuffer.append(TEXT_237);
    
            }
        } else if (typeToGenerate.equals("String")) {
            
    stringBuffer.append(TEXT_238);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(codeData1);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(codeData2);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_243);
    greater(columnName);
    stringBuffer.append(TEXT_244);
    lesser(columnName);
    stringBuffer.append(TEXT_245);
    
        } else if (typeToGenerate.equals("Object")
                || typeToGenerate.equals("List")
                || typeToGenerate.equals("Document")
                || typeToGenerate.equals("Dynamic")) {
            
    stringBuffer.append(TEXT_246);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_248);
    
        }

        // Close the open else statement for nullable columns.
        if (nullable) {
            
    stringBuffer.append(TEXT_249);
    
        }
    }
}

    
final SparkRowTransformUtil sparkTransformUtil = new SparkRowTransformUtil();
final TSortRowUtil tSortRowUtil = new TSortRowUtil(
        node, codeGenArgument, sparkTransformUtil);

java.util.Map<String, java.util.List<org.talend.core.model.metadata.IMetadataColumn>> keyList = bigDataNode.getKeyList();
org.talend.designer.spark.generator.SparkFunction sparkFunction = new org.talend.designer.spark.generator.SortByKeyFunction(false, keyList);

sparkTransformUtil.generateSparkCode(tSortRowUtil, sparkFunction);

    return stringBuffer.toString();
  }
}
