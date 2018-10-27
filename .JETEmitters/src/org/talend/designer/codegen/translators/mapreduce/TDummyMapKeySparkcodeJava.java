package org.talend.designer.codegen.translators.mapreduce;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IBigDataNode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.core.model.utils.NodeUtil;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TDummyMapKeySparkcodeJava
{
  protected static String nl;
  public static synchronized TDummyMapKeySparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TDummyMapKeySparkcodeJava result = new TDummyMapKeySparkcodeJava();
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
  protected final String TEXT_92 = NL + "        ";
  protected final String TEXT_93 = " = ";
  protected final String TEXT_94 = ";";
  protected final String TEXT_95 = NL + "        ";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
// Parse the inputs to this javajet generator.
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
 * Contains common processing for tDummyMap code generation.  This is
 * used in MapReduce and Storm components.
 *
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class TDummyMapKeyUtil extends org.talend.designer.common.TransformerBase {

    /** The list of columns that should be copied directly from the input to
     *  the output schema (where they have the same column names). */
    final private Iterable<IMetadataColumn> copiedInColumns;

    /** TODO: Used in DI, tied to CHECK_NUM */
    private int columnsSize = 0;

    public TDummyMapKeyUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil) {
        super(node, argument, rowTransformUtil, "FLOW", "REJECT");

        if (null != getInConn() && null != getOutConnMain()) {
            copiedInColumns = org.talend.designer.common.TransformerBaseUtil.getColumnsUnion(getInColumns(), getOutColumnsMain());
            columnsSize = getOutConnMain().getMetadataTable().getListColumns().size();
        } else if(null != getInConn() && null != getOutConnReject()) {
            /**
            * There is no reject in a tDummyMap. But if a tDummyMap is generated in the middle of a Reject row, then
            * the new output row will be a "reject" as weel. Then we will consider a REJECT as a FLOW MAIN for this component.
            */
            copiedInColumns = org.talend.designer.common.TransformerBaseUtil.getColumnsUnion(getInColumns(), getOutColumnsReject());
            columnsSize = getOutConnReject().getMetadataTable().getListColumns().size();
        } else {
            copiedInColumns = null;
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
        generateTransform(false);
    }

    /**
     * Generates code that performs the tranformation from one input string
     * to many output strings, or to a reject flow. The boolean parameter is
     * used to define whether the transform method return type is void or something
     * else.
     */
    public void generateTransform(boolean hasAReturnedType) {
    
    stringBuffer.append(TEXT_92);
    stringBuffer.append(getOutConnMainName());
    stringBuffer.append(TEXT_93);
    stringBuffer.append(getRowTransform().getInValue());
    stringBuffer.append(TEXT_94);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(getRowTransform().getCodeToEmit(false));
    
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
final TDummyMapKeyUtil tDummyMapUtil = new TDummyMapKeyUtil(
        node, codeGenArgument, sparkTransformUtil);

org.talend.designer.spark.generator.SparkFunction sparkFunction = null;
String requiredInputType = bigDataNode.getRequiredInputType();
String requiredOutputType = bigDataNode.getRequiredOutputType();
String incomingType = bigDataNode.getIncomingType();
String outgoingType = bigDataNode.getOutgoingType();
boolean inputIsPair = requiredInputType != null ? "KEYVALUE".equals(requiredInputType) : "KEYVALUE".equals(incomingType);
java.util.Map<String, java.util.List<org.talend.core.model.metadata.IMetadataColumn>> keyList = bigDataNode.getKeyList();

String type = requiredOutputType == null ? outgoingType : requiredOutputType;
if("KEYVALUE".equals(type)) {
    sparkFunction = new org.talend.designer.spark.generator.MapToPairFunction(inputIsPair, false, keyList);
} else if("VALUE".equals(type)) {
    sparkFunction = new org.talend.designer.spark.generator.MapFunction(inputIsPair, keyList);
}

sparkTransformUtil.generateSparkCode(tDummyMapUtil, sparkFunction);

    return stringBuffer.toString();
  }
}
