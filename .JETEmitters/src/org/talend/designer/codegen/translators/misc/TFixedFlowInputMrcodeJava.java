package org.talend.designer.codegen.translators.misc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TFixedFlowInputMrcodeJava
{
  protected static String nl;
  public static synchronized TFixedFlowInputMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TFixedFlowInputMrcodeJava result = new TFixedFlowInputMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = "\t";
  protected final String TEXT_3 = NL + "\t\t\toutput_";
  protected final String TEXT_4 = ".collect(";
  protected final String TEXT_5 = ", ";
  protected final String TEXT_6 = ");" + NL + "\t\t";
  protected final String TEXT_7 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_8 = " extends MapReduceBase" + NL + "\t\t\t\timplements Mapper<";
  protected final String TEXT_9 = ", ";
  protected final String TEXT_10 = ", ";
  protected final String TEXT_11 = ", ";
  protected final String TEXT_12 = ">{" + NL + "" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\t";
  protected final String TEXT_13 = " ";
  protected final String TEXT_14 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\t\t";
  protected final String TEXT_16 = " ";
  protected final String TEXT_17 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\t\t";
  protected final String TEXT_19 = NL + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_20 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_21 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_22 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_24 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_25 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_26 = " = new ";
  protected final String TEXT_27 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_28 = NL + "\t\t\t\t\t";
  protected final String TEXT_29 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_30 = " = new ";
  protected final String TEXT_31 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_32 = NL + "\t\t\t\t\t";
  protected final String TEXT_33 = NL + "  \t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void map(";
  protected final String TEXT_34 = " key_";
  protected final String TEXT_35 = ", ";
  protected final String TEXT_36 = " value_";
  protected final String TEXT_37 = "," + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_38 = ", ";
  protected final String TEXT_39 = "> output_";
  protected final String TEXT_40 = ", Reporter reporter_";
  protected final String TEXT_41 = ") throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_42 = NL + "\t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_43 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_44 = NL + "\t\t\t\tChainReducer.addMapper(job, ";
  protected final String TEXT_45 = ".class, ";
  protected final String TEXT_46 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_47 = ".class, ";
  protected final String TEXT_48 = ".class, ";
  protected final String TEXT_49 = ".class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_50 = NL + "\t\t\t\tchainMapper.addMapper(job, ";
  protected final String TEXT_51 = ".class, ";
  protected final String TEXT_52 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_53 = ".class, ";
  protected final String TEXT_54 = ".class, ";
  protected final String TEXT_55 = ".class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_56 = NL + "\t\t\t\tchainMapper.setCid(\"";
  protected final String TEXT_57 = "\");" + NL + "\t\t\t";
  protected final String TEXT_58 = NL + "\t\t\t\tmos_";
  protected final String TEXT_59 = ".getCollector(\"";
  protected final String TEXT_60 = "\", reporter_";
  protected final String TEXT_61 = ").collect(";
  protected final String TEXT_62 = ", ";
  protected final String TEXT_63 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_64 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_65 = " extends MapReduceBase" + NL + "\t\t\t\timplements Mapper<";
  protected final String TEXT_66 = ", ";
  protected final String TEXT_67 = ", ";
  protected final String TEXT_68 = ", WritableComparable>{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\tpublic MultipleOutputs mos_";
  protected final String TEXT_69 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_70 = " ";
  protected final String TEXT_71 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_72 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_73 = " ";
  protected final String TEXT_74 = " = null;" + NL + "\t\t\t\t\t";
  protected final String TEXT_75 = NL + "\t\t\t\t";
  protected final String TEXT_76 = NL + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_77 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_78 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_79 = ");" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_80 = " = new MultipleOutputs(job_";
  protected final String TEXT_81 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_82 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_83 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_84 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_85 = " = new ";
  protected final String TEXT_86 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_87 = NL + "\t\t\t\t\t";
  protected final String TEXT_88 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_89 = " = new ";
  protected final String TEXT_90 = "();" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_91 = NL + "\t\t\t\t\t";
  protected final String TEXT_92 = NL + "  \t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void map(";
  protected final String TEXT_93 = " key_";
  protected final String TEXT_94 = ", ";
  protected final String TEXT_95 = " value_";
  protected final String TEXT_96 = "," + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_97 = ", WritableComparable> output_";
  protected final String TEXT_98 = ", Reporter reporter_";
  protected final String TEXT_99 = ") throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_100 = NL + "\t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_101 = ".close();" + NL + "\t\t\t\t\t";
  protected final String TEXT_102 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_103 = NL + "\t\t\t\tChainReducer.addMapper(job, ";
  protected final String TEXT_104 = ".class, ";
  protected final String TEXT_105 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_106 = ".class, ";
  protected final String TEXT_107 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_108 = NL + "\t\t\t\tchainMapper.addMapper(job, ";
  protected final String TEXT_109 = ".class, ";
  protected final String TEXT_110 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_111 = ".class, ";
  protected final String TEXT_112 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_113 = NL + "        \tMultipleOutputs.setWorkDir(job, genTempFolderForComponent(\"MultipleOutputs_";
  protected final String TEXT_114 = "\"));" + NL + "        \t";
  protected final String TEXT_115 = NL + "\t\t\t\tMultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_116 = "\", ";
  protected final String TEXT_117 = ".class, ";
  protected final String TEXT_118 = ".class);" + NL + "        \t";
  protected final String TEXT_119 = NL + "            // No mrcode generated for unnecessary ";
  protected final String TEXT_120 = NL + "                ";
  protected final String TEXT_121 = " ";
  protected final String TEXT_122 = " = value_";
  protected final String TEXT_123 = ";";
  protected final String TEXT_124 = NL + "            // No mrconfig generated for unnecessary ";
  protected final String TEXT_125 = NL + "        chainMapper.addMapper(job, ";
  protected final String TEXT_126 = "," + NL + "                NullWritable.class, ";
  protected final String TEXT_127 = "Struct.class," + NL + "                NullWritable.class, ";
  protected final String TEXT_128 = "Struct.class," + NL + "                            true, new JobConf(false));" + NL;
  protected final String TEXT_129 = NL + "            MultipleOutputs.setWorkDir(job," + NL + "                    genTempFolderForComponent(\"";
  protected final String TEXT_130 = "\"));" + NL + "            MultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_131 = "\"," + NL + "                    NullWritable.class, ";
  protected final String TEXT_132 = "Struct.class);";
  protected final String TEXT_133 = NL;
  protected final String TEXT_134 = NL + "        // Nothing to do";
  protected final String TEXT_135 = NL + "            ";
  protected final String TEXT_136 = " = init(new Object[][] { new Object[] {";
  protected final String TEXT_137 = "}});";
  protected final String TEXT_138 = NL + "            // replace \\ by \\\\\\\\";
  protected final String TEXT_139 = NL + "            ";
  protected final String TEXT_140 = " = init(";
  protected final String TEXT_141 = NL + "                        .split(";
  protected final String TEXT_142 = "));";
  protected final String TEXT_143 = NL + "            ";
  protected final String TEXT_144 = " = init(";
  protected final String TEXT_145 = ");";
  protected final String TEXT_146 = NL + "            private java.util.List<java.util.List<Object>> init(Object[][] fixedValuesAsArray) {" + NL + "                List<List<Object>> fixedValues = new java.util.ArrayList<java.util.List<Object>>();" + NL + "                for (Object[] tuple : fixedValuesAsArray)" + NL + "                    fixedValues.add(java.util.Arrays.asList(tuple));" + NL + "                return fixedValues;" + NL + "            }";
  protected final String TEXT_147 = NL + "            private java.util.List<java.util.List<Object>> init(String[] fixedValuesAsArray) {" + NL + "                List<List<Object>> fixedValues = new java.util.ArrayList<java.util.List<Object>>();" + NL + "                for (String tuple : fixedValuesAsArray)" + NL + "                    fixedValues.add(java.util.Arrays.asList((Object[])tuple.split(";
  protected final String TEXT_148 = ")));" + NL + "                return fixedValues;" + NL + "            }";
  protected final String TEXT_149 = NL + "            private java.util.List<java.util.List<Object>> init(String... base64Rows) {" + NL + "                java.util.List<java.util.List<Object>> fixedValues = new java.util.ArrayList<java.util.List<Object>>();" + NL + "                StringBuilder base64 = new StringBuilder();" + NL + "                for (String row : base64Rows) {" + NL + "                    base64.append(row);";
  protected final String TEXT_150 = NL + "                        base64.append(";
  protected final String TEXT_151 = ");";
  protected final String TEXT_152 = NL + "                }" + NL + "" + NL + "                String originalFileContent = \"\";" + NL + "                String javajetLineSeparator_";
  protected final String TEXT_153 = "  = ";
  protected final String TEXT_154 = ";" + NL + "                try {" + NL + "                    if (checkForBase64Encode_";
  protected final String TEXT_155 = "(base64.toString())) {" + NL + "                        originalFileContent = new String((new sun.misc.BASE64Decoder()).decodeBuffer(base64.toString()), utf8Charset);" + NL + "                    } else {" + NL + "                        originalFileContent = base64.toString();" + NL + "                    }" + NL + "                    org.talend.fileprocess.FileInputDelimited fid = new org.talend.fileprocess.FileInputDelimited(" + NL + "                            new java.io.ByteArrayInputStream(originalFileContent.getBytes(utf8Charset))," + NL + "                            utf8Charset,";
  protected final String TEXT_156 = NL + "                            ";
  protected final String TEXT_157 = "," + NL + "                            javajetLineSeparator_";
  protected final String TEXT_158 = "," + NL + "                            true, 0, 0, -1, -1, false);" + NL + "" + NL + "                    while (fid.nextRecord()) {" + NL + "                        java.util.List<Object> tuple = new java.util.ArrayList<Object>();" + NL;
  protected final String TEXT_159 = NL + NL + "                            if (";
  protected final String TEXT_160 = " < fid.getColumnsCountOfCurrentRow()) {" + NL + "                                String colContent = fid.get(";
  protected final String TEXT_161 = ");";
  protected final String TEXT_162 = NL + "                                    tuple.add(colContent == null || colContent.length() == 0" + NL + "                                            ? ";
  protected final String TEXT_163 = ": colContent);";
  protected final String TEXT_164 = NL + "                                    if ( colContent == null || colContent.length() == 0) {" + NL + "                                        tuple.add(BigDataParserUtils.parseTo_Date(";
  protected final String TEXT_165 = NL + "                                                (String)";
  protected final String TEXT_166 = " ";
  protected final String TEXT_167 = ",";
  protected final String TEXT_168 = NL + "                                            ";
  protected final String TEXT_169 = "));" + NL + "                                    } else {" + NL + "                                        tuple.add(BigDataParserUtils.parseTo_Date(colContent, ";
  protected final String TEXT_170 = "));" + NL + "                                    }";
  protected final String TEXT_171 = NL + "                                    tuple.add(colContent == null || colContent.length() == 0" + NL + "                                             ? ";
  protected final String TEXT_172 = " : colContent.getBytes());";
  protected final String TEXT_173 = NL + "                                    tuple.add(colContent == null || colContent.trim().length() == 0" + NL + "                                            ? ";
  protected final String TEXT_174 = " : BigDataParserUtils.parseTo_";
  protected final String TEXT_175 = "(colContent));";
  protected final String TEXT_176 = NL + "                            } else {";
  protected final String TEXT_177 = NL + "                                    tuple.add(";
  protected final String TEXT_178 = ");";
  protected final String TEXT_179 = NL + "                                    tuple.add(BigDataParserUtils.parseTo_Date((String)";
  protected final String TEXT_180 = ", ";
  protected final String TEXT_181 = "));";
  protected final String TEXT_182 = NL + "                                    tuple.add(BigDataParserUtils.parseTo_Date(";
  protected final String TEXT_183 = ", ";
  protected final String TEXT_184 = "));";
  protected final String TEXT_185 = NL + "                            }";
  protected final String TEXT_186 = NL + NL + "                        fixedValues.add(tuple);" + NL + "                    }" + NL + "" + NL + "                } catch (java.lang.Exception e) {" + NL + "                    e.printStackTrace();" + NL + "                }" + NL + "" + NL + "                return fixedValues;" + NL + "            }";
  protected final String TEXT_187 = NL + "\t\t           Object o_";
  protected final String TEXT_188 = " = ";
  protected final String TEXT_189 = ".get(";
  protected final String TEXT_190 = ").get(";
  protected final String TEXT_191 = ");" + NL + "\t\t           if(o_";
  protected final String TEXT_192 = " != null) {" + NL + "\t\t               ";
  protected final String TEXT_193 = ".";
  protected final String TEXT_194 = " = java.nio.ByteBuffer.wrap(o_";
  protected final String TEXT_195 = ".toString().getBytes());" + NL + "\t\t\t\t\t\t\t\t\t\t\t\t\t}";
  protected final String TEXT_196 = NL + "                    ";
  protected final String TEXT_197 = ".";
  protected final String TEXT_198 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_199 = NL + "                            (";
  protected final String TEXT_200 = ".get(";
  protected final String TEXT_201 = ").get(";
  protected final String TEXT_202 = ").toString(), ";
  protected final String TEXT_203 = ");";
  protected final String TEXT_204 = NL + "                    // All inputs came from the context, there are string we want to cast";
  protected final String TEXT_205 = NL + "                    ";
  protected final String TEXT_206 = ".";
  protected final String TEXT_207 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_208 = NL + "                            (";
  protected final String TEXT_209 = ".get(";
  protected final String TEXT_210 = ").get(";
  protected final String TEXT_211 = ").toString());";
  protected final String TEXT_212 = NL + "\t\t\t           Object o_";
  protected final String TEXT_213 = " = ";
  protected final String TEXT_214 = ".get(";
  protected final String TEXT_215 = ").get(";
  protected final String TEXT_216 = ");" + NL + "\t\t\t           if(o_";
  protected final String TEXT_217 = " != null) {" + NL + "\t\t\t               ";
  protected final String TEXT_218 = ".";
  protected final String TEXT_219 = " = java.nio.ByteBuffer.wrap((";
  protected final String TEXT_220 = ") o_";
  protected final String TEXT_221 = ");" + NL + "\t\t\t           }" + NL + "\t\t            ";
  protected final String TEXT_222 = NL + "\t\t            ";
  protected final String TEXT_223 = ".";
  protected final String TEXT_224 = " =" + NL + "\t\t                    (";
  protected final String TEXT_225 = ") ";
  protected final String TEXT_226 = ".get(";
  protected final String TEXT_227 = ").get(";
  protected final String TEXT_228 = ");" + NL + "\t\t            ";
  protected final String TEXT_229 = NL + "                   Object o_";
  protected final String TEXT_230 = " = ";
  protected final String TEXT_231 = ".get(";
  protected final String TEXT_232 = ");" + NL + "                   if(o_";
  protected final String TEXT_233 = " != null) {";
  protected final String TEXT_234 = NL + "                       ";
  protected final String TEXT_235 = ".";
  protected final String TEXT_236 = " = java.nio.ByteBuffer.wrap(o_";
  protected final String TEXT_237 = ".toString().getBytes());" + NL + "                                                    }";
  protected final String TEXT_238 = NL + "                    ";
  protected final String TEXT_239 = ".";
  protected final String TEXT_240 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_241 = NL + "                            (";
  protected final String TEXT_242 = ".get(";
  protected final String TEXT_243 = ").toString(), ";
  protected final String TEXT_244 = ");";
  protected final String TEXT_245 = NL + "                    // All inputs came from the context, there are string we want to cast";
  protected final String TEXT_246 = NL + "                    ";
  protected final String TEXT_247 = ".";
  protected final String TEXT_248 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_249 = NL + "                            (";
  protected final String TEXT_250 = ".get(";
  protected final String TEXT_251 = ").toString());";
  protected final String TEXT_252 = NL + "                       Object o_";
  protected final String TEXT_253 = " = ";
  protected final String TEXT_254 = ".get(";
  protected final String TEXT_255 = ");" + NL + "                       if(o_";
  protected final String TEXT_256 = " != null) {";
  protected final String TEXT_257 = NL + "                           ";
  protected final String TEXT_258 = ".";
  protected final String TEXT_259 = " = java.nio.ByteBuffer.wrap((";
  protected final String TEXT_260 = ") o_";
  protected final String TEXT_261 = ");" + NL + "                       }";
  protected final String TEXT_262 = NL + "                    ";
  protected final String TEXT_263 = ".";
  protected final String TEXT_264 = " =" + NL + "                            (";
  protected final String TEXT_265 = ") ";
  protected final String TEXT_266 = ".get(";
  protected final String TEXT_267 = ");";
  protected final String TEXT_268 = NL + NL + "/**" + NL + " * This method check if the context string is encoded or not" + NL + " */" + NL + "public static boolean checkForBase64Encode_";
  protected final String TEXT_269 = "(String string) {" + NL + "    String pattern = \"^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$\";" + NL + "    java.util.regex.Pattern r = java.util.regex.Pattern.compile(pattern);" + NL + "    java.util.regex.Matcher m = r.matcher(string);" + NL + "    return m.find();" + NL + "}";
  protected final String TEXT_270 = NL + NL + "public static class ";
  protected final String TEXT_271 = "_InputFormat implements InputFormat<NullWritable, ";
  protected final String TEXT_272 = ">, org.apache.hadoop.conf.Configurable {" + NL + "" + NL + "    private ContextProperties context;" + NL + "    private java.util.List<java.util.List<Object>> stuff;" + NL + "" + NL + "    //init" + NL + "    public void setConf(Configuration conf){" + NL + "        context = new ContextProperties(conf); ";
  protected final String TEXT_273 = NL + "    }" + NL + "" + NL + "    // never called" + NL + "    public Configuration getConf(){" + NL + "        return null;" + NL + "    }" + NL;
  protected final String TEXT_274 = NL + NL + "    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {" + NL + "        NullInputSplit[] splits = new NullInputSplit[";
  protected final String TEXT_275 = "];" + NL + "        for (int i = 0; i < splits.length; i++)" + NL + "            splits[i] = new NullInputSplit();" + NL + "        return splits;" + NL + "    }" + NL + "" + NL + "" + NL + "    public RecordReader<NullWritable, ";
  protected final String TEXT_276 = "> getRecordReader(InputSplit split, JobConf jobConf," + NL + "              Reporter reporter) throws IOException {" + NL + "        return new RecordReader<NullWritable, ";
  protected final String TEXT_277 = ">() {" + NL + "            private int mCount = 0;" + NL + "" + NL + "            public boolean next(NullWritable key, ";
  protected final String TEXT_278 = " ";
  protected final String TEXT_279 = ") throws IOException {" + NL + "                if (mCount >= stuff.size())" + NL + "                    return false;";
  protected final String TEXT_280 = NL + "                mCount++;" + NL + "                return true;" + NL + "            }" + NL + "" + NL + "            public NullWritable createKey() {" + NL + "                return NullWritable.get();" + NL + "            }" + NL + "" + NL + "            public ";
  protected final String TEXT_281 = " createValue() {" + NL + "                return new ";
  protected final String TEXT_282 = "();" + NL + "            }" + NL + "" + NL + "            public long getPos() {" + NL + "                return 0;" + NL + "            }" + NL + "" + NL + "            public void close() {" + NL + "            }" + NL + "" + NL + "            public float getProgress() throws IOException {" + NL + "                if (mCount >= stuff.size())" + NL + "                    return 1.0f;" + NL + "                return 1.0f * mCount / stuff.size();" + NL + "            }" + NL + "        };" + NL + "    }" + NL + "" + NL + "    public static class NullInputSplit implements InputSplit, Writable {" + NL + "" + NL + "        public long getLength() throws IOException {" + NL + "            // Since the input split is fixed, there's no choices to be made based on the size." + NL + "            return 0;" + NL + "        }" + NL + "" + NL + "        public String[] getLocations() throws IOException {" + NL + "            // It looks like if the locations are empty, then any task can process the split." + NL + "            return new String[] {};" + NL + "        }" + NL + "" + NL + "        public void write(DataOutput out) throws IOException {" + NL + "            out.writeInt(0);" + NL + "        }" + NL + "" + NL + "        public void readFields(DataInput in) throws IOException {" + NL + "            int size = in.readInt();" + NL + "        }" + NL + "    }" + NL + "}";
  protected final String TEXT_283 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
// Parse the inputs to this javajet generator.
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode) codeGenArgument.getArgument();
final String cid = node.getUniqueName();

    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
	//need to define MapperHelper before MapperGenerator, so use this trick
	class MapperHelperBase{
		public void map(){
		}

		public void prepare(){
		}

		public void configure(){
		}

		public void close(){
		}
	}

	class MapperGenerator{
		MapperHelperBase mapper;

		org.talend.core.model.process.AbstractNode node = null;
		String cid = null;
		String mapperClass = null;

		Object inKey = null;
		Object inKeyClass = null;

		Object inValue = null;
		Object inValueClass = null;

		Object outKey = null;
		Object outKeyClass = null;

		Object outValue = null;
		Object outValueClass = null;

		public MapperGenerator(MapperHelperBase mapper){
			this.mapper = mapper;
		}

		public void init(INode node, String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			this.node = (org.talend.core.model.process.AbstractNode)node;
			this.cid = cid;
			this.inKey = (inKey == null ? "key_"+cid : inKey);
			this.inValue = (inValue == null ? "value_"+cid : inValue);
			this.outKey = (outKey == null ? "outputKey_"+cid : outKey);
			this.outValue = (outValue == null ? "outputValue_"+cid : outValue);
			this.mapperClass = buildClassName(cid, "m");
			this.inKeyClass = buildClassName(inKey, "row");
			this.inValueClass = buildClassName(inValue, "row");
			this.outKeyClass = buildClassName(outKey, "row");
			this.outValueClass = buildClassName(outValue, "row");
		}

		private String buildClassName(String name, String type){
			if(type.equals("m")){
				return name + "Mapper";
			}else if(type.equals("r")){
				return name + "Reducer";
			}else if(type.equals("row")){
				return name + "Struct";
			}else{
				return null;
			}
		}

		private Object buildClassName(Object name, String type){
			if(type.equals("row")){
				if(name instanceof java.util.Map){
					java.util.Map<String, String> classes = new java.util.HashMap<String, String>();
					java.util.Map<String, String> names = (java.util.Map<String, String>)name;
					for(String key : names.keySet()){
						classes.put(key, buildClassName(names.get(key), "row"));
					}
					return classes;
				}else if(name instanceof String){
					return buildClassName(name.toString(), "row");
				}else if(name == null){
					return "NullWritable";
				}
			}
			return null;
		}

		public String getInKeyClass(String name){
			if(inKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKeyClass).get(name);
			}
			return getInKeyClass();
		}

		public String getInKeyClass(){
			if(inKeyClass instanceof String){
				return inKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInKey(String name){
			if(inKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKey).get(name);
			}
			return getInKey();
		}

		public String getInKey(){
			if(inKey instanceof String){
				return inKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutKeyClass(String name){
			if(outKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKeyClass).get(name);
			}
			return getOutKeyClass();
		}

		public String getOutKeyClass(){
			if(outKeyClass instanceof String){
				return outKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutKey(String name){
			if(outKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKey).get(name);
			}
			return getOutKey();
		}

		public String getOutKey(){
			if(outKey instanceof String){
				return outKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInValueClass(String name){
			if(inValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValueClass).get(name);
			}
			return getInValueClass();
		}

		public String getInValueClass(){
			if(inValueClass instanceof String){
				return inValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInValue(String name){
			if(inValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValue).get(name);
			}
			return getInValue();
		}

		public String getInValue(){
			if(inValue instanceof String){
				return inValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutValueClass(String name){
			if(outValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValueClass).get(name);
			}
			return getOutValueClass();
		}

		public String getOutValueClass(){
			if(outValueClass instanceof String){
				return outValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutValue(String name){
			if(outValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValue).get(name);
			}
			return getOutValue();
		}

		public String getOutValue(){
			if(outValue instanceof String){
				return outValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_6);
    
		}

		public String getCodeEmit(String outKey, String outValue) {
			return "output_" + cid + ".collect("
					+ (outKey == null ? "outputKey_"+cid : outKey) + ","
					+ outValue + ");";
		}

		public void generate(){
		
    stringBuffer.append(TEXT_7);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_14);
    if(!outKey.equals(outValue)){//for tFindQuantiles, if outKey same as outValue, assume the write want to reuse same object
    stringBuffer.append(TEXT_15);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_17);
    }
    stringBuffer.append(TEXT_18);
    mapper.prepare();
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_23);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_24);
    }else{
    stringBuffer.append(TEXT_25);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_27);
    }
    stringBuffer.append(TEXT_28);
    if(!outKey.equals(outValue)){
    stringBuffer.append(TEXT_29);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_31);
    }
    stringBuffer.append(TEXT_32);
    mapper.configure();
    stringBuffer.append(TEXT_33);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    mapper.map();
    stringBuffer.append(TEXT_42);
    mapper.close();
    stringBuffer.append(TEXT_43);
    
		}
		public void generateConf(){
			if(node.isMapOnlyAfterReduce()){
			
    stringBuffer.append(TEXT_44);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_49);
    
			}else{
			
    stringBuffer.append(TEXT_50);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_55);
    
			}
		}

		public void generateConf(org.talend.core.model.process.IConnection inConn){
			String startNodeCid = inConn.getSource().getSubProcessStartNode(false).getUniqueName();
			
    stringBuffer.append(TEXT_56);
    stringBuffer.append(startNodeCid);
    stringBuffer.append(TEXT_57);
    
			generateConf();
		}
	}

	class MOMapperGenerator extends MapperGenerator{

		/** The single connection to pass along the output chain of Mappers
		 *  instead of sending to a dedicated collector via MultipleOutputs. */
		String connectionToChain = null;

		public MOMapperGenerator(MapperHelperBase mapper){
			super(mapper);
		}

		public void sendOutConnectionToChain(String name) {
			connectionToChain = getOutValue(name);
		}

		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			if (outValue != null && outValue.equals(connectionToChain))
				super.output(outKey, outValue);
			else {
				
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_63);
    
			}
		}

		public String getCodeEmit(String outKey, String outValue) {
			if (outValue != null && outValue.equals(connectionToChain))
				return super.getCodeEmit(outKey, outValue);
			else
				return "mos_" + cid + ".getCollector(\"" + outValue
						+ "\", reporter_"+ cid + ")"+ ".collect("
						+ (outKey == null ? "outputKey_"+cid : outKey) + ","
						+ outValue + ");";
		}

		public void generate(){
		
    stringBuffer.append(TEXT_64);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_71);
    
				if(outValueClass instanceof java.util.Map){
					for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
					
    stringBuffer.append(TEXT_72);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_73);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_74);
    
					}
				}
				
    stringBuffer.append(TEXT_75);
    mapper.prepare();
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_82);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_83);
    }else{
    stringBuffer.append(TEXT_84);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_86);
    }
    stringBuffer.append(TEXT_87);
    
					if(outValueClass instanceof java.util.Map){
						for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
						
    stringBuffer.append(TEXT_88);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_89);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_90);
    
						}
					}
					
    stringBuffer.append(TEXT_91);
    mapper.configure();
    stringBuffer.append(TEXT_92);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    mapper.map();
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    mapper.close();
    stringBuffer.append(TEXT_102);
    
		}
		public void generateConf(){
			if(node.isMapOnlyAfterReduce()){
			
    stringBuffer.append(TEXT_103);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_107);
    
			}else{
			
    stringBuffer.append(TEXT_108);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_112);
    
			}
			
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    
        	java.util.Map<String, String> values = (java.util.Map<String, String>)outValue;
        	for(String key : values.keySet()){
        	
    stringBuffer.append(TEXT_115);
    stringBuffer.append(values.get(key));
    stringBuffer.append(TEXT_116);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(getOutValueClass(key));
    stringBuffer.append(TEXT_118);
    
        	}
		}
	}

	final String M_TYPE_BASE = "base";
	final String M_TYPE_MO = "mo";

	class MapperHelper extends MapperHelperBase{

		MapperGenerator mapperGen;

		String cid = null;

		public void setType(String type){
			if(type.equals(M_TYPE_BASE)){
				mapperGen = new MapperGenerator(this);
			}else if(type.equals(M_TYPE_MO)){
				mapperGen = new MOMapperGenerator(this);
			}
		}

		public void init(INode node, String cid, String inKey, String inValue, String outKey, Object outValue){
			if(mapperGen == null){
				mapperGen = new MapperGenerator(this);
			}
			mapperGen.init(node, cid, inKey, inValue, outKey, outValue);
			this.cid = mapperGen.cid;
		}

		public String getInKeyClass(String name){
			return mapperGen.getInKeyClass(name);
		}

		public String getInKeyClass(){
			return mapperGen.getInKeyClass();
		}

		public String getInKey(String name){
			return mapperGen.getInKey(name);
		}

		public String getInKey(){
			return mapperGen.getInKey();
		}

		public String getOutKeyClass(String name){
			return mapperGen.getOutKeyClass(name);
		}

		public String getOutKeyClass(){
			return mapperGen.getOutKeyClass();
		}

		public String getOutKey(String name){
			return mapperGen.getOutKey(name);
		}

		public String getOutKey(){
			return mapperGen.getOutKey();
		}

		public String getInValueClass(String name){
			return mapperGen.getInValueClass(name);
		}

		public String getInValueClass(){
			return mapperGen.getInValueClass();
		}

		public String getInValue(String name){
			return mapperGen.getInValue(name);
		}

		public String getInValue(){
			return mapperGen.getInValue();
		}

		public String getOutValueClass(String name){
			return mapperGen.getOutValueClass(name);
		}

		public String getOutValueClass(){
			return mapperGen.getOutValueClass();
		}

		public String getOutValue(String name){
			return mapperGen.getOutValue(name);
		}

		public String getOutValue(){
			return mapperGen.getOutValue();
		}

		/**
		 * In the case where the underlying implementation supports multiple
		 * outputs, this causes the named output to be passed along the chain
		 * of mapper tasks instead of using the MultipleOutputs object.
		 */
		public void sendOutConnectionToChain(String name) {
			if (mapperGen instanceof MOMapperGenerator)
				((MOMapperGenerator)mapperGen).sendOutConnectionToChain(name);
		}

		public void output(String outKey, String outValue){
			mapperGen.output(outKey, outValue);
		}

		public String getCodeEmit(String outKey, String outValue){
			return mapperGen.getCodeEmit(outKey, outValue);
		}

		public void generate(){
			mapperGen.generate();
		}

		public void generateConf(){
			mapperGen.generateConf();
		}

		public void generateConf(org.talend.core.model.process.IConnection inConn){
			mapperGen.generateConf(inConn);
		}

		public void map(){
		}

		public void prepare(){
		}

		public void configure(){
		}

		public void close(){
		}
	}
	
    

/**
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class MrMapperRowTransformUtil extends org.talend.designer.common.CommonRowTransformUtil {
    /** Must be set to provide a Mapper context to this tool. */
    public MapperHelper helper;

    public String getCodeToGetInField(String columnName) {
        return helper.getInValue() + "." + columnName;
    }
    public String getInValue() {
        return helper.getInValue();
    }
    
    /** Not used in MR, but had to implement it so it returns null*/
    public String getOutValue(){
        return null;
    }

    public String getInValueClass() {
        return helper.getInValueClass();
    }
    
    /** Not used in MR, but had to implement it so it returns null*/
    public String getOutValueClass() {
        return null;
    }

    public String getCodeToGetOutField(boolean isReject, String columnName) {
        return helper.getOutValue(isReject ? "reject" : "main") + "." + columnName;
    }

    public String getCodeToSetOutField(boolean isReject, String columnName, String codeValue) {
        return helper.getOutValue(isReject ? "reject" : "main") + "." + columnName + " = " + codeValue + ";";
    }


    public String getCodeToSetOutField(boolean isReject, String columnName, String codeValue, String operator) {
        return helper.getOutValue(isReject ? "reject" : "main") + "." + columnName + " = " + codeValue + ";";
    }


    public String getCodeToEmit(boolean isReject) {
        return helper.getCodeEmit(null, helper.getOutValue(isReject ? "reject" : "main"));
    }

    /**
     * Generates a Mapper that is typically correct for the given
     * TransformerBase.  This is a shortcut for many use cases, but not
     * mandatory.
     */
    public void generateMrCode(final org.talend.designer.common.TransformerBase transformer) {

        if (transformer.isUnnecessary()) {
            
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    
            return;
        }

        // Use a MapperHelper base that has been tied into the transformer.
        class Mapper extends MapperHelper {

            public void prepare() {
                transformer.generateHelperClasses(false);
                transformer.generateTransformContextDeclaration();
            }

            public void configure() {
                transformer.generateTransformContextInitialization();
            }

            public void map() {
                
    stringBuffer.append(TEXT_120);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_121);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    
                transformer.generateTransform();
            }
        }

        Mapper mapper = new Mapper();
        this.helper = mapper;

        if (transformer.isMultiOutput()) {
            // The multi-output condition is slightly different, and the
            // MapperHelper is configured with internal names for the
            // connections.
            mapper.setType(M_TYPE_MO);
            java.util.HashMap<String, String> names = new java.util.HashMap<String, String>();
            names.put("main", transformer.getOutConnMainName());
            names.put("reject", transformer.getOutConnRejectName());
            mapper.init(node, cid, null, transformer.getInConnName(), null,
                    names);
            // When there is more than one output, the main output continues
            // along the chain, and the reject output is sent to a secondary
            // file.
            mapper.sendOutConnectionToChain("main");
        } else {
            mapper.init(node, cid, null, transformer.getInConnName(), null,
                    transformer.getOutConnMainName() != null
                            ? transformer.getOutConnMainName()
                            : transformer.getOutConnRejectName());
        }

        // Use the configured mapper helper to create the Mapper MR_CODE
        mapper.generate();
    }


    public void generateMrConfig(final org.talend.designer.common.TransformerBase transformer,
            String inputMapperClass) {

        if (transformer.isUnnecessary()) {
            
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    
            return;
        }

        // The connection that contains rows to be passed along the chain.  At
        // least one of these is guaranteed to be present.
        IConnection chained = transformer.getOutConnMain() != null
                ? transformer.getOutConnMain() : transformer.getOutConnReject();

        
    stringBuffer.append(TEXT_125);
    stringBuffer.append(inputMapperClass);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(transformer.getInConnName());
    stringBuffer.append(TEXT_127);
    stringBuffer.append(chained.getName());
    stringBuffer.append(TEXT_128);
    
        // If multiple outputs, the rejected output is guaranteed to be on a
        // multiple outputs.
        if (transformer.isMultiOutput()) {
            String shortCid = cid.replaceAll("_Out", "").replaceAll("_In", "");
            
    stringBuffer.append(TEXT_129);
    stringBuffer.append(shortCid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_131);
    stringBuffer.append(transformer.getOutConnRejectName());
    stringBuffer.append(TEXT_132);
    
        }
    }
}

    stringBuffer.append(TEXT_133);
    

/**
 * Contains common processing for tFixedFlowInput code generation.  This is
 * used in MapReduce.
 *
 * The following imports must occur before importing this file:
 * @ include file="@{org.talend.designer.components.mrprovider}/resources/utils/common_codegen_util.javajet"
 */
class TFixedFlowInputUtil extends org.talend.designer.common.TransformerBase {

    /** If initialized, contains code that can be assigned to a list of lists.
     *  Exactly one of codeValues or codeValuesBase64 will be non-null. */
    private StringBuilder codeValues = null;

    /** If initialized, contains code that can be assigned to a list of lists.
     *  Exactly one of codeValues or codeValuesBase64 will be non-null. */
    private StringBuilder codeValuesBase64 = null;

    /** The number of times the specified values should be repeated. */
    public final String nbRows;

    public TFixedFlowInputUtil(INode node,
            org.talend.designer.common.BigDataCodeGeneratorArgument argument,
            org.talend.designer.common.CommonRowTransformUtil rowTransformUtil) {
        super(node, argument, rowTransformUtil, "FLOW", "REJECT");

        nbRows = ElementParameterParser.getValue(node, "__NB_ROWS__");

        // Exactly one of these will be true.
        if ("true".equals(ElementParameterParser.getValue(node, "__USE_SINGLEMODE__")))
            initCodeValuesSingleMode((List<Map<String,String>>)
                    ElementParameterParser.getObjectValue(node, "__VALUES__"));
        else if ("true".equals(ElementParameterParser.getValue(node, "__USE_INTABLE__")))
            initCodeValuesInTable((List<Map<String,String>>)
                    ElementParameterParser.getObjectValue(node, "__INTABLE__"));
        else if ("true".equals(ElementParameterParser.getValue(node, "__USE_INLINECONTENT__")))
            initCodeValuesInlineContent(ElementParameterParser.getValue(node,"__INLINECONTENT__"));
        else if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__")))
            initCodeValuesContext(ElementParameterParser.getValue(node,"__CONTEXT_VARIABLE__"));
    }

    /**
     * Used from the constructor to initialize the fixed values when only a
     * single value is specified.
     */
    private void initCodeValuesSingleMode(List<Map<String,String>> tableValues) {
        codeValues = new StringBuilder();
        if (tableValues == null || tableValues.size() == 0)
            return;

        for(Map<String, String> tableValue : tableValues) {
            String label = tableValue.get("SCHEMA_COLUMN");
            String value = tableValue.get("VALUE");
            if (value == null || value.length() == 0) { //use the default value
                IMetadataColumn column = getOutConnMain().getMetadataTable().getColumn(label);
                String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                String defaultValue = "null";
                if (column.getDefault() != null && column.getDefault().length() > 0) {
                    defaultValue = column.getDefault();
                } else {
                    if (typeToGenerate == null) {
                        throw new IllegalArgumentException();
                    }
                    if (JavaTypesManager.isJavaPrimitiveType(typeToGenerate)) {
                        if ("char".equals(typeToGenerate)) {
                            defaultValue = "' '";
                        } else if ("boolean".equals(typeToGenerate)) {
                            defaultValue = "false";
                        } else if ("byte".equals(typeToGenerate)) {
                            defaultValue = "(byte)0";
                        } else if ("double".equals(typeToGenerate)) {
                            defaultValue = "0.0d";
                        } else if ("float".equals(typeToGenerate)) {
                            defaultValue = "0.0f";
                        } else if ("long".equals(typeToGenerate)) {
                            defaultValue = "0l";
                        } else if ("short".equals(typeToGenerate)) {
                            defaultValue = "(short) 0";
                        } else {
                            defaultValue = "0";
                        }
                    }
                }
                codeValues.append(defaultValue).append(',');
            } else {
                codeValues.append(value).append(',');
            }
        }
        // Remove trailing commas.
        if (codeValues.length() > 0)
            codeValues.setLength(codeValues.length() - 1);
    }

    /**
     * Used from the constructor to initialize the fixed values when the list of
     * values is specified in the parameter table.
     */
    private void initCodeValuesInTable(List<Map<String,String>> tableValues) {
        codeValues = new StringBuilder();
        if (tableValues == null || tableValues.size() == 0)
            return;

        for (Map<String, String> tableValue : tableValues) {
            if (0 != codeValues.length())
                codeValues.append("}, new Object[] {");
            for (IMetadataColumn column: getOutColumnsMain()) {
                String label = column.getLabel();
                String value = tableValue.get(label);
                if (value == null || value.length() == 0) { //use the default value
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                    if (column.getDefault() != null && column.getDefault().length() > 0) {
                        codeValues.append(column.getDefault()).append(',');
                    } else {
                        if (typeToGenerate == null) {
                            throw new IllegalArgumentException();
                        }
                        if (JavaTypesManager.isJavaPrimitiveType(typeToGenerate)) {
                            if ("char".equals(typeToGenerate)) {
                                codeValues.append("' '").append(',');
                            } else if ("boolean".equals(typeToGenerate)) {
                                codeValues.append("false").append(',');
                            } else if ("byte".equals(typeToGenerate)) {
                                codeValues.append("(byte)0").append(',');
                            } else if ("double".equals(typeToGenerate)) {
                                codeValues.append("0.0d").append(',');
                            } else if ("float".equals(typeToGenerate)) {
                                codeValues.append("0.0f").append(',');
                            } else if ("long".equals(typeToGenerate)) {
                                codeValues.append("0l").append(',');
                            } else if ("short".equals(typeToGenerate)) {
                                codeValues.append("(short) 0").append(',');
                            } else {
                                codeValues.append("0").append(',');
                            }
                        } else {
                            codeValues.append("null").append(',');
                        }
                    }
                } else {
                    codeValues.append(value).append(',');
                }
            }
            // Remove trailing commas.
            if (codeValues.length() > 0)
                codeValues.setLength(codeValues.length() - 1);
        }
    }

    /**
     * Used from the constructor to initialize the fixed values when the list of
     * values is specified inline.
     */
    private void initCodeValuesInlineContent(String fileContent) {
        String lineSeparator = (String) java.security.AccessController.doPrivileged(
                new sun.security.action.GetPropertyAction("line.separator"));

        String fileContentEnCodeStr = "";
        try {
            fileContentEnCodeStr = (new sun.misc.BASE64Encoder()).encode(fileContent.getBytes("UTF-8"));
        } catch (java.io.UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        codeValuesBase64 = new StringBuilder();
        for(String item : fileContentEnCodeStr.split(lineSeparator))
            codeValuesBase64.append('"').append(item).append("\",");
        // Remove trailing commas.
        if (codeValuesBase64.length() > 0)
            codeValuesBase64.setLength(codeValuesBase64.length() - 1);
    }

    /**
     * Used from the constructor to initialize the fixed values when the list of
     * values is in the context.
     */
    private void initCodeValuesContext(String contextVariable) {
        
    stringBuffer.append(TEXT_134);
    
    }

    /**
     * Generates the initialized class member that will contain the fixed data.
     */
    public void generateFixedDataMember(String codeFixedVar) {
        // The fixed data member is composed of a simple declaration, but also
        // a method to parse the data passed in.
        if (codeValues != null) {
            
    stringBuffer.append(TEXT_135);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_136);
    stringBuffer.append( codeValues );
    stringBuffer.append(TEXT_137);
    
        } else if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__"))) {
            
    stringBuffer.append(TEXT_138);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(ElementParameterParser.getValue(node, "__CONTEXT_VARIABLE__"));
    stringBuffer.append(TEXT_141);
    stringBuffer.append(ElementParameterParser.getValue(node, "__CONTEXTROWSEPARATOR__").replace("\\", "\\\\\\\\"));
    stringBuffer.append(TEXT_142);
    
        } else {
            
    stringBuffer.append(TEXT_143);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_144);
    stringBuffer.append( codeValuesBase64 );
    stringBuffer.append(TEXT_145);
    
        }
    }

    /**
     * Generates a private static init method that can be used to initialize a
     * static List of List of objects from the values specified by the user.
     */
    public void generateFixedDataInit() {
        org.talend.core.model.process.IProcess process = node.getProcess();
        if (codeValues != null) {
            
    stringBuffer.append(TEXT_146);
    
        } else if ((!org.talend.core.model.process.ProcessUtils.isTestContainer(process))
                && ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__")))) {
            // NOT test mode, handle context variable as string
            
    stringBuffer.append(TEXT_147);
    stringBuffer.append( ElementParameterParser.getValue(node, "__CONTEXTFIELDSEPARATOR__") );
    stringBuffer.append(TEXT_148);
    
        } else {
            String fieldSeparator = "";
            String rowSeparator = "";
            if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__"))) {
                // test mode, handle context variable as encoded string
                fieldSeparator= ElementParameterParser.getValue(node, "__CONTEXTFIELDSEPARATOR__");
                rowSeparator = ElementParameterParser.getValue(node, "__CONTEXTROWSEPARATOR__");
            } else {// inline content
                fieldSeparator= ElementParameterParser.getValue(node, "__FIELDSEPARATOR__");
                rowSeparator = ElementParameterParser.getValue(node, "__ROWSEPARATOR__");
            }
            
    stringBuffer.append(TEXT_149);
     if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__"))) { 
    stringBuffer.append(TEXT_150);
    stringBuffer.append(rowSeparator);
    stringBuffer.append(TEXT_151);
     } 
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(rowSeparator);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(TEXT_156);
    stringBuffer.append( fieldSeparator );
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    
                        int i = -1;
                        for (IMetadataColumn column : getOutColumnsMain()) {
                            i++;
                            String label = column.getLabel();
                            String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                            JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                            String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();

                            String defaultValue = "null";
                            if (column.getDefault() != null && column.getDefault().length() > 0) {
                                defaultValue = column.getDefault();
                            } else {
                                if (typeToGenerate == null) {
                                    throw new IllegalArgumentException();
                                }
                                if (JavaTypesManager.isJavaPrimitiveType(typeToGenerate)) {
                                    if ("char".equals(typeToGenerate)) {
                                        defaultValue = "' '";
                                    } else if ("boolean".equals(typeToGenerate)) {
                                        defaultValue = "false";
                                    } else if ("byte".equals(typeToGenerate)) {
                                        defaultValue = "(byte)0";
                                    } else if ("double".equals(typeToGenerate)) {
                                        defaultValue = "0.0d";
                                    } else if ("float".equals(typeToGenerate)) {
                                        defaultValue = "0.0f";
                                    } else if ("long".equals(typeToGenerate)) {
                                        defaultValue = "0l";
                                    } else if ("short".equals(typeToGenerate)) {
                                        defaultValue = "(short) 0";
                                    } else {
                                        defaultValue = "0";
                                    }
                                }
                            }
                            
    stringBuffer.append(TEXT_159);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_161);
    
                                if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
                                    
    stringBuffer.append(TEXT_162);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_163);
    
                                } else if (javaType == JavaTypesManager.DATE) {
                                    
    stringBuffer.append(TEXT_164);
     if ((defaultValue==null) || "".equals(defaultValue) || "null".equals(defaultValue)){ 
    stringBuffer.append(TEXT_165);
     } 
    stringBuffer.append(TEXT_166);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_167);
    stringBuffer.append(TEXT_168);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_169);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_170);
    
                                } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                                    
    stringBuffer.append(TEXT_171);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_172);
    
                                } else {
                                    
    stringBuffer.append(TEXT_173);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_174);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_175);
    
                                }
                                
    stringBuffer.append(TEXT_176);
    
                                if (javaType != JavaTypesManager.DATE) {
                                    
    stringBuffer.append(TEXT_177);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_178);
    
                                } else if (defaultValue == null || "".equals(defaultValue) || "null".equals(defaultValue)) {
                                    
    stringBuffer.append(TEXT_179);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_180);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_181);
    
                                } else {
                                    
    stringBuffer.append(TEXT_182);
    stringBuffer.append(defaultValue );
    stringBuffer.append(TEXT_183);
    stringBuffer.append( patternValue );
    stringBuffer.append(TEXT_184);
    
                                }
                                
    stringBuffer.append(TEXT_185);
    
                        }
                        
    stringBuffer.append(TEXT_186);
    
        }
    }

    /**
     * Generates code to copy from the fixed buffer to the named output struct.
     */
    public void generateCopyFromFixedToOut(String codeFixedVar, String codeOutStruct, String codeRowCount) {
        int i = 0;
        for (IMetadataColumn column : getOutColumnsMain()) {
            // TODO: this assignment should be handled by a new RowTransformUtil
            // that doesn't rely on an underlying Mapper.
            JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());

            if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__"))) {
                if (codeGenArgument.getRecordStructGenerator().isByteArrayWrappedInBuffer() && (javaType == JavaTypesManager.BYTE_ARRAY)) {
                    
    stringBuffer.append(TEXT_187);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(codeRowCount);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_194);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_195);
    
                } else if (javaType == JavaTypesManager.DATE) {
                    
    stringBuffer.append(TEXT_196);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_198);
    stringBuffer.append( javaType.getNullableClass().getSimpleName());
    stringBuffer.append(TEXT_199);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(codeRowCount);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(column.getPattern());
    stringBuffer.append(TEXT_203);
    
                } else {
                    
    stringBuffer.append(TEXT_204);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_207);
    stringBuffer.append( javaType.getNullableClass().getSimpleName());
    stringBuffer.append(TEXT_208);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(codeRowCount);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_211);
    
                }
            } else {
		        // Avro metadata table compatibility
		        if (codeGenArgument.getRecordStructGenerator().isByteArrayWrappedInBuffer()&& (javaType == JavaTypesManager.BYTE_ARRAY)) {
		            
    stringBuffer.append(TEXT_212);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(codeRowCount);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_219);
    stringBuffer.append( javaType.getNullableClass().getSimpleName() );
    stringBuffer.append(TEXT_220);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_221);
    
					i++;
		        } else {
		            
    stringBuffer.append(TEXT_222);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_224);
    stringBuffer.append( javaType.getNullableClass().getSimpleName() );
    stringBuffer.append(TEXT_225);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(codeRowCount);
    stringBuffer.append(TEXT_227);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_228);
    
                }
            }
        }
    }
    

    /**
     * Generates code to copy from the fixed buffer to the named output struct.
     */
    public void generateCopyFromFixedToOut(String codeFixedVar, String codeOutStruct) {
        int i = 0;
        for (IMetadataColumn column : getOutColumnsMain()) {
            // TODO: this assignment should be handled by a new RowTransformUtil
            // that doesn't rely on an underlying Mapper.
            JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());

            if ("true".equals(ElementParameterParser.getValue(node, "__USE_CONTEXT__"))) {
                if (codeGenArgument.getRecordStructGenerator().isByteArrayWrappedInBuffer() && (javaType == JavaTypesManager.BYTE_ARRAY)) {
                    
    stringBuffer.append(TEXT_229);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_232);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_233);
    stringBuffer.append(TEXT_234);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_236);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_237);
    
                } else if (javaType == JavaTypesManager.DATE) {
                    
    stringBuffer.append(TEXT_238);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_240);
    stringBuffer.append( javaType.getNullableClass().getSimpleName());
    stringBuffer.append(TEXT_241);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_243);
    stringBuffer.append(column.getPattern());
    stringBuffer.append(TEXT_244);
    
                } else {
                    
    stringBuffer.append(TEXT_245);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_248);
    stringBuffer.append( javaType.getNullableClass().getSimpleName());
    stringBuffer.append(TEXT_249);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_251);
    
                }
            } else {
                // Avro metadata table compatibility
                if (codeGenArgument.getRecordStructGenerator().isByteArrayWrappedInBuffer()&& (javaType == JavaTypesManager.BYTE_ARRAY)) {
                    
    stringBuffer.append(TEXT_252);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_259);
    stringBuffer.append( javaType.getNullableClass().getSimpleName() );
    stringBuffer.append(TEXT_260);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_261);
    
                    i++;
                } else {
                    
    stringBuffer.append(TEXT_262);
    stringBuffer.append(codeOutStruct);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_264);
    stringBuffer.append( javaType.getNullableClass().getSimpleName() );
    stringBuffer.append(TEXT_265);
    stringBuffer.append(codeFixedVar);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(i++);
    stringBuffer.append(TEXT_267);
    
                }
            }
        }
    }
}

    
final MrMapperRowTransformUtil mrTransformUtil = new MrMapperRowTransformUtil();
final TFixedFlowInputUtil tFixedFlowInputUtil = new TFixedFlowInputUtil(
        node, codeGenArgument, mrTransformUtil);

List< ? extends IConnection> connections = node.getOutgoingConnections();
if (connections == null || connections.size() == 0)
    return "";

IConnection connection = connections.get(0);
String connName = connection.getName();
String outStruct = connName + "Struct";


    stringBuffer.append(TEXT_268);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_269);
    

    stringBuffer.append(TEXT_270);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_271);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_272);
     tFixedFlowInputUtil.generateFixedDataMember("stuff"); 
    stringBuffer.append(TEXT_273);
     tFixedFlowInputUtil.generateFixedDataInit(); 
    stringBuffer.append(TEXT_274);
    stringBuffer.append(tFixedFlowInputUtil.nbRows);
    stringBuffer.append(TEXT_275);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_279);
    tFixedFlowInputUtil.generateCopyFromFixedToOut("stuff", connName, "mCount"); 
    stringBuffer.append(TEXT_280);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_281);
    stringBuffer.append(outStruct);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(TEXT_283);
    return stringBuffer.toString();
  }
}
