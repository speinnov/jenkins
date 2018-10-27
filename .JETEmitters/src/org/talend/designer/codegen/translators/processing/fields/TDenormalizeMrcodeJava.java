package org.talend.designer.codegen.translators.processing.fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TDenormalizeMrcodeJava
{
  protected static String nl;
  public static synchronized TDenormalizeMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TDenormalizeMrcodeJava result = new TDenormalizeMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
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
  protected final String TEXT_119 = NL + "\t";
  protected final String TEXT_120 = "\t";
  protected final String TEXT_121 = NL + "\t\t\toutput_";
  protected final String TEXT_122 = ".collect(";
  protected final String TEXT_123 = ", ";
  protected final String TEXT_124 = ");" + NL + "\t\t";
  protected final String TEXT_125 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_126 = " extends MapReduceBase " + NL + "\t\t\t\timplements Reducer<";
  protected final String TEXT_127 = ", ";
  protected final String TEXT_128 = ", ";
  protected final String TEXT_129 = ", ";
  protected final String TEXT_130 = ">{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\t";
  protected final String TEXT_131 = " ";
  protected final String TEXT_132 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_133 = " ";
  protected final String TEXT_134 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_135 = NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_136 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_137 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_138 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_139 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_140 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_141 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_142 = " = new ";
  protected final String TEXT_143 = "(); " + NL + "\t\t\t\t\t";
  protected final String TEXT_144 = NL + "\t\t\t\t\t";
  protected final String TEXT_145 = " = new ";
  protected final String TEXT_146 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_147 = NL + "  \t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void reduce(";
  protected final String TEXT_148 = " key_";
  protected final String TEXT_149 = ", Iterator<";
  protected final String TEXT_150 = "> values_";
  protected final String TEXT_151 = ", " + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_152 = ", ";
  protected final String TEXT_153 = "> output_";
  protected final String TEXT_154 = ", Reporter reporter_";
  protected final String TEXT_155 = ") throws IOException{" + NL + "\t\t\t\t    final OutputCollector<";
  protected final String TEXT_156 = ", ";
  protected final String TEXT_157 = "> outputCollect_";
  protected final String TEXT_158 = "=output_";
  protected final String TEXT_159 = ";" + NL + "\t\t\t\t\t";
  protected final String TEXT_160 = NL + "\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_161 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_162 = NL + "\t\t\tChainReducer.setReducer(job, ";
  protected final String TEXT_163 = ".class, ";
  protected final String TEXT_164 = ".class," + NL + "        \t\t";
  protected final String TEXT_165 = ".class, ";
  protected final String TEXT_166 = ".class, ";
  protected final String TEXT_167 = ".class, true, new JobConf(false));" + NL + "\t\t";
  protected final String TEXT_168 = NL + "\t\t\tmos_";
  protected final String TEXT_169 = ".getCollector(\"";
  protected final String TEXT_170 = "\", reporter_";
  protected final String TEXT_171 = ").collect(";
  protected final String TEXT_172 = ", ";
  protected final String TEXT_173 = ");" + NL + "\t\t";
  protected final String TEXT_174 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_175 = " extends MapReduceBase " + NL + "\t\t\t\timplements Reducer<";
  protected final String TEXT_176 = ", ";
  protected final String TEXT_177 = ", ";
  protected final String TEXT_178 = ", WritableComparable>{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\tpublic MultipleOutputs mos_";
  protected final String TEXT_179 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_180 = " ";
  protected final String TEXT_181 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_182 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_183 = " ";
  protected final String TEXT_184 = " = null;\t" + NL + "\t\t\t\t\t";
  protected final String TEXT_185 = NL + "\t\t\t\t";
  protected final String TEXT_186 = NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_187 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_188 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_189 = ");" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_190 = " = new MultipleOutputs(job_";
  protected final String TEXT_191 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_192 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_193 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_194 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_195 = " = new ";
  protected final String TEXT_196 = "(); " + NL + "\t\t\t\t\t";
  protected final String TEXT_197 = NL + "\t\t\t\t\t";
  protected final String TEXT_198 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_199 = " = new ";
  protected final String TEXT_200 = "();\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_201 = NL + "\t\t\t\t\t";
  protected final String TEXT_202 = NL + "  \t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void reduce(";
  protected final String TEXT_203 = " key_";
  protected final String TEXT_204 = ", Iterator<";
  protected final String TEXT_205 = "> values_";
  protected final String TEXT_206 = ", " + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_207 = ", WritableComparable> output_";
  protected final String TEXT_208 = ", Reporter reporter_";
  protected final String TEXT_209 = ") throws IOException{" + NL + "\t\t\t\t    final Reporter reporterFinal_";
  protected final String TEXT_210 = "= reporter_";
  protected final String TEXT_211 = ";" + NL + "\t\t\t\t\t";
  protected final String TEXT_212 = NL + "\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_213 = ".close();" + NL + "\t\t\t\t\t";
  protected final String TEXT_214 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_215 = NL + "\t\t\tChainReducer.setReducer(job, ";
  protected final String TEXT_216 = ".class, ";
  protected final String TEXT_217 = ".class," + NL + "        \t\t";
  protected final String TEXT_218 = ".class, ";
  protected final String TEXT_219 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "        \tMultipleOutputs.setWorkDir(job, genTempFolderForComponent(\"MultipleOutputs_";
  protected final String TEXT_220 = "\"));" + NL + "\t\t\t";
  protected final String TEXT_221 = NL + "\t\t\t\tMultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_222 = "\", ";
  protected final String TEXT_223 = ".class, ";
  protected final String TEXT_224 = ".class);" + NL + "        \t";
  protected final String TEXT_225 = NL + "\t\t\tjob.setCombinerClass(";
  protected final String TEXT_226 = ".class);" + NL + "\t\t";
  protected final String TEXT_227 = NL + "\t";
  protected final String TEXT_228 = NL + "\t";
  protected final String TEXT_229 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_230 = "\", 1);" + NL + "\t\t";
  protected final String TEXT_231 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_232 = "\", -1);" + NL + "\t\t";
  protected final String TEXT_233 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_234 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_235 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_236 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_237 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_238 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_239 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t";
  protected final String TEXT_240 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_241 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_242 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_243 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_244 = " && !null2_";
  protected final String TEXT_245 = "){" + NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_246 = " && null2_";
  protected final String TEXT_247 = "){" + NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_248 = " && null2_";
  protected final String TEXT_249 = "){" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_250 = "\t\t" + NL + "            \t";
  protected final String TEXT_251 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_252 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_253 = NL + "\t            \tpos1 += length1_";
  protected final String TEXT_254 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_255 = ";" + NL + "\t            ";
  protected final String TEXT_256 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_257 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_258 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_259 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_260 = NL + "\t                pos1 += (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 4; " + NL + "\t                pos2 += (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 4; " + NL + "\t            ";
  protected final String TEXT_261 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_262 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_263 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_264 = NL + "\t            ";
  protected final String TEXT_265 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_266 = NL + "\t            \tpos1 += 4;" + NL + "\t            \tpos1 += readInt(b1, pos1);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tpos2 += readInt(b2, pos2);" + NL + "\t            ";
  protected final String TEXT_267 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_268 = NL + "\t            ";
  protected final String TEXT_269 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_270 = NL + "\t            ";
  protected final String TEXT_271 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_272 = NL + "\t            ";
  protected final String TEXT_273 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_274 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_275 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_276 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t\tboolean null1_";
  protected final String TEXT_277 = " = length1_";
  protected final String TEXT_278 = " == -1;" + NL + "\t\t\t\t\tboolean null2_";
  protected final String TEXT_279 = " = length2_";
  protected final String TEXT_280 = " == -1;" + NL + "\t\t\t\t";
  protected final String TEXT_281 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_282 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_283 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_284 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_285 = " && !null2_";
  protected final String TEXT_286 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_287 = NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_288 = " && null2_";
  protected final String TEXT_289 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_290 = NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_291 = " && null2_";
  protected final String TEXT_292 = "){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_293 = "\t\t" + NL + "            \t";
  protected final String TEXT_294 = NL + "\t        \t\tif(b1[pos1] > 0 && b2[pos2] ==0){" + NL + "\t        \t\t\t";
  protected final String TEXT_295 = NL + "\t        \t\t}else if(b1[pos1] == 0 && b2[pos2] > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_296 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_297 = NL + "\t                if(b1[pos1] > b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_298 = NL + "\t        \t\t}else if(b1[pos1] < b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_299 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_300 = NL + "\t            \tint n_";
  protected final String TEXT_301 = " = Math.min(length1_";
  protected final String TEXT_302 = ", length2_";
  protected final String TEXT_303 = ");" + NL + "\t            \tfor(int i = 0; i < n_";
  protected final String TEXT_304 = "; i++){" + NL + "\t            \t\tif(b1[pos1+i] > b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_305 = NL + "\t            \t\t}else if(b1[pos1+i] < b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_306 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += length1_";
  protected final String TEXT_307 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_308 = ";" + NL + "\t            \tif(length1_";
  protected final String TEXT_309 = " > length2_";
  protected final String TEXT_310 = "){" + NL + "\t            \t\t";
  protected final String TEXT_311 = NL + "\t            \t}else if(length1_";
  protected final String TEXT_312 = " < length2_";
  protected final String TEXT_313 = "){" + NL + "\t            \t\t";
  protected final String TEXT_314 = NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_315 = NL + "\t            \tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) != 0){" + NL + "\t            \t\tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_316 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_317 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_318 = NL + "\t            \tlong v1_";
  protected final String TEXT_319 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_320 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_321 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_322 = " = FormatterUtils.format_DateInUTC(new java.util.Date(v1_";
  protected final String TEXT_323 = "), ";
  protected final String TEXT_324 = ").compareTo(FormatterUtils.format_DateInUTC(new java.util.Date(v2_";
  protected final String TEXT_325 = "), ";
  protected final String TEXT_326 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_327 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_328 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_329 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_330 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_331 = NL + "\t\t                if(v1_";
  protected final String TEXT_332 = " > v2_";
  protected final String TEXT_333 = "){" + NL + "\t\t                \t";
  protected final String TEXT_334 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_335 = " < v2_";
  protected final String TEXT_336 = "){" + NL + "\t\t                \t";
  protected final String TEXT_337 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_338 = NL + "\t                double v1_";
  protected final String TEXT_339 = " = readDouble(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t                double v2_";
  protected final String TEXT_340 = " = readDouble(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_341 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_342 = " = String.valueOf(v1_";
  protected final String TEXT_343 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_344 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_345 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_346 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_347 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_348 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_349 = NL + "\t\t                if(v1_";
  protected final String TEXT_350 = " > v2_";
  protected final String TEXT_351 = "){" + NL + "\t\t                \t";
  protected final String TEXT_352 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_353 = " < v2_";
  protected final String TEXT_354 = "){" + NL + "\t\t                \t";
  protected final String TEXT_355 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_356 = NL + "\t            \tfloat v1_";
  protected final String TEXT_357 = " = readFloat(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t                float v2_";
  protected final String TEXT_358 = " = readFloat(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_359 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_360 = " = String.valueOf(v1_";
  protected final String TEXT_361 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_362 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_363 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_364 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_365 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_366 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_367 = NL + "\t\t                if(v1_";
  protected final String TEXT_368 = " > v2_";
  protected final String TEXT_369 = "){" + NL + "\t\t                \t";
  protected final String TEXT_370 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_371 = " < v2_";
  protected final String TEXT_372 = "){" + NL + "\t\t                \t";
  protected final String TEXT_373 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_374 = NL + "\t                byte[] bs1_";
  protected final String TEXT_375 = " = new byte[(short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff))];" + NL + "\t                pos1 += 2; " + NL + "\t                for(int i = 0; i < bs1_";
  protected final String TEXT_376 = ".length; i++){" + NL + "\t                \tbs1_";
  protected final String TEXT_377 = "[i] = b1[pos1+i];" + NL + "\t                }" + NL + "\t                pos1 += bs1_";
  protected final String TEXT_378 = ".length;" + NL + "\t                int scale1_";
  protected final String TEXT_379 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 2;" + NL + "\t                java.math.BigDecimal bd1_";
  protected final String TEXT_380 = " = new java.math.BigDecimal(new java.math.BigInteger(bs1_";
  protected final String TEXT_381 = "), scale1_";
  protected final String TEXT_382 = ");" + NL + "\t                " + NL + "\t                byte[] bs2_";
  protected final String TEXT_383 = " = new byte[(short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff))];" + NL + "\t                pos2 += 2; " + NL + "\t                for(int i = 0; i < bs2_";
  protected final String TEXT_384 = ".length; i++){" + NL + "\t                \tbs2_";
  protected final String TEXT_385 = "[i] = b2[pos2+i];" + NL + "\t                }" + NL + "\t                pos2 += bs2_";
  protected final String TEXT_386 = ".length;" + NL + "\t                int scale2_";
  protected final String TEXT_387 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 2;" + NL + "\t                java.math.BigDecimal bd2_";
  protected final String TEXT_388 = " = new java.math.BigDecimal(new java.math.BigInteger(bs2_";
  protected final String TEXT_389 = "), scale2_";
  protected final String TEXT_390 = ");" + NL + "\t                " + NL + "\t                ";
  protected final String TEXT_391 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_392 = " = String.valueOf(bd1_";
  protected final String TEXT_393 = ").compareTo(String.valueOf(bd2_";
  protected final String TEXT_394 = "));" + NL + "\t            \t";
  protected final String TEXT_395 = NL + "\t\t                int cmp_";
  protected final String TEXT_396 = " = bd1_";
  protected final String TEXT_397 = ".compareTo(bd2_";
  protected final String TEXT_398 = ");" + NL + "\t\t            ";
  protected final String TEXT_399 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_400 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_401 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_402 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_403 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_404 = NL + "\t            \tint v1_";
  protected final String TEXT_405 = " = readInt(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t            \tint v2_";
  protected final String TEXT_406 = " = readInt(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_407 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_408 = " = String.valueOf(v1_";
  protected final String TEXT_409 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_410 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_411 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_412 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_413 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_414 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_415 = NL + "\t\t                if(v1_";
  protected final String TEXT_416 = " > v2_";
  protected final String TEXT_417 = "){" + NL + "\t\t                \t";
  protected final String TEXT_418 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_419 = " < v2_";
  protected final String TEXT_420 = "){" + NL + "\t\t                \t";
  protected final String TEXT_421 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_422 = NL + "\t           \t\tlong v1_";
  protected final String TEXT_423 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_424 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t               \t";
  protected final String TEXT_425 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_426 = " = String.valueOf(v1_";
  protected final String TEXT_427 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_428 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_429 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_430 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_431 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_432 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_433 = NL + "\t\t                if(v1_";
  protected final String TEXT_434 = " > v2_";
  protected final String TEXT_435 = "){" + NL + "\t\t                \t";
  protected final String TEXT_436 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_437 = " < v2_";
  protected final String TEXT_438 = "){" + NL + "\t\t                \t";
  protected final String TEXT_439 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_440 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_441 = NL + "\t            ";
  protected final String TEXT_442 = NL + "\t            \tshort v1_";
  protected final String TEXT_443 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t            \tshort v2_";
  protected final String TEXT_444 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            \t";
  protected final String TEXT_445 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_446 = " = String.valueOf(v1_";
  protected final String TEXT_447 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_448 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_449 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_450 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_451 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_452 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_453 = NL + "\t\t            \tif(v1_";
  protected final String TEXT_454 = " > v2_";
  protected final String TEXT_455 = "){" + NL + "\t\t                \t";
  protected final String TEXT_456 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_457 = " < v2_";
  protected final String TEXT_458 = "){" + NL + "\t\t                \t";
  protected final String TEXT_459 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_460 = NL + "\t            \tint len1_";
  protected final String TEXT_461 = " = readInt(b1, pos1);" + NL + "\t            \tpos1 += 4;" + NL + "\t            \tbyte[] bs1_";
  protected final String TEXT_462 = " = new byte[len1_";
  protected final String TEXT_463 = "];" + NL + "\t            \tfor(int i = 0; i < bs1_";
  protected final String TEXT_464 = ".length; i++){" + NL + "\t            \t\tbs1_";
  protected final String TEXT_465 = "[i] = b1[pos1 + i];" + NL + "\t            \t}" + NL + "\t            \tpos1 += bs1_";
  protected final String TEXT_466 = ".length;" + NL + "\t            \tString v1_";
  protected final String TEXT_467 = " = new String(bs1_";
  protected final String TEXT_468 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint len2_";
  protected final String TEXT_469 = " = readInt(b2, pos2);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tbyte[] bs2_";
  protected final String TEXT_470 = " = new byte[len2_";
  protected final String TEXT_471 = "];" + NL + "\t            \tfor(int i = 0; i < bs2_";
  protected final String TEXT_472 = ".length; i++){" + NL + "\t            \t\tbs2_";
  protected final String TEXT_473 = "[i] = b2[pos2 + i];" + NL + "\t            \t}" + NL + "\t            \tpos2 += bs2_";
  protected final String TEXT_474 = ".length;" + NL + "\t            \tString v2_";
  protected final String TEXT_475 = " = new String(bs2_";
  protected final String TEXT_476 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint comp_";
  protected final String TEXT_477 = " = v1_";
  protected final String TEXT_478 = ".compareTo(v2_";
  protected final String TEXT_479 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_480 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_481 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_482 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_483 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_484 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_485 = NL + "\t            ";
  protected final String TEXT_486 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_487 = NL + "\t            ";
  protected final String TEXT_488 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_489 = NL + "\t            ";
  protected final String TEXT_490 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_491 = NL + "\t\t\t\t\tjava.util.Map<String, Integer> compareColumns = new java.util.HashMap<String, Integer>();\t" + NL + "\t\t\t\t";
  protected final String TEXT_492 = NL + "                    \t";
  protected final String TEXT_493 = NL + "                \t";
  protected final String TEXT_494 = NL + "                \t\t";
  protected final String TEXT_495 = NL + "                \t";
  protected final String TEXT_496 = NL + "\t\t\t\t\t\tif(compareColumns.get(\"";
  protected final String TEXT_497 = "\") == null){" + NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_498 = "\") > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_499 = NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_500 = "\") < 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_501 = NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_502 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_503 = " extends WritableComparator{" + NL + "\t\t\t\tint pos1;" + NL + "\t\t\t\tint pos2;" + NL + "\t\t\t\tint comp = 0;" + NL + "\t\t\t\t" + NL + "\t\t\t\tprotected ";
  protected final String TEXT_504 = "(){" + NL + "\t\t\t\t\tsuper(";
  protected final String TEXT_505 = ".class, false);" + NL + "\t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2){" + NL + "\t\t\t\t\ttry{" + NL + "\t\t\t\t\t\tpos1 = s1;" + NL + "\t\t\t\t\t\tpos2 = s2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_506 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_507 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn comp;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t}catch(Exception e){" + NL + "\t\t\t\t\t\tthrow new RuntimeException(e);" + NL + "\t\t\t\t\t}\t\t\t\t\t" + NL + "\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_508 = NL + "\t\t\t\t\tpublic int compare(WritableComparable w1, WritableComparable w2){" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_509 = " k1 = (";
  protected final String TEXT_510 = ")w1;" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_511 = " k2 = (";
  protected final String TEXT_512 = ")w2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_513 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn 0;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_514 = NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_515 = NL + "\t\t\t\tif(k1.";
  protected final String TEXT_516 = " == null && k2.";
  protected final String TEXT_517 = " != null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_518 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_519 = " != null && k2.";
  protected final String TEXT_520 = " == null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_521 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_522 = " == null && k2.";
  protected final String TEXT_523 = " == null){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_524 = "\t\t" + NL + "            \t";
  protected final String TEXT_525 = NL + "\t        \t\tif(k1.";
  protected final String TEXT_526 = " != k2.";
  protected final String TEXT_527 = "){" + NL + "\t        \t\t\tif(k1.";
  protected final String TEXT_528 = "){" + NL + "\t\t        \t\t\t";
  protected final String TEXT_529 = NL + "\t        \t\t\t}else{" + NL + "\t\t        \t\t\t";
  protected final String TEXT_530 = NL + "\t        \t\t\t}" + NL + "\t        \t\t}" + NL + "\t        \t";
  protected final String TEXT_531 = NL + "\t                if(k1.";
  protected final String TEXT_532 = " > k2.";
  protected final String TEXT_533 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_534 = NL + "\t        \t\t}else if(k1.";
  protected final String TEXT_535 = " < k2.";
  protected final String TEXT_536 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_537 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_538 = NL + "\t            \tString s1_";
  protected final String TEXT_539 = " = new String(k1.";
  protected final String TEXT_540 = ");" + NL + "\t            \tString s2_";
  protected final String TEXT_541 = " = new String(k2.";
  protected final String TEXT_542 = ");" + NL + "\t            \tif(!s1_";
  protected final String TEXT_543 = ".equals(s2_";
  protected final String TEXT_544 = ")){" + NL + "\t\t\t\t\t\tif(s1_";
  protected final String TEXT_545 = ".compareTo(s2_";
  protected final String TEXT_546 = ") > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_547 = NL + "\t\t\t\t\t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_548 = NL + "\t\t\t\t\t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_549 = NL + "\t            \tif(k1.";
  protected final String TEXT_550 = " - k2.";
  protected final String TEXT_551 = " != 0){" + NL + "\t            \t\tif(k1.";
  protected final String TEXT_552 = " - k2.";
  protected final String TEXT_553 = " > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_554 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_555 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_556 = NL + "\t                ";
  protected final String TEXT_557 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_558 = " = FormatterUtils.format_DateInUTC(k1.";
  protected final String TEXT_559 = ", ";
  protected final String TEXT_560 = ").compareTo(FormatterUtils.format_DateInUTC(k2.";
  protected final String TEXT_561 = ", ";
  protected final String TEXT_562 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_563 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_564 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_565 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_566 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_567 = NL + "\t            \t\tif(!k1.";
  protected final String TEXT_568 = ".equals(k2.";
  protected final String TEXT_569 = ")){" + NL + "\t\t\t                if(k1.";
  protected final String TEXT_570 = ".compareTo(k2.";
  protected final String TEXT_571 = ") > 0){" + NL + "\t\t\t                \t";
  protected final String TEXT_572 = NL + "\t\t\t                }else{" + NL + "\t\t\t                \t";
  protected final String TEXT_573 = NL + "\t\t\t                }" + NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_574 = NL + "\t                ";
  protected final String TEXT_575 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_576 = " = String.valueOf(k1.";
  protected final String TEXT_577 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_578 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_579 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_580 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_581 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_582 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_583 = NL + "\t\t                if(k1.";
  protected final String TEXT_584 = " > k2.";
  protected final String TEXT_585 = "){" + NL + "\t\t                \t";
  protected final String TEXT_586 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_587 = " < k2.";
  protected final String TEXT_588 = "){" + NL + "\t\t                \t";
  protected final String TEXT_589 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_590 = NL + "\t                ";
  protected final String TEXT_591 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_592 = " = String.valueOf(k1.";
  protected final String TEXT_593 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_594 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_595 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_596 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_597 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_598 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_599 = NL + "\t\t                if(k1.";
  protected final String TEXT_600 = " > k2.";
  protected final String TEXT_601 = "){" + NL + "\t\t                \t";
  protected final String TEXT_602 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_603 = " < k2.";
  protected final String TEXT_604 = "){" + NL + "\t\t                \t";
  protected final String TEXT_605 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_606 = NL + "\t                ";
  protected final String TEXT_607 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_608 = " = String.valueOf(k1.";
  protected final String TEXT_609 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_610 = "));" + NL + "\t            \t";
  protected final String TEXT_611 = NL + "\t\t                int cmp_";
  protected final String TEXT_612 = " = k1.";
  protected final String TEXT_613 = ".compareTo(k2.";
  protected final String TEXT_614 = ");" + NL + "\t\t            ";
  protected final String TEXT_615 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_616 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_617 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_618 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_619 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_620 = NL + "\t                ";
  protected final String TEXT_621 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_622 = " = String.valueOf(k1.";
  protected final String TEXT_623 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_624 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_625 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_626 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_627 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_628 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_629 = NL + "\t\t                if(k1.";
  protected final String TEXT_630 = " > k2.";
  protected final String TEXT_631 = "){" + NL + "\t\t                \t";
  protected final String TEXT_632 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_633 = " < k2.";
  protected final String TEXT_634 = "){" + NL + "\t\t                \t";
  protected final String TEXT_635 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_636 = NL + "\t               \t";
  protected final String TEXT_637 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_638 = " = String.valueOf(k1.";
  protected final String TEXT_639 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_640 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_641 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_642 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_643 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_644 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_645 = NL + "\t\t                if(k1.";
  protected final String TEXT_646 = " > k2.";
  protected final String TEXT_647 = "){" + NL + "\t\t                \t";
  protected final String TEXT_648 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_649 = " < k2.";
  protected final String TEXT_650 = "){" + NL + "\t\t                \t";
  protected final String TEXT_651 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_652 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_653 = NL + "\t            ";
  protected final String TEXT_654 = NL + "\t            \t";
  protected final String TEXT_655 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_656 = " = String.valueOf(k1.";
  protected final String TEXT_657 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_658 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_659 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_660 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_661 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_662 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_663 = NL + "\t\t            \tif(k1.";
  protected final String TEXT_664 = " > k2.";
  protected final String TEXT_665 = "){" + NL + "\t\t                \t";
  protected final String TEXT_666 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_667 = " < k2.";
  protected final String TEXT_668 = "){" + NL + "\t\t                \t";
  protected final String TEXT_669 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_670 = NL + "\t            \tint comp_";
  protected final String TEXT_671 = " = k1.";
  protected final String TEXT_672 = ".compareTo(k2.";
  protected final String TEXT_673 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_674 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_675 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_676 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_677 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_678 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_679 = NL + "\t            ";
  protected final String TEXT_680 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_681 = NL + "\t            ";
  protected final String TEXT_682 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_683 = NL + "\t            ";
  protected final String TEXT_684 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_685 = NL + "                        ";
  protected final String TEXT_686 = ".write";
  protected final String TEXT_687 = "(this.";
  protected final String TEXT_688 = ");";
  protected final String TEXT_689 = NL + "                        if(this.";
  protected final String TEXT_690 = " == null){";
  protected final String TEXT_691 = NL + "                                ";
  protected final String TEXT_692 = ".writeInt(-1);";
  protected final String TEXT_693 = NL + "                                ";
  protected final String TEXT_694 = ".writeByte(-1);";
  protected final String TEXT_695 = NL + "                        }else{";
  protected final String TEXT_696 = NL + "                                ";
  protected final String TEXT_697 = ".writeInt(this.";
  protected final String TEXT_698 = ".length);";
  protected final String TEXT_699 = NL + "                                ";
  protected final String TEXT_700 = ".writeByte(0);";
  protected final String TEXT_701 = NL + "                                ";
  protected final String TEXT_702 = ".writeBoolean(this.";
  protected final String TEXT_703 = ");";
  protected final String TEXT_704 = NL + "                                ";
  protected final String TEXT_705 = ".writeByte(this.";
  protected final String TEXT_706 = ");";
  protected final String TEXT_707 = NL + "                                ";
  protected final String TEXT_708 = ".write(this.";
  protected final String TEXT_709 = ");";
  protected final String TEXT_710 = NL + "                                ";
  protected final String TEXT_711 = ".writeChar(this.";
  protected final String TEXT_712 = ");";
  protected final String TEXT_713 = NL + "                                ";
  protected final String TEXT_714 = ".writeLong(this.";
  protected final String TEXT_715 = ".getTime());";
  protected final String TEXT_716 = NL + "                                ";
  protected final String TEXT_717 = ".writeDouble(this.";
  protected final String TEXT_718 = ");";
  protected final String TEXT_719 = NL + "                                ";
  protected final String TEXT_720 = ".writeFloat(this.";
  protected final String TEXT_721 = ");";
  protected final String TEXT_722 = NL + "                                byte[] bytes_";
  protected final String TEXT_723 = " = this.";
  protected final String TEXT_724 = ".unscaledValue().toByteArray();" + NL + "                                short length_";
  protected final String TEXT_725 = " = (short)bytes_";
  protected final String TEXT_726 = ".length;" + NL + "                                short scale_";
  protected final String TEXT_727 = " = (short)this.";
  protected final String TEXT_728 = ".scale();";
  protected final String TEXT_729 = NL + "                                ";
  protected final String TEXT_730 = ".writeShort(length_";
  protected final String TEXT_731 = ");";
  protected final String TEXT_732 = NL + "                                ";
  protected final String TEXT_733 = ".write(bytes_";
  protected final String TEXT_734 = ");";
  protected final String TEXT_735 = NL + "                                ";
  protected final String TEXT_736 = ".writeShort(scale_";
  protected final String TEXT_737 = ");";
  protected final String TEXT_738 = NL + "                                ";
  protected final String TEXT_739 = ".writeInt(this.";
  protected final String TEXT_740 = ");";
  protected final String TEXT_741 = NL + "                                ";
  protected final String TEXT_742 = ".writeLong(this.";
  protected final String TEXT_743 = ");";
  protected final String TEXT_744 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_745 = NL + "                                ";
  protected final String TEXT_746 = ".writeShort(this.";
  protected final String TEXT_747 = ");";
  protected final String TEXT_748 = NL + "                                byte[] bytes_";
  protected final String TEXT_749 = " = this.";
  protected final String TEXT_750 = ".getBytes(\"UTF-8\");";
  protected final String TEXT_751 = NL + "                                ";
  protected final String TEXT_752 = ".writeInt(bytes_";
  protected final String TEXT_753 = ".length);";
  protected final String TEXT_754 = NL + "                                ";
  protected final String TEXT_755 = ".write(bytes_";
  protected final String TEXT_756 = ");";
  protected final String TEXT_757 = NL + "                                Don't support List type: column--";
  protected final String TEXT_758 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_759 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_760 = NL + "                        }";
  protected final String TEXT_761 = NL + "                        this.";
  protected final String TEXT_762 = " = ";
  protected final String TEXT_763 = ".read";
  protected final String TEXT_764 = "();";
  protected final String TEXT_765 = NL + "                        int length_";
  protected final String TEXT_766 = " = ";
  protected final String TEXT_767 = ".readInt();" + NL + "                        if(length_";
  protected final String TEXT_768 = " == -1){";
  protected final String TEXT_769 = NL + "                        if(";
  protected final String TEXT_770 = ".readByte() == -1){";
  protected final String TEXT_771 = NL + "                            this.";
  protected final String TEXT_772 = " = null;" + NL + "                        }else{";
  protected final String TEXT_773 = NL + "                                this.";
  protected final String TEXT_774 = " = ";
  protected final String TEXT_775 = ".readBoolean();";
  protected final String TEXT_776 = NL + "                                this.";
  protected final String TEXT_777 = " = ";
  protected final String TEXT_778 = ".readByte();";
  protected final String TEXT_779 = NL + "                                this.";
  protected final String TEXT_780 = " = new byte[length_";
  protected final String TEXT_781 = "];";
  protected final String TEXT_782 = NL + "                                ";
  protected final String TEXT_783 = ".readFully(this.";
  protected final String TEXT_784 = ");";
  protected final String TEXT_785 = NL + "                                this.";
  protected final String TEXT_786 = " = ";
  protected final String TEXT_787 = ".readChar();";
  protected final String TEXT_788 = NL + "                                this.";
  protected final String TEXT_789 = " = new java.util.Date(";
  protected final String TEXT_790 = ".readLong());";
  protected final String TEXT_791 = NL + "                                this.";
  protected final String TEXT_792 = " = ";
  protected final String TEXT_793 = ".readDouble();";
  protected final String TEXT_794 = NL + "                                this.";
  protected final String TEXT_795 = " = ";
  protected final String TEXT_796 = ".readFloat();";
  protected final String TEXT_797 = NL + "                                int length_";
  protected final String TEXT_798 = " = ";
  protected final String TEXT_799 = ".readShort();" + NL + "                                byte[] bytes_";
  protected final String TEXT_800 = " = new byte[length_";
  protected final String TEXT_801 = "];";
  protected final String TEXT_802 = NL + "                                ";
  protected final String TEXT_803 = ".readFully(bytes_";
  protected final String TEXT_804 = ");" + NL + "                                int scale_";
  protected final String TEXT_805 = " = ";
  protected final String TEXT_806 = ".readShort();" + NL + "                                this.";
  protected final String TEXT_807 = " = new java.math.BigDecimal(new java.math.BigInteger(bytes_";
  protected final String TEXT_808 = "), scale_";
  protected final String TEXT_809 = ");";
  protected final String TEXT_810 = NL + "                                this.";
  protected final String TEXT_811 = " = ";
  protected final String TEXT_812 = ".readInt();";
  protected final String TEXT_813 = NL + "                                this.";
  protected final String TEXT_814 = " = ";
  protected final String TEXT_815 = ".readLong();";
  protected final String TEXT_816 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_817 = NL + "                                this.";
  protected final String TEXT_818 = " = ";
  protected final String TEXT_819 = ".readShort();";
  protected final String TEXT_820 = NL + "                                int length_";
  protected final String TEXT_821 = " = ";
  protected final String TEXT_822 = ".readInt();" + NL + "                                byte[] bytes_";
  protected final String TEXT_823 = " = new byte[length_";
  protected final String TEXT_824 = "];";
  protected final String TEXT_825 = NL + "                                ";
  protected final String TEXT_826 = ".readFully(bytes_";
  protected final String TEXT_827 = ", 0, length_";
  protected final String TEXT_828 = ");" + NL + "                                this.";
  protected final String TEXT_829 = " = new String(bytes_";
  protected final String TEXT_830 = ", 0, length_";
  protected final String TEXT_831 = ", \"UTF-8\");";
  protected final String TEXT_832 = NL + "                                Don't support List type: column--";
  protected final String TEXT_833 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_834 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_835 = NL + "                        }";
  protected final String TEXT_836 = NL + "            final int prime = 31;" + NL + "            int result = 1;";
  protected final String TEXT_837 = NL + "                            result = prime * result + (this.";
  protected final String TEXT_838 = " ? 1231 : 1237);";
  protected final String TEXT_839 = NL + "                            result = prime * result + (int) this.";
  protected final String TEXT_840 = ";";
  protected final String TEXT_841 = NL + "                        result = prime * result + java.util.Arrays.hashCode(this.";
  protected final String TEXT_842 = ");";
  protected final String TEXT_843 = NL + "                        result = prime * result + ((this.";
  protected final String TEXT_844 = " == null) ? 0 : this.";
  protected final String TEXT_845 = ".hashCode());";
  protected final String TEXT_846 = NL + "            return result;";
  protected final String TEXT_847 = NL + "            if (this == ";
  protected final String TEXT_848 = ") return true;" + NL + "            if (";
  protected final String TEXT_849 = " == null) return false;" + NL + "            if (getClass() != ";
  protected final String TEXT_850 = ".getClass()) return false;" + NL + "            final ";
  protected final String TEXT_851 = " other = (";
  protected final String TEXT_852 = ") ";
  protected final String TEXT_853 = ";";
  protected final String TEXT_854 = NL + "                        if (this.";
  protected final String TEXT_855 = " != other.";
  protected final String TEXT_856 = ")" + NL + "                            return false;";
  protected final String TEXT_857 = NL + "                        if(!java.util.Arrays.equals(this.";
  protected final String TEXT_858 = ", other.";
  protected final String TEXT_859 = ")) {" + NL + "                            return false;" + NL + "                        }";
  protected final String TEXT_860 = NL + "                        if (this.";
  protected final String TEXT_861 = " == null) {" + NL + "                            if (other.";
  protected final String TEXT_862 = " != null)" + NL + "                                return false;" + NL + "                        } else if (!this.";
  protected final String TEXT_863 = ".equals(other.";
  protected final String TEXT_864 = "))" + NL + "                            return false;";
  protected final String TEXT_865 = NL + "            return true;";
  protected final String TEXT_866 = NL + "            int returnValue = -1;";
  protected final String TEXT_867 = NL + "                    returnValue = checkNullsAndCompare(this.";
  protected final String TEXT_868 = ", ";
  protected final String TEXT_869 = ".";
  protected final String TEXT_870 = ");" + NL + "                    if(returnValue != 0) {" + NL + "                        return returnValue;" + NL + "                    }";
  protected final String TEXT_871 = NL + "            return returnValue;";
  protected final String TEXT_872 = NL + "            public static class ";
  protected final String TEXT_873 = " implements ";
  protected final String TEXT_874 = " {" + NL;
  protected final String TEXT_875 = NL + "                    public ";
  protected final String TEXT_876 = " ";
  protected final String TEXT_877 = NL + "                        = ' '";
  protected final String TEXT_878 = ";";
  protected final String TEXT_879 = NL;
  protected final String TEXT_880 = NL + NL + "                public int hashCode() {";
  protected final String TEXT_881 = NL + "                }" + NL + "" + NL + "                public boolean equals(Object obj) {";
  protected final String TEXT_882 = NL + "                }" + NL + "" + NL + "                public String toString() {" + NL + "                    StringBuilder sb = new StringBuilder();" + NL + "                    sb.append(super.toString());" + NL + "                    sb.append(\"[\");";
  protected final String TEXT_883 = NL + "                                sb.append(\"";
  protected final String TEXT_884 = "=\"+";
  protected final String TEXT_885 = ");";
  protected final String TEXT_886 = NL + "                                sb.append(\"";
  protected final String TEXT_887 = "=\"+String.valueOf(";
  protected final String TEXT_888 = "));";
  protected final String TEXT_889 = NL + "                    sb.append(\"]\");" + NL + "" + NL + "                    return sb.toString();" + NL + "                }" + NL + "" + NL + "                public void write(DataOutput out) throws IOException {";
  protected final String TEXT_890 = NL + "                }" + NL + "" + NL + "                public void readFields(DataInput in) throws IOException {";
  protected final String TEXT_891 = NL + "                }" + NL + "" + NL + "                public int compareTo(";
  protected final String TEXT_892 = " other) {";
  protected final String TEXT_893 = NL + "                }" + NL + "" + NL + "                private int checkNullsAndCompare(Object object1, Object object2) {" + NL + "                    int returnValue = 0;" + NL + "                    if (object1 instanceof Comparable && object2 instanceof Comparable) {" + NL + "                        returnValue = ((Comparable) object1).compareTo(object2);" + NL + "                    } else if (object1 != null && object2 != null) {" + NL + "                        returnValue = compareStrings(object1.toString(), object2.toString());" + NL + "                    } else if (object1 == null && object2 != null) {" + NL + "                        returnValue = 1;" + NL + "                    } else if (object1 != null && object2 == null) {" + NL + "                        returnValue = -1;" + NL + "                    } else {" + NL + "                        returnValue = 0;" + NL + "                    }" + NL + "" + NL + "                    return returnValue;" + NL + "                }" + NL + "" + NL + "                private int compareStrings(String string1, String string2) {" + NL + "                    return string1.compareTo(string2);" + NL + "                }" + NL;
  protected final String TEXT_894 = NL + "            }";
  protected final String TEXT_895 = NL + "                static{" + NL + "                    WritableComparator.define(";
  protected final String TEXT_896 = ".class, new ";
  protected final String TEXT_897 = "());" + NL + "                }";
  protected final String TEXT_898 = NL + "\t\t\t\t\t";
  protected final String TEXT_899 = " ";
  protected final String TEXT_900 = " = value_";
  protected final String TEXT_901 = ";";
  protected final String TEXT_902 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_903 = ".";
  protected final String TEXT_904 = " = ";
  protected final String TEXT_905 = ".";
  protected final String TEXT_906 = ";";
  protected final String TEXT_907 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_908 = ".";
  protected final String TEXT_909 = " = ";
  protected final String TEXT_910 = ".";
  protected final String TEXT_911 = ";";
  protected final String TEXT_912 = NL + "    \t\t\t\t\t";
  protected final String TEXT_913 = ".";
  protected final String TEXT_914 = " = key_";
  protected final String TEXT_915 = ".";
  protected final String TEXT_916 = ";";
  protected final String TEXT_917 = NL + "\t\t\t\t\t\t\tjava.util.List<";
  protected final String TEXT_918 = "> ";
  protected final String TEXT_919 = "list= new java.util.ArrayList<";
  protected final String TEXT_920 = ">();";
  protected final String TEXT_921 = NL + "\t\t\t\t\t\t\tStringBuilder ";
  protected final String TEXT_922 = "list = null;";
  protected final String TEXT_923 = NL + "\t\t\t\t\t\t\tjava.util.Set<";
  protected final String TEXT_924 = "> ";
  protected final String TEXT_925 = "set = new java.util.HashSet<";
  protected final String TEXT_926 = ">();";
  protected final String TEXT_927 = NL + "    \t\t\t\twhile(values_";
  protected final String TEXT_928 = ".hasNext()){" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_929 = " ";
  protected final String TEXT_930 = " = values_";
  protected final String TEXT_931 = ".next();" + NL;
  protected final String TEXT_932 = NL + "\t\t\t\t\t\t\t\tif(!";
  protected final String TEXT_933 = "set.contains(";
  protected final String TEXT_934 = ".";
  protected final String TEXT_935 = ")) {";
  protected final String TEXT_936 = NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_937 = "list.add(";
  protected final String TEXT_938 = ".";
  protected final String TEXT_939 = ");";
  protected final String TEXT_940 = NL + "\t\t\t\t\t\t\t\tif(";
  protected final String TEXT_941 = "list == null) {" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_942 = "list  = new StringBuilder();";
  protected final String TEXT_943 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_944 = "list.append(FormatterUtils.format_DateInUTC(";
  protected final String TEXT_945 = ".";
  protected final String TEXT_946 = ", ";
  protected final String TEXT_947 = "));";
  protected final String TEXT_948 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_949 = "list.append(";
  protected final String TEXT_950 = ".";
  protected final String TEXT_951 = ");";
  protected final String TEXT_952 = NL + "\t\t\t\t\t\t\t\t} else {" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_953 = "list.append(";
  protected final String TEXT_954 = ");";
  protected final String TEXT_955 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_956 = "list.append(FormatterUtils.format_DateInUTC(";
  protected final String TEXT_957 = ".";
  protected final String TEXT_958 = ", ";
  protected final String TEXT_959 = "));";
  protected final String TEXT_960 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_961 = "list.append(";
  protected final String TEXT_962 = ".";
  protected final String TEXT_963 = ");";
  protected final String TEXT_964 = NL + "\t\t\t\t\t\t\t\t}";
  protected final String TEXT_965 = NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_966 = "set.add(";
  protected final String TEXT_967 = ".";
  protected final String TEXT_968 = ");" + NL + "\t\t\t\t\t\t\t\t}";
  protected final String TEXT_969 = NL + "    \t\t\t\t}";
  protected final String TEXT_970 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_971 = ".";
  protected final String TEXT_972 = " = ";
  protected final String TEXT_973 = "list;";
  protected final String TEXT_974 = NL + "\t\t\t\t\t\t\tif(";
  protected final String TEXT_975 = "list!=null) {" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_976 = ".";
  protected final String TEXT_977 = " = ";
  protected final String TEXT_978 = "list.toString();" + NL + "\t\t\t\t\t\t\t}";
  protected final String TEXT_979 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
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
	
    stringBuffer.append(TEXT_119);
    stringBuffer.append(TEXT_120);
    
	//need to define MapperHelper before MapperGenerator, so use this trick
	class ReducerHelperBase{
		public void reduce(){
		}
		
		public void prepare(){
		}
		
		public void configure(){
		}
		
		public void close(){
		}
	}
	
	class ReducerGenerator{
		ReducerHelperBase reducer;
		
		String cid = null;
		String reducerClass = null;

		Object inKey = null;
		Object inKeyClass = null;

		Object inValue = null;
		Object inValueClass = null;

		Object outKey = null;
		Object outKeyClass = null;

		Object outValue = null;
		Object outValueClass = null;
		
		public ReducerGenerator(ReducerHelperBase reducer){
			this.reducer = reducer; 
		}
		
		public void init(String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			this.cid = cid;
			this.inKey = (inKey == null ? "key_"+cid : inKey);
			this.inValue = (inValue == null ? "values_"+cid : inValue);
			this.outKey = (outKey == null ? "outputKey_"+cid : outKey);
			this.outValue = (outValue == null ? "outputValue_"+cid : outValue);
			this.reducerClass = buildClassName(cid, "r");
			this.inKeyClass = buildClassName(inKey, "row");
			this.inValueClass = buildClassName(inValue, "row");
			this.outKeyClass = buildClassName(outKey, "row");
			this.outValueClass = buildClassName(outValue, "row");
		}
		
		protected String buildClassName(String name, String type){
			if(type.equals("m")){
				return name + "Mapper";
			}else if(type.equals("r")){
				return name + "Reducer";
			}else if(type.equals("c")){
				return name + "Combiner";
			}else if(type.equals("row")){
				return name + "Struct";
			}else{
				return null;
			}
		}
		
		protected Object buildClassName(Object name, String type){
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
			
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_124);
    
		}
		public void generate(){
		
    stringBuffer.append(TEXT_125);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_134);
    reducer.prepare();
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_139);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_140);
    }else{
    stringBuffer.append(TEXT_141);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_143);
    }
    stringBuffer.append(TEXT_144);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_146);
    reducer.configure();
    stringBuffer.append(TEXT_147);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    reducer.reduce();
    stringBuffer.append(TEXT_160);
    reducer.close();
    stringBuffer.append(TEXT_161);
    
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_162);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_167);
    
		}
	} 
	
	class MOReducerGenerator extends ReducerGenerator{
		public MOReducerGenerator(ReducerHelperBase reducer){
			super(reducer); 
		}
		
		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_173);
    
		}
		public void generate(){
		
    stringBuffer.append(TEXT_174);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_179);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_181);
    
				if(outValueClass instanceof java.util.Map){
					for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
					
    stringBuffer.append(TEXT_182);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_183);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_184);
    	
					}
				}
				
    stringBuffer.append(TEXT_185);
    reducer.prepare();
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_192);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_193);
    }else{
    stringBuffer.append(TEXT_194);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_196);
    }
    stringBuffer.append(TEXT_197);
    
					if(outValueClass instanceof java.util.Map){
						for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
						
    stringBuffer.append(TEXT_198);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_199);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_200);
    	
						}
					}
					
    stringBuffer.append(TEXT_201);
    reducer.configure();
    stringBuffer.append(TEXT_202);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    reducer.reduce();
    stringBuffer.append(TEXT_212);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_213);
    reducer.close();
    stringBuffer.append(TEXT_214);
    
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_215);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_220);
    
        	java.util.Map<String, String> values = (java.util.Map<String, String>)outValue;
        	for(String key : values.keySet()){
        	
    stringBuffer.append(TEXT_221);
    stringBuffer.append(values.get(key));
    stringBuffer.append(TEXT_222);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(getOutValueClass(key));
    stringBuffer.append(TEXT_224);
    
        	}
		}
	}
	
	class CombinerGenerator extends ReducerGenerator{
		public CombinerGenerator(ReducerHelperBase reducer){
			super(reducer); 
		}
		public void init(String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			super.init(cid, inKey, inValue, outKey, outValue);
			this.reducerClass = buildClassName(cid, "c");
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_225);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_226);
    			
		}
	}
	
	final String R_TYPE_BASE = "base";
	final String R_TYPE_MO = "mo";
	final String R_TYPE_COMBINER = "combiner";
	
	class ReducerHelper extends ReducerHelperBase{
		
		ReducerGenerator reducerGen;
		
		String cid = null;
		
		public void setType(String type){
			if(type.equals(R_TYPE_BASE)){
				reducerGen = new ReducerGenerator(this);
			}else if(type.equals(R_TYPE_MO)){
				reducerGen = new MOReducerGenerator(this);
			}else if(type.equals(R_TYPE_COMBINER)){
				reducerGen = new CombinerGenerator(this);
			}
		}
		
		public void init(String cid, String inKey, String inValue, String outKey, Object outValue){
			if(reducerGen == null){
				reducerGen = new ReducerGenerator(this);
			}
			reducerGen.init(cid, inKey, inValue, outKey, outValue);
			this.cid = reducerGen.cid;
		}
		
		public String getInKeyClass(String name){
			return reducerGen.getInKeyClass(name);
		}
		
		public String getInKeyClass(){
			return reducerGen.getInKeyClass();
		}
		
		public String getInKey(String name){
			return reducerGen.getInKey(name);
		}
		
		public String getInKey(){
			return reducerGen.getInKey();
		}
		
		public String getOutKeyClass(String name){
			return reducerGen.getOutKeyClass(name);
		}
		
		public String getOutKeyClass(){
			return reducerGen.getOutKeyClass();
		}
		
		public String getOutKey(String name){
			return reducerGen.getOutKey(name);
		}
		
		public String getOutKey(){
			return reducerGen.getOutKey();
		}
		
		public String getInValueClass(String name){
			return reducerGen.getInValueClass(name);
		}
		
		public String getInValueClass(){
			return reducerGen.getInValueClass();
		}
		
		public String getInValue(String name){
			return reducerGen.getInValue(name);
		}
		
		public String getInValue(){
			return reducerGen.getInValue();
		}
		
		public String getOutValueClass(String name){
			return reducerGen.getOutValueClass(name);
		}
		
		public String getOutValueClass(){
			return reducerGen.getOutValueClass();
		}
		
		public String getOutValue(String name){
			return reducerGen.getOutValue(name);
		}
		
		public String getOutValue(){
			return reducerGen.getOutValue();
		}
		
		public void output(String outKey, String outValue){
			reducerGen.output(outKey, outValue);
		}
		
		public void generate(){
			reducerGen.generate();
		}
		
		public void generateConf(){
			reducerGen.generateConf();
		}
		
		public void reduce(){
		}
		
		public void prepare(){
		}
		
		public void configure(){
		}
		
		public void close(){
		}
	}
	
    stringBuffer.append(TEXT_227);
    stringBuffer.append(TEXT_228);
    
	class ComparatorHelper{
		String className;
		List<IMetadataColumn> columns;
		String structName;
		Map<String, Boolean> criterias;
		List<String> orders;
		boolean unorder = false;
		Map<String, Boolean> sortTypes; // false means sort by alpha
		boolean genObjectCompare = false;
		
		public void init(String className, List<IMetadataColumn> columns, String structName){
			this.className = className;
			this.columns = columns;
			this.structName = structName;
			orders = new java.util.ArrayList<String>();
			criterias = new java.util.HashMap<String, Boolean>();
			sortTypes = new java.util.HashMap<String, Boolean>();
			for(IMetadataColumn column : columns){
				orders.add(column.getLabel());
				criterias.put(column.getLabel(), true);
				sortTypes.put(column.getLabel(), true);
			}
		}
		public void setGenObjectCompare(Boolean genObjectCompare){
			this.genObjectCompare = genObjectCompare;
		}
		//true is asc
		//false is desc
		public void setCriterias(Map<String,Boolean> criterias){
			this.criterias = criterias;
		}
		
		public void setOrders(List<String> orders){
			this.orders = orders;
			int index = 0;
			for(String order : orders){
				for(int i = 0; i < columns.size(); i++){
					if(order.equals(columns.get(i).getLabel())){
						if(i < index){
							unorder = true;
						}else{
							index = i;
						}
						break;
					}
				}
				if(unorder){
					break;
				}
			}
		}
		
		public void setSortTypes(Map<String, Boolean> sortTypes){
			this.sortTypes = sortTypes;
		}
		
		private void greater(String columnName){
			if(unorder){
				addGreater(columnName);
			}else{
				genGreater(columnName);
			}
		}
		private void lesser(String columnName){
			if(unorder){
				addLesser(columnName);
			}else{
				genLesser(columnName);
			}			
		}
		private void addGreater(String columnName){
		
    stringBuffer.append(TEXT_229);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_230);
    
		}
		private void addLesser(String columnName){
		
    stringBuffer.append(TEXT_231);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_232);
    	
		}
		private void genGreater(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_233);
    	
			}else{
			
    stringBuffer.append(TEXT_234);
    
			}
		}
		private void genLesser(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_235);
    	
			}else{
			
    stringBuffer.append(TEXT_236);
    
			}
		}
		
		private void skipColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
    stringBuffer.append(TEXT_237);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_238);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_239);
    
				}else{
	       		
    stringBuffer.append(TEXT_240);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_242);
    
	        	}
				
    stringBuffer.append(TEXT_243);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_244);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_249);
    }
    stringBuffer.append(TEXT_250);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_251);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_252);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_253);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_255);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_256);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	            
    stringBuffer.append(TEXT_257);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_258);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_259);
    
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_260);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_261);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_262);
    
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_263);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_264);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_265);
    
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_266);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_267);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_268);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_269);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_270);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_271);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_272);
    
	            }
				
    if(nullable){
    stringBuffer.append(TEXT_273);
    
        	}
		}
		
		private void compareColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
    stringBuffer.append(TEXT_274);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_275);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_280);
    
				}else{
	       		
    stringBuffer.append(TEXT_281);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_283);
    
	        	}
				
    stringBuffer.append(TEXT_284);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_286);
    lesser(columnName);
    stringBuffer.append(TEXT_287);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_288);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_289);
    greater(columnName);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_292);
    }
    stringBuffer.append(TEXT_293);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_294);
    greater(columnName);
    stringBuffer.append(TEXT_295);
    lesser(columnName);
    stringBuffer.append(TEXT_296);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_297);
    greater(columnName);
    stringBuffer.append(TEXT_298);
    lesser(columnName);
    stringBuffer.append(TEXT_299);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_300);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_301);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_302);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_303);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_304);
    greater(columnName);
    stringBuffer.append(TEXT_305);
    lesser(columnName);
    stringBuffer.append(TEXT_306);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_307);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_309);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_310);
    greater(columnName);
    stringBuffer.append(TEXT_311);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_313);
    lesser(columnName);
    stringBuffer.append(TEXT_314);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_315);
    greater(columnName);
    stringBuffer.append(TEXT_316);
    lesser(columnName);
    stringBuffer.append(TEXT_317);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_318);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_319);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_320);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_321);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_322);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_323);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_324);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_326);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_327);
    greater(columnName);
    stringBuffer.append(TEXT_328);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_329);
    lesser(columnName);
    stringBuffer.append(TEXT_330);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_331);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_332);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_333);
    greater(columnName);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_336);
    lesser(columnName);
    stringBuffer.append(TEXT_337);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_338);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_339);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_340);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_341);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_343);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_344);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_345);
    greater(columnName);
    stringBuffer.append(TEXT_346);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_347);
    lesser(columnName);
    stringBuffer.append(TEXT_348);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_349);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_350);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_351);
    greater(columnName);
    stringBuffer.append(TEXT_352);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_353);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_354);
    lesser(columnName);
    stringBuffer.append(TEXT_355);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_356);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_357);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_358);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_359);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_361);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_362);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_363);
    greater(columnName);
    stringBuffer.append(TEXT_364);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_365);
    lesser(columnName);
    stringBuffer.append(TEXT_366);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_367);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_369);
    greater(columnName);
    stringBuffer.append(TEXT_370);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_371);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_372);
    lesser(columnName);
    stringBuffer.append(TEXT_373);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_374);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_375);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_376);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_377);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_378);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_379);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_380);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_381);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_382);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_383);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_384);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_385);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_386);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_387);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_388);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_389);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_390);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_391);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_392);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_393);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_394);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_395);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_396);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_397);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_398);
    }
    stringBuffer.append(TEXT_399);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_400);
    greater(columnName);
    stringBuffer.append(TEXT_401);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_402);
    lesser(columnName);
    stringBuffer.append(TEXT_403);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_404);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_405);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_406);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_407);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_408);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_409);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_410);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_411);
    greater(columnName);
    stringBuffer.append(TEXT_412);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_413);
    lesser(columnName);
    stringBuffer.append(TEXT_414);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_415);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_416);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_417);
    greater(columnName);
    stringBuffer.append(TEXT_418);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_419);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_420);
    lesser(columnName);
    stringBuffer.append(TEXT_421);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_422);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_423);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_424);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_425);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_426);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_427);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_428);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_429);
    greater(columnName);
    stringBuffer.append(TEXT_430);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_431);
    lesser(columnName);
    stringBuffer.append(TEXT_432);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_433);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_434);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_435);
    greater(columnName);
    stringBuffer.append(TEXT_436);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_437);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_438);
    lesser(columnName);
    stringBuffer.append(TEXT_439);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_440);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_441);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_442);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_443);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_444);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_445);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_446);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_447);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_448);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_449);
    greater(columnName);
    stringBuffer.append(TEXT_450);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_451);
    lesser(columnName);
    stringBuffer.append(TEXT_452);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_453);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_454);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_455);
    greater(columnName);
    stringBuffer.append(TEXT_456);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_457);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_458);
    lesser(columnName);
    stringBuffer.append(TEXT_459);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_460);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_461);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_462);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_463);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_464);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_465);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_466);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_467);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_468);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_469);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_470);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_471);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_472);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_473);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_474);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_475);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_476);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_477);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_478);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_479);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_480);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_481);
    greater(columnName);
    stringBuffer.append(TEXT_482);
    lesser(columnName);
    stringBuffer.append(TEXT_483);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_484);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_485);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_486);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_487);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_488);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_489);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_490);
    
        	}
		}
		
		public void compareColumns(){
			if(columns != null){
				if(unorder){
				
    stringBuffer.append(TEXT_491);
    
				}
	            for(IMetadataColumn column : columns){
	            	String columnName = column.getLabel();
                	if(orders.contains(columnName)){
                	
    stringBuffer.append(TEXT_492);
    compareColumn(column);
    stringBuffer.append(TEXT_493);
    
                	}else{
                	
    stringBuffer.append(TEXT_494);
    skipColumn(column);
    stringBuffer.append(TEXT_495);
    	
	               	}
				}
				if(unorder){
					for(String columnName : orders){
					
    stringBuffer.append(TEXT_496);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_497);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_498);
    genGreater(columnName);
    stringBuffer.append(TEXT_499);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_500);
    genLesser(columnName);
    stringBuffer.append(TEXT_501);
    	
					}
				}
			}
		}
		
		public void compareAfterColumns(){
		}
		
		public void generateCode(){
		
    stringBuffer.append(TEXT_502);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_503);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_504);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_505);
    compareColumns();
    stringBuffer.append(TEXT_506);
    compareAfterColumns();
    stringBuffer.append(TEXT_507);
    if(genObjectCompare){
    stringBuffer.append(TEXT_508);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_509);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_510);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_511);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_512);
    
						for(IMetadataColumn column : columns){
			            	String columnName = column.getLabel();
		                	compareObjectColumn(column);
						}
						
    stringBuffer.append(TEXT_513);
    }
    stringBuffer.append(TEXT_514);
    
		}
		
		private void compareObjectColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
			
    stringBuffer.append(TEXT_515);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_516);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_517);
    lesser(columnName);
    stringBuffer.append(TEXT_518);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_519);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_520);
    greater(columnName);
    stringBuffer.append(TEXT_521);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_522);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_523);
    }
    stringBuffer.append(TEXT_524);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_525);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_526);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_527);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_528);
    greater(columnName);
    stringBuffer.append(TEXT_529);
    lesser(columnName);
    stringBuffer.append(TEXT_530);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_531);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_532);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_533);
    greater(columnName);
    stringBuffer.append(TEXT_534);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_535);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_536);
    lesser(columnName);
    stringBuffer.append(TEXT_537);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_538);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_539);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_540);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_541);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_542);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_543);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_544);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_545);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_546);
    greater(columnName);
    stringBuffer.append(TEXT_547);
    lesser(columnName);
    stringBuffer.append(TEXT_548);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_549);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_550);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_551);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_552);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_553);
    greater(columnName);
    stringBuffer.append(TEXT_554);
    lesser(columnName);
    stringBuffer.append(TEXT_555);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_556);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_557);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_558);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_559);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_560);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_561);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_562);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_563);
    greater(columnName);
    stringBuffer.append(TEXT_564);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_565);
    lesser(columnName);
    stringBuffer.append(TEXT_566);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_567);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_568);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_569);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_570);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_571);
    greater(columnName);
    stringBuffer.append(TEXT_572);
    lesser(columnName);
    stringBuffer.append(TEXT_573);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_574);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_575);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_576);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_577);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_578);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_579);
    greater(columnName);
    stringBuffer.append(TEXT_580);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_581);
    lesser(columnName);
    stringBuffer.append(TEXT_582);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_583);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_584);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_585);
    greater(columnName);
    stringBuffer.append(TEXT_586);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_587);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_588);
    lesser(columnName);
    stringBuffer.append(TEXT_589);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_590);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_591);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_592);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_593);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_594);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_595);
    greater(columnName);
    stringBuffer.append(TEXT_596);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_597);
    lesser(columnName);
    stringBuffer.append(TEXT_598);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_599);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_600);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_601);
    greater(columnName);
    stringBuffer.append(TEXT_602);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_603);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_604);
    lesser(columnName);
    stringBuffer.append(TEXT_605);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_606);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_607);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_608);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_609);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_610);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_611);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_612);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_613);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_614);
    }
    stringBuffer.append(TEXT_615);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_616);
    greater(columnName);
    stringBuffer.append(TEXT_617);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_618);
    lesser(columnName);
    stringBuffer.append(TEXT_619);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_620);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_621);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_622);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_623);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_624);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_625);
    greater(columnName);
    stringBuffer.append(TEXT_626);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_627);
    lesser(columnName);
    stringBuffer.append(TEXT_628);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_629);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_630);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_631);
    greater(columnName);
    stringBuffer.append(TEXT_632);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_633);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_634);
    lesser(columnName);
    stringBuffer.append(TEXT_635);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_636);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_637);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_638);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_639);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_640);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_641);
    greater(columnName);
    stringBuffer.append(TEXT_642);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_643);
    lesser(columnName);
    stringBuffer.append(TEXT_644);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_645);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_646);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_647);
    greater(columnName);
    stringBuffer.append(TEXT_648);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_649);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_650);
    lesser(columnName);
    stringBuffer.append(TEXT_651);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_652);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_653);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_654);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_655);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_656);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_657);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_658);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_659);
    greater(columnName);
    stringBuffer.append(TEXT_660);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_661);
    lesser(columnName);
    stringBuffer.append(TEXT_662);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_663);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_664);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_665);
    greater(columnName);
    stringBuffer.append(TEXT_666);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_667);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_668);
    lesser(columnName);
    stringBuffer.append(TEXT_669);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_670);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_671);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_672);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_673);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_674);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_675);
    greater(columnName);
    stringBuffer.append(TEXT_676);
    lesser(columnName);
    stringBuffer.append(TEXT_677);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_678);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_679);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_680);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_681);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_682);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_683);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_684);
    
        	}
		}
	}
	
    
    class StructHelper{
        String structName;
        List<IMetadataColumn> columns;
        java.util.Set<IMetadataColumn> columnsNonSerialized;
        String implementsClasses;
        final boolean genComparator;

        /**
         * The comparator should usually only be generated if the structure is
         * being used as an input KEY into a reduce step.
         */
        public StructHelper(String structName, List<IMetadataColumn> columns, boolean genComparator, java.util.Set<IMetadataColumn> columnsNonSerialized) {
            this.structName = structName + "Struct";
            this.columns = columns;
            this.columnsNonSerialized = columnsNonSerialized;
            this.implementsClasses = "WritableComparable<"+this.structName+">";
            this.genComparator = genComparator;
        }

        public StructHelper(String structName, List<IMetadataColumn> columns, boolean genComparator) {
            this.structName = structName + "Struct";
            this.columns = columns;
            this.implementsClasses = "WritableComparable<"+this.structName+">";
            this.genComparator = genComparator;
        }

        public String getStructName(){
            return structName;
        }

        public void appendImplClasses(String className){
            this.implementsClasses += ",";
            this.implementsClasses += className;
        }

        public void overrideWrite(String dataOutput){
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    if (columnsNonSerialized != null && columnsNonSerialized.contains(columns))
                        continue;
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());

                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                        typeToGenerate=typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
                        
    stringBuffer.append(TEXT_685);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_686);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_687);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_688);
    
                    } else{
                        
    stringBuffer.append(TEXT_689);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_690);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_691);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_692);
    
                            }else{
                            
    stringBuffer.append(TEXT_693);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_694);
    
                            }
                            
    stringBuffer.append(TEXT_695);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_696);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_697);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_698);
    
                            }else{
                            
    stringBuffer.append(TEXT_699);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_700);
    
                            }
                            
    
                            if(typeToGenerate.equals("Boolean")) {
                            
    stringBuffer.append(TEXT_701);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_702);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_703);
    
                            } else if(typeToGenerate.equals("Byte")) {
                            
    stringBuffer.append(TEXT_704);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_705);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_706);
    
                            } else if(typeToGenerate.equals("byte[]")) {
                            
    stringBuffer.append(TEXT_707);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_708);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_709);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")) {
                            
    stringBuffer.append(TEXT_710);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_711);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_712);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_713);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_714);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_715);
    
                            } else if(typeToGenerate.equals("Double")) {
                            
    stringBuffer.append(TEXT_716);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_717);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_718);
    
                            } else if(typeToGenerate.equals("Float")) {
                            
    stringBuffer.append(TEXT_719);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_720);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_721);
    
                            } else if(typeToGenerate.equals("BigDecimal")) {
                            
    stringBuffer.append(TEXT_722);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_723);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_724);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_725);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_726);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_727);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_728);
    stringBuffer.append(TEXT_729);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_730);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_731);
    stringBuffer.append(TEXT_732);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_733);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_734);
    stringBuffer.append(TEXT_735);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_736);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_737);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_738);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_739);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_740);
    
                            } else if(typeToGenerate.equals("Long")) {
                            
    stringBuffer.append(TEXT_741);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_742);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_743);
    
                            } else if(typeToGenerate.equals("Object")) {
                            
    stringBuffer.append(TEXT_744);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")) {
                            
    stringBuffer.append(TEXT_745);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_746);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_747);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_748);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_749);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_750);
    stringBuffer.append(TEXT_751);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_752);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_753);
    stringBuffer.append(TEXT_754);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_755);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_756);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_757);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Document")) {
                            
    stringBuffer.append(TEXT_758);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")) {
                            
    stringBuffer.append(TEXT_759);
    stringBuffer.append(column.getLabel());
    
                            }
                               
    stringBuffer.append(TEXT_760);
    
                    }
                }
            }
        }

        public void overrideReadFields(String dataInput){
            if (columns != null) {
                for (IMetadataColumn column: columns) {
                    if (columnsNonSerialized != null && columnsNonSerialized.contains(columns))
                        continue;
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());

                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                        typeToGenerate=typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
                        
    stringBuffer.append(TEXT_761);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_762);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_763);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_764);
    
                    }else{
                        if(typeToGenerate.equals("byte[]")){
                        
    stringBuffer.append(TEXT_765);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_766);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_767);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_768);
    
                        }else{
                           
    stringBuffer.append(TEXT_769);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_770);
    
                        }
                        
    stringBuffer.append(TEXT_771);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_772);
    
                            if(typeToGenerate.equals("Boolean")){
                            
    stringBuffer.append(TEXT_773);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_774);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_775);
    
                            } else if(typeToGenerate.equals("Byte")){
                            
    stringBuffer.append(TEXT_776);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_777);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_778);
    
                            } else if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_779);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_780);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_781);
    stringBuffer.append(TEXT_782);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_783);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_784);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")){
                            
    stringBuffer.append(TEXT_785);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_786);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_787);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_788);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_789);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_790);
    
                            } else if(typeToGenerate.equals("Double")){
                            
    stringBuffer.append(TEXT_791);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_792);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_793);
    
                            } else if(typeToGenerate.equals("Float")){
                            
    stringBuffer.append(TEXT_794);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_795);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_796);
    
                            } else if(typeToGenerate.equals("BigDecimal")){
                            
    stringBuffer.append(TEXT_797);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_798);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_799);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_800);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_801);
    stringBuffer.append(TEXT_802);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_803);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_804);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_805);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_806);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_807);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_808);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_809);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_810);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_811);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_812);
    
                            } else if(typeToGenerate.equals("Long")){
                            
    stringBuffer.append(TEXT_813);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_814);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_815);
    
                            } else if(typeToGenerate.equals("Object")){
                            
    stringBuffer.append(TEXT_816);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")){
                            
    stringBuffer.append(TEXT_817);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_818);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_819);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_820);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_821);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_822);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_823);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_824);
    stringBuffer.append(TEXT_825);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_826);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_827);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_828);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_829);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_830);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_831);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_832);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Doucument")) {
                            
    stringBuffer.append(TEXT_833);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")){
                            
    stringBuffer.append(TEXT_834);
    stringBuffer.append(column.getLabel());
    
                            }
                            
    stringBuffer.append(TEXT_835);
    
                    }
                }
            }
        }

        public void overrideHashCode(){
        
    stringBuffer.append(TEXT_836);
    
            if (columns != null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                         String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                         if(javaType == JavaTypesManager.BOOLEAN) {
                        
    stringBuffer.append(TEXT_837);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_838);
    
                        } else {
                        
    stringBuffer.append(TEXT_839);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_840);
    
                        }
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                        
    stringBuffer.append(TEXT_841);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_842);
    
                    } else {
                    
    stringBuffer.append(TEXT_843);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_844);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_845);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_846);
    
        }

        public void overrideEquals(String objName){
        
    stringBuffer.append(TEXT_847);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_848);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_849);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_850);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_851);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_852);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_853);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                    
    stringBuffer.append(TEXT_854);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_855);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_856);
    
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                    
    stringBuffer.append(TEXT_857);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_858);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_859);
    
                    } else {
                    
    stringBuffer.append(TEXT_860);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_861);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_862);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_863);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_864);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_865);
    
        }

        public void overrideCompareTo(String otherName){
            //int returnValue = 0; ?
            
    stringBuffer.append(TEXT_866);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                
    stringBuffer.append(TEXT_867);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_868);
    stringBuffer.append(otherName);
    stringBuffer.append(TEXT_869);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_870);
    
                }
            }
            
    stringBuffer.append(TEXT_871);
    
        }

        public void addMethods(){
        }

        public void declareVars(){
        }

        public void generateCode(){
        
    stringBuffer.append(TEXT_872);
    stringBuffer.append(structName );
    stringBuffer.append(TEXT_873);
    stringBuffer.append(implementsClasses);
    stringBuffer.append(TEXT_874);
    
                if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    
    stringBuffer.append(TEXT_875);
    stringBuffer.append( JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()) );
    stringBuffer.append(TEXT_876);
    stringBuffer.append(column.getLabel() );
    
                        if(javaType == JavaTypesManager.CHARACTER && !column.isNullable()) {
                        
    stringBuffer.append(TEXT_877);
    
                        }
                        
    stringBuffer.append(TEXT_878);
    
                    }
                }
                
    stringBuffer.append(TEXT_879);
    declareVars();
    stringBuffer.append(TEXT_880);
    overrideHashCode();
    stringBuffer.append(TEXT_881);
    overrideEquals("obj");
    stringBuffer.append(TEXT_882);
    
                    if (columns !=null) {
                        for(int i=0; i< columns.size(); i++) {
                            IMetadataColumn column = columns.get(i);
                            String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                            if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_883);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_884);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_885);
    
                            }else{
                            
    stringBuffer.append(TEXT_886);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_887);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_888);
    
                            }
                        }
                    }
                    
    stringBuffer.append(TEXT_889);
    overrideWrite("out");
    stringBuffer.append(TEXT_890);
    overrideReadFields("in");
    stringBuffer.append(TEXT_891);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_892);
    overrideCompareTo("other");
    stringBuffer.append(TEXT_893);
    addMethods();
    stringBuffer.append(TEXT_894);
    
            if(genComparator){
                ComparatorHelper StructComparator = new ComparatorHelper();
                StructComparator.init(structName + "_Comparator", columns, structName);
                StructComparator.generateCode();

                
    stringBuffer.append(TEXT_895);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_896);
    stringBuffer.append(structName + "_Comparator");
    stringBuffer.append(TEXT_897);
    
            }
        }
    }
    
    
    BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();

    List<IMetadataTable> metadatas = node.getMetadataList();
    if ((metadatas!=null)&&(metadatas.size()>0)) {
        IMetadataTable metadata = metadatas.get(0);
        List<? extends IConnection> inConns = node.getIncomingConnections();
        List<? extends IConnection> outConns = node.getOutgoingConnections();
        IMetadataTable inMetadata = null;

        String inConnName = null;
        String outConnName = null;

        if(inConns != null){
        	for (IConnection conn : inConns) {
    			if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
    				inConnName = conn.getName();
    				inMetadata = conn.getMetadataTable();
        			break;
    			}
    		}
    	}

    	if(outConns!=null) {
    		for(IConnection conn : outConns){
        		if(conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){
        			outConnName = conn.getName();
        			break;
        		}
    		}
		}

    	if (metadata != null && inMetadata != null) {
        	final List<IMetadataColumn> keyColumns = new ArrayList<IMetadataColumn>();
    		final List<IMetadataColumn> valueColumns = new ArrayList<IMetadataColumn>();
    		List<String> valueColumnNames = new ArrayList<String>();

    		List<Map<String, String>> denormalizes = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__DENORMALIZE_COLUMNS__");

			List<IMetadataColumn> inColumns = inMetadata.getListColumns();
   			List<IMetadataColumn> outColumns = metadata.getListColumns();

    		final Map<String, String> outTypesMap = new HashMap<String, String>();
    		final Map<String, String> delimiterMap = new HashMap<String, String>();
    		final Map<String, Boolean> mergeMap = new HashMap<String, Boolean>();
    		final Map<String, String> patternsMap = new HashMap<String, String>();

        	for(IMetadataColumn outColumn : outColumns){
        		String type = JavaTypesManager.getTypeToGenerate(outColumn.getTalendType(), true);
        		outTypesMap.put(outColumn.getLabel(), type);
        	}

	        for(Map<String, String> denormalize : denormalizes){
        		String columnName = denormalize.get("INPUT_COLUMN");
            	if(valueColumnNames.contains(columnName)){
            		continue;
            	}
            	valueColumnNames.add(columnName);
            	delimiterMap.put(columnName,denormalize.get("DELIMITER"));
            	mergeMap.put(columnName,("true").equals(denormalize.get("MERGE")));
   			}

            for(IMetadataColumn column : inColumns){
            	String columnName = column.getLabel();
            	if(valueColumnNames.contains(columnName)){
            		valueColumns.add(column);
	    			String pattern = ((column.getPattern() == null) || (column.getPattern().trim().length() == 0)) ? null : column.getPattern();
					patternsMap.put(columnName, pattern);
            	} else {
            		keyColumns.add(column);
            	}
            }

    		//struct init
    		StructHelper keyStruct = new StructHelper("mOutKey_"+cid, keyColumns, true);
        	keyStruct.generateCode();

			StructHelper rowStruct = new StructHelper("mOutValue_"+cid, valueColumns, false);
			rowStruct.generateCode();

        	//map init
    		class Mapper extends MapperHelper{

				public void map(){

    stringBuffer.append(TEXT_898);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_899);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_900);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_901);
    
        			for(IMetadataColumn outputCol : valueColumns){
        				String columnName = outputCol.getLabel();

    stringBuffer.append(TEXT_902);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_903);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_904);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_905);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_906);
    
					}
        			for(IMetadataColumn outputCol : keyColumns){
        				String columnName = outputCol.getLabel();

    stringBuffer.append(TEXT_907);
    stringBuffer.append(getOutKey());
    stringBuffer.append(TEXT_908);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_909);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_910);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_911);
    
					}
					this.output(getOutKey(), getOutValue());
				}
			}

        	Mapper mapper = new Mapper();
        	mapper.init(node, cid, null, inConnName, "mOutKey_"+cid, "mOutValue_"+cid);
        	mapper.generate();

        	//reduce init
        	class Reducer extends ReducerHelper{

        		public void reduce(){
					for(IMetadataColumn column : keyColumns) {
						String columnName = column.getLabel();

    stringBuffer.append(TEXT_912);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_913);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_914);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_915);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_916);
    
					}

					for(IMetadataColumn column : valueColumns) {
						String columnName = column.getLabel();
						String type = outTypesMap.get(columnName);
						Boolean removeDuplicate = mergeMap.get(columnName);

						String intype = JavaTypesManager.getTypeToGenerate(column.getTalendType(), true);

    					if("List".equals(type)){

    stringBuffer.append(TEXT_917);
    stringBuffer.append(intype );
    stringBuffer.append(TEXT_918);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_919);
    stringBuffer.append(intype );
    stringBuffer.append(TEXT_920);
    
    					} else {

    stringBuffer.append(TEXT_921);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_922);
    
    					}

    					if(removeDuplicate) {

    stringBuffer.append(TEXT_923);
    stringBuffer.append(intype );
    stringBuffer.append(TEXT_924);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_925);
    stringBuffer.append(intype );
    stringBuffer.append(TEXT_926);
    
    					}
					}

    stringBuffer.append(TEXT_927);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_928);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_929);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_930);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_931);
    
						for(IMetadataColumn column : valueColumns) {
							String columnName = column.getLabel();
							String type = outTypesMap.get(columnName);
							String delimiter = delimiterMap.get(columnName);
    						Boolean removeDuplicate = mergeMap.get(columnName);
    						String pattern = patternsMap.get(columnName);

    						String intype = JavaTypesManager.getTypeToGenerate(column.getTalendType(), true);

    						if(removeDuplicate) {

    stringBuffer.append(TEXT_932);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_933);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_934);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_935);
    
    						}

    						if("List".equals(type)){

    stringBuffer.append(TEXT_936);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_937);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_938);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_939);
    
    						} else {

    stringBuffer.append(TEXT_940);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_941);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_942);
    
								if("java.util.Date".equals(intype) && pattern!=null) {

    stringBuffer.append(TEXT_943);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_944);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_945);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_946);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_947);
    
								} else {

    stringBuffer.append(TEXT_948);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_949);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_950);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_951);
    
								}

    stringBuffer.append(TEXT_952);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_953);
    stringBuffer.append(delimiter);
    stringBuffer.append(TEXT_954);
    
								if("java.util.Date".equals(intype) && pattern!=null) {

    stringBuffer.append(TEXT_955);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_956);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_957);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_958);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_959);
    
								} else {

    stringBuffer.append(TEXT_960);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_961);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_962);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_963);
    
								}

    stringBuffer.append(TEXT_964);
    
    						}

    						if(removeDuplicate) {

    stringBuffer.append(TEXT_965);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_966);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_967);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_968);
    
    						}
						}

    stringBuffer.append(TEXT_969);
    
					for(IMetadataColumn column : valueColumns) {
						String columnName = column.getLabel();
						String type = outTypesMap.get(columnName);
						if("List".equals(type)){

    stringBuffer.append(TEXT_970);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_971);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_972);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_973);
    
						} else {

    stringBuffer.append(TEXT_974);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_975);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_976);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_977);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_978);
    
						}
					}

        			this.output(null, getOutValue());
        		}
        	}

    		Reducer reducer = new Reducer();
        	reducer.init(cid, "mOutKey_"+cid, "mOutValue_"+cid, null, outConnName);
        	reducer.generate();

	    }
    }

    stringBuffer.append(TEXT_979);
    return stringBuffer.toString();
  }
}
