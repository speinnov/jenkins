package org.talend.designer.codegen.translators.processing.hadoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.pigmap.PigMapComponent;
import org.talend.designer.pigmap.model.emf.pigmap.PigMapData;
import org.talend.designer.pigmap.model.emf.pigmap.InputTable;
import org.talend.designer.pigmap.model.emf.pigmap.OutputTable;
import org.talend.designer.pigmap.model.emf.pigmap.TableNode;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.utils.NodeUtil;

public class TPigMapMainJava {

  protected static String nl;
  public static synchronized TPigMapMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TPigMapMainJava result = new TPigMapMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t";
  protected final String TEXT_3 = NL + "\t";
  protected final String TEXT_4 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_5 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_7 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_8 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_10 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_11 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_13 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_14 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_16 = " - Written records count: \" + nb_line_";
  protected final String TEXT_17 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_19 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_20 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_21 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_22 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_24 = " - Writing the record \" + nb_line_";
  protected final String TEXT_25 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_26 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_27 = " - Processing the record \" + nb_line_";
  protected final String TEXT_28 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_29 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_30 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_31 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_32 = "\t" + NL + "\t" + NL + "\troutines.system.PigHelper helper_";
  protected final String TEXT_33 = " = new routines.system.PigHelper();";
  protected final String TEXT_34 = NL + "\t\t\tStringBuilder sb_";
  protected final String TEXT_35 = " = new StringBuilder();" + NL + "\t\t\t//the bag name of main input bag" + NL + "\t\t\thelper_";
  protected final String TEXT_36 = ".add(\"query\",\"";
  protected final String TEXT_37 = "_";
  protected final String TEXT_38 = "_RESULT = FOREACH ";
  protected final String TEXT_39 = "_";
  protected final String TEXT_40 = "_RESULT GENERATE *;\");" + NL + "\t\t\thelper_";
  protected final String TEXT_41 = ".add(\"query\",\"";
  protected final String TEXT_42 = "_RESULT = FOREACH ";
  protected final String TEXT_43 = "_";
  protected final String TEXT_44 = "_RESULT GENERATE *;\");" + NL + "\t\t\t" + NL + "\t\t\tStringBuilder sb_innerJoinReject_";
  protected final String TEXT_45 = " = new StringBuilder();";
  protected final String TEXT_46 = NL + "\t\t//get the scripts in the lookup process" + NL + "\t\tjava.util.List<String[]> ps_";
  protected final String TEXT_47 = " = (java.util.List<String[]>)globalMap.get(\"";
  protected final String TEXT_48 = "\");" + NL + "\t\t" + NL + "\t\tfor(int i_";
  protected final String TEXT_49 = "=0; i_";
  protected final String TEXT_50 = "<ps_";
  protected final String TEXT_51 = ".size(); i_";
  protected final String TEXT_52 = "++) {" + NL + "\t\t\tString[] o_";
  protected final String TEXT_53 = " = ps_";
  protected final String TEXT_54 = ".get(i_";
  protected final String TEXT_55 = ");" + NL + "\t\t\tif(o_";
  protected final String TEXT_56 = ".length > 2 && o_";
  protected final String TEXT_57 = "[2]!=null) {" + NL + "\t\t\t\thelper_";
  protected final String TEXT_58 = ".add(o_";
  protected final String TEXT_59 = "[0],o_";
  protected final String TEXT_60 = "[1],o_";
  protected final String TEXT_61 = "[2]);" + NL + "\t\t\t} else {" + NL + "\t\t\t\thelper_";
  protected final String TEXT_62 = ".add(o_";
  protected final String TEXT_63 = "[0],o_";
  protected final String TEXT_64 = "[1]);" + NL + "\t\t\t}" + NL + "\t\t\t" + NL + "\t\t\tif(i_";
  protected final String TEXT_65 = " == (ps_";
  protected final String TEXT_66 = ".size() - 1)) {//get the bag name of current lookup bag" + NL + "\t\t\t\tString bagName_";
  protected final String TEXT_67 = " = o_";
  protected final String TEXT_68 = "[1];" + NL + "\t\t\t\tbagName_";
  protected final String TEXT_69 = " = bagName_";
  protected final String TEXT_70 = ".substring(0,bagName_";
  protected final String TEXT_71 = ".indexOf(\"_RESULT\")+7);" + NL + "\t\t\t\thelper_";
  protected final String TEXT_72 = ".add(\"query\",\"";
  protected final String TEXT_73 = "_";
  protected final String TEXT_74 = "_RESULT = FOREACH \" + bagName_";
  protected final String TEXT_75 = " + \" GENERATE *;\");" + NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_76 = NL + "\t\t\tsb_";
  protected final String TEXT_77 = ".append(\"";
  protected final String TEXT_78 = "_RESULT = JOIN ";
  protected final String TEXT_79 = "_RESULT BY (\");";
  protected final String TEXT_80 = NL + "\t\t\thelper_";
  protected final String TEXT_81 = ".add(\"query\",\"";
  protected final String TEXT_82 = "_";
  protected final String TEXT_83 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_84 = "_RESULT GENERATE *;\");" + NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_85 = ".append(\"";
  protected final String TEXT_86 = "_";
  protected final String TEXT_87 = "_INNER_REJECT_RESULT = JOIN ";
  protected final String TEXT_88 = "_";
  protected final String TEXT_89 = "_INNER_REJECT_RESULT BY (\");";
  protected final String TEXT_90 = NL + "\t\t\t\tsb_";
  protected final String TEXT_91 = ".append(";
  protected final String TEXT_92 = ");";
  protected final String TEXT_93 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_94 = ".append(";
  protected final String TEXT_95 = ");\t\t\t\t";
  protected final String TEXT_96 = NL + "\t\t\t\tsb_";
  protected final String TEXT_97 = ".append(\",\");";
  protected final String TEXT_98 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_99 = ".append(\",\");\t\t\t\t";
  protected final String TEXT_100 = NL + "\t\t\tsb_";
  protected final String TEXT_101 = ".append(\") ";
  protected final String TEXT_102 = ", ";
  protected final String TEXT_103 = "_";
  protected final String TEXT_104 = "_RESULT BY (\");";
  protected final String TEXT_105 = NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_106 = ".append(\") LEFT, ";
  protected final String TEXT_107 = "_";
  protected final String TEXT_108 = "_RESULT BY (\");\t\t\t\t";
  protected final String TEXT_109 = NL + "\t\t\t\tsb_";
  protected final String TEXT_110 = ".append(\"";
  protected final String TEXT_111 = "\");";
  protected final String TEXT_112 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_113 = ".append(\"";
  protected final String TEXT_114 = "\");\t\t\t\t";
  protected final String TEXT_115 = NL + "\t\t\t\tsb_";
  protected final String TEXT_116 = ".append(\",\");";
  protected final String TEXT_117 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_118 = ".append(\",\");\t\t\t\t";
  protected final String TEXT_119 = NL + "\t\t\tsb_";
  protected final String TEXT_120 = ".append(\")\");";
  protected final String TEXT_121 = NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_122 = ".append(\")\");\t\t\t\t";
  protected final String TEXT_123 = NL + "\t\t\tsb_";
  protected final String TEXT_124 = ".append(\" USING '";
  protected final String TEXT_125 = "'\");";
  protected final String TEXT_126 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_127 = ".append(\" USING '";
  protected final String TEXT_128 = "'\");\t\t\t\t";
  protected final String TEXT_129 = NL + "\t\t\tsb_";
  protected final String TEXT_130 = ".append(\" PARTITION BY \").append(";
  protected final String TEXT_131 = ");";
  protected final String TEXT_132 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_133 = ".append(\" PARTITION BY \").append(";
  protected final String TEXT_134 = ");\t\t\t\t";
  protected final String TEXT_135 = NL + "\t\t\tsb_";
  protected final String TEXT_136 = ".append(\" PARALLEL \").append(";
  protected final String TEXT_137 = ");";
  protected final String TEXT_138 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_139 = ".append(\" PARALLEL \").append(";
  protected final String TEXT_140 = ");\t\t\t\t";
  protected final String TEXT_141 = NL + "\t\t\tsb_";
  protected final String TEXT_142 = ".append(\";\");" + NL + "\t\t\thelper_";
  protected final String TEXT_143 = ".add(\"query\",sb_";
  protected final String TEXT_144 = ".toString());" + NL + "\t\t\tsb_";
  protected final String TEXT_145 = ".setLength(0);";
  protected final String TEXT_146 = NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_147 = ".append(\";\");" + NL + "\t\t\thelper_";
  protected final String TEXT_148 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_149 = ".toString());" + NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_150 = ".setLength(0);" + NL + "\t\t\t" + NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_151 = ".append(\"";
  protected final String TEXT_152 = "_";
  protected final String TEXT_153 = "_INNER_REJECT_RESULT = FILTER ";
  protected final String TEXT_154 = "_";
  protected final String TEXT_155 = "_INNER_REJECT_RESULT BY \");";
  protected final String TEXT_156 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_157 = ".append(\"(";
  protected final String TEXT_158 = " is null)\");";
  protected final String TEXT_159 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_160 = ".append(\" OR \");";
  protected final String TEXT_161 = NL + "    \t\tsb_innerJoinReject_";
  protected final String TEXT_162 = ".append(\";\");" + NL + "    \t\thelper_";
  protected final String TEXT_163 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_164 = ".toString());" + NL + "    \t\tsb_innerJoinReject_";
  protected final String TEXT_165 = ".setLength(0);\t\t\t\t\t\t\t";
  protected final String TEXT_166 = NL + "\t\t\thelper_";
  protected final String TEXT_167 = ".add(\"query\",\"";
  protected final String TEXT_168 = "_";
  protected final String TEXT_169 = "_FILTER_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_170 = "_RESULT GENERATE *;\");" + NL + "\t\t\thelper_";
  protected final String TEXT_171 = ".add(\"query\",\"";
  protected final String TEXT_172 = "_";
  protected final String TEXT_173 = "_FILTER_INNER_REJECT_RESULT = FILTER ";
  protected final String TEXT_174 = "_";
  protected final String TEXT_175 = "_FILTER_INNER_REJECT_RESULT BY (NOT (\"+";
  protected final String TEXT_176 = "+\"));\");" + NL + "\t\t\t//need to set the lookup columns as null" + NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_177 = ".append(\"";
  protected final String TEXT_178 = "_";
  protected final String TEXT_179 = "_FILTER_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_180 = "_";
  protected final String TEXT_181 = "_FILTER_INNER_REJECT_RESULT GENERATE \");";
  protected final String TEXT_182 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_183 = ".append(\"$";
  protected final String TEXT_184 = ",\");";
  protected final String TEXT_185 = NL + "\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_186 = ".append(\"null AS column";
  protected final String TEXT_187 = ":";
  protected final String TEXT_188 = "\");";
  protected final String TEXT_189 = NL + "\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_190 = ".append(\",\");";
  protected final String TEXT_191 = NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_192 = ".append(\";\");" + NL + "        \t\thelper_";
  protected final String TEXT_193 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_194 = ".toString());" + NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_195 = ".setLength(0);";
  protected final String TEXT_196 = NL + "\t\t\t\t//init inner reject resultset" + NL + "\t\t\t\thelper_";
  protected final String TEXT_197 = ".add(\"query\",\"";
  protected final String TEXT_198 = "_INNER_REJECT_RESULT = UNION ";
  protected final String TEXT_199 = "_";
  protected final String TEXT_200 = "_INNER_REJECT_RESULT,";
  protected final String TEXT_201 = "_";
  protected final String TEXT_202 = "_FILTER_INNER_REJECT_RESULT;\");";
  protected final String TEXT_203 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_204 = ".append(\"";
  protected final String TEXT_205 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_206 = "_INNER_REJECT_RESULT GENERATE \");";
  protected final String TEXT_207 = NL + "\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_208 = ".append(\"$";
  protected final String TEXT_209 = ",\");";
  protected final String TEXT_210 = NL + "\t\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_211 = ".append(\"null AS ";
  protected final String TEXT_212 = ":";
  protected final String TEXT_213 = "\");";
  protected final String TEXT_214 = NL + "\t\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_215 = ".append(\",\");";
  protected final String TEXT_216 = NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_217 = ".append(\";\");" + NL + "        \t\t//syn the schema" + NL + "        \t\thelper_";
  protected final String TEXT_218 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_219 = ".toString());" + NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_220 = ".setLength(0);" + NL + "\t\t\t\t//union" + NL + "\t\t\t\thelper_";
  protected final String TEXT_221 = ".add(\"query\",\"";
  protected final String TEXT_222 = "_INNER_REJECT_RESULT = UNION ";
  protected final String TEXT_223 = "_INNER_REJECT_RESULT,";
  protected final String TEXT_224 = "_";
  protected final String TEXT_225 = "_INNER_REJECT_RESULT,";
  protected final String TEXT_226 = "_";
  protected final String TEXT_227 = "_FILTER_INNER_REJECT_RESULT;\");";
  protected final String TEXT_228 = NL + "\t\t\thelper_";
  protected final String TEXT_229 = ".add(\"query\",\"";
  protected final String TEXT_230 = "_RESULT = FILTER ";
  protected final String TEXT_231 = "_RESULT BY \"+";
  protected final String TEXT_232 = "+\";\");";
  protected final String TEXT_233 = NL + "\t\t\t\t//init inner reject resultset" + NL + "\t\t\t\thelper_";
  protected final String TEXT_234 = ".add(\"query\",\"";
  protected final String TEXT_235 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_236 = "_";
  protected final String TEXT_237 = "_INNER_REJECT_RESULT GENERATE *;\");";
  protected final String TEXT_238 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_239 = ".append(\"";
  protected final String TEXT_240 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_241 = "_INNER_REJECT_RESULT GENERATE \");";
  protected final String TEXT_242 = NL + "\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_243 = ".append(\"$";
  protected final String TEXT_244 = ",\");";
  protected final String TEXT_245 = NL + "\t\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_246 = ".append(\"null AS ";
  protected final String TEXT_247 = ":";
  protected final String TEXT_248 = "\");";
  protected final String TEXT_249 = NL + "\t\t\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_250 = ".append(\",\");";
  protected final String TEXT_251 = NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_252 = ".append(\";\");" + NL + "        \t\t//syn the schema" + NL + "        \t\thelper_";
  protected final String TEXT_253 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_254 = ".toString());" + NL + "        \t\tsb_innerJoinReject_";
  protected final String TEXT_255 = ".setLength(0);" + NL + "\t\t\t\t//union" + NL + "\t\t\t\thelper_";
  protected final String TEXT_256 = ".add(\"query\",\"";
  protected final String TEXT_257 = "_INNER_REJECT_RESULT = UNION ";
  protected final String TEXT_258 = "_INNER_REJECT_RESULT,";
  protected final String TEXT_259 = "_";
  protected final String TEXT_260 = "_INNER_REJECT_RESULT;\");";
  protected final String TEXT_261 = NL + "\t\tsb_innerJoinReject_";
  protected final String TEXT_262 = ".append(\"";
  protected final String TEXT_263 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_264 = "_INNER_REJECT_RESULT GENERATE \");";
  protected final String TEXT_265 = NL + "\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_266 = ".append(\"$";
  protected final String TEXT_267 = ",\");";
  protected final String TEXT_268 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_269 = ".append(\"null AS ";
  protected final String TEXT_270 = ":";
  protected final String TEXT_271 = "\");";
  protected final String TEXT_272 = NL + "\t\t\t\t\tsb_innerJoinReject_";
  protected final String TEXT_273 = ".append(\",\");";
  protected final String TEXT_274 = NL + "    \t\tsb_innerJoinReject_";
  protected final String TEXT_275 = ".append(\";\");" + NL + "    \t\t//syn the schema" + NL + "    \t\thelper_";
  protected final String TEXT_276 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_277 = ".toString());" + NL + "    \t\tsb_innerJoinReject_";
  protected final String TEXT_278 = ".setLength(0);";
  protected final String TEXT_279 = NL + "\t\thelper_";
  protected final String TEXT_280 = ".add(\"query\",\"";
  protected final String TEXT_281 = "_";
  protected final String TEXT_282 = "_RESULT = FOREACH ";
  protected final String TEXT_283 = "_RESULT GENERATE *;\");";
  protected final String TEXT_284 = NL + "\t\thelper_";
  protected final String TEXT_285 = ".add(\"query\",\"";
  protected final String TEXT_286 = "_";
  protected final String TEXT_287 = "_INNER_REJECT_RESULT = FOREACH ";
  protected final String TEXT_288 = "_INNER_REJECT_RESULT GENERATE *;\");";
  protected final String TEXT_289 = NL + "\t\t\tsb_";
  protected final String TEXT_290 = ".append(\"";
  protected final String TEXT_291 = "_";
  protected final String TEXT_292 = "_RESULT = FILTER ";
  protected final String TEXT_293 = "_";
  protected final String TEXT_294 = "_RESULT BY \");";
  protected final String TEXT_295 = NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_296 = ".append(\"";
  protected final String TEXT_297 = "_";
  protected final String TEXT_298 = "_INNER_REJECT_RESULT = FILTER ";
  protected final String TEXT_299 = "_";
  protected final String TEXT_300 = "_INNER_REJECT_RESULT BY \");";
  protected final String TEXT_301 = NL + "\t\t\tsb_";
  protected final String TEXT_302 = ".append(";
  protected final String TEXT_303 = ");";
  protected final String TEXT_304 = NL + "\t\t\tsb_innerJoinReject_";
  protected final String TEXT_305 = ".append(";
  protected final String TEXT_306 = ");";
  protected final String TEXT_307 = NL + "\t\t\tsb_";
  protected final String TEXT_308 = ".append(\" AND \");";
  protected final String TEXT_309 = NL + "\t\t\tsb_";
  protected final String TEXT_310 = ".append(\"(NOT (\");";
  protected final String TEXT_311 = NL + "\t\t\t\tsb_";
  protected final String TEXT_312 = ".append(";
  protected final String TEXT_313 = ");";
  protected final String TEXT_314 = NL + "\t\t\t\tsb_";
  protected final String TEXT_315 = ".append(\" OR \");";
  protected final String TEXT_316 = NL + "\t\t\tsb_";
  protected final String TEXT_317 = ".append(\"))\");";
  protected final String TEXT_318 = NL + "\t\t\tsb_";
  protected final String TEXT_319 = ".append(\" AND \");";
  protected final String TEXT_320 = NL + "\t\t\tsb_";
  protected final String TEXT_321 = ".append(\"(1!=1)\");";
  protected final String TEXT_322 = NL + "\t\tsb_";
  protected final String TEXT_323 = ".append(\";\");" + NL + "\t\thelper_";
  protected final String TEXT_324 = ".add(\"query\",sb_";
  protected final String TEXT_325 = ".toString());" + NL + "\t\tsb_";
  protected final String TEXT_326 = ".setLength(0);";
  protected final String TEXT_327 = NL + "\t\tsb_innerJoinReject_";
  protected final String TEXT_328 = ".append(\";\");" + NL + "\t\thelper_";
  protected final String TEXT_329 = ".add(\"query\",sb_innerJoinReject_";
  protected final String TEXT_330 = ".toString());" + NL + "\t\tsb_innerJoinReject_";
  protected final String TEXT_331 = ".setLength(0);";
  protected final String TEXT_332 = NL + "\t\t\thelper_";
  protected final String TEXT_333 = ".add(\"query\",\"";
  protected final String TEXT_334 = "_";
  protected final String TEXT_335 = "_RESULT = UNION ";
  protected final String TEXT_336 = "_";
  protected final String TEXT_337 = "_RESULT, ";
  protected final String TEXT_338 = "_";
  protected final String TEXT_339 = "_INNER_REJECT_RESULT;\");";
  protected final String TEXT_340 = NL + "\t\t\thelper_";
  protected final String TEXT_341 = ".add(\"query\",\"";
  protected final String TEXT_342 = "_";
  protected final String TEXT_343 = "_RESULT = FOREACH ";
  protected final String TEXT_344 = "_";
  protected final String TEXT_345 = "_INNER_REJECT_RESULT GENERATE *;\");";
  protected final String TEXT_346 = NL + "\t\tsb_";
  protected final String TEXT_347 = ".append(\"";
  protected final String TEXT_348 = "_";
  protected final String TEXT_349 = "_RESULT = FOREACH ";
  protected final String TEXT_350 = "_";
  protected final String TEXT_351 = "_RESULT GENERATE \");";
  protected final String TEXT_352 = NL + "\t\t\tsb_";
  protected final String TEXT_353 = ".append(";
  protected final String TEXT_354 = ");";
  protected final String TEXT_355 = NL + "\t\t\tsb_";
  protected final String TEXT_356 = ".append(\",\");";
  protected final String TEXT_357 = NL + "\t\tsb_";
  protected final String TEXT_358 = ".append(\";\");" + NL + "\t\thelper_";
  protected final String TEXT_359 = ".add(\"query\",sb_";
  protected final String TEXT_360 = ".toString());" + NL + "\t\tsb_";
  protected final String TEXT_361 = ".setLength(0);";
  protected final String TEXT_362 = NL;
  protected final String TEXT_363 = NL + "\tjava.util.List<String[]> pigLatins_";
  protected final String TEXT_364 = " = helper_";
  protected final String TEXT_365 = ".getPigLatins();";
  protected final String TEXT_366 = NL + "\t\tfor(String[] pigLatin_";
  protected final String TEXT_367 = " : pigLatins_";
  protected final String TEXT_368 = ") {" + NL + "\t\t\tString type_";
  protected final String TEXT_369 = " = pigLatin_";
  protected final String TEXT_370 = "[0];" + NL + "\t\t\t";
  protected final String TEXT_371 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_372 = " - register \" + type_";
  protected final String TEXT_373 = " + \" : \" + pigLatin_";
  protected final String TEXT_374 = "[1]);" + NL + "\t\t\t";
  protected final String TEXT_375 = NL + "\t\t\tif(\"query\".equals(type_";
  protected final String TEXT_376 = ")) {";
  protected final String TEXT_377 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_378 = ".write(pigLatin_";
  protected final String TEXT_379 = "[1]);";
  protected final String TEXT_380 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_381 = ".registerQuery(pigLatin_";
  protected final String TEXT_382 = "[1]);";
  protected final String TEXT_383 = NL + "\t\t\t} else if(\"jar\".equals(type_";
  protected final String TEXT_384 = ")) {";
  protected final String TEXT_385 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_386 = ".write(\"REGISTER \" + wasbPath_";
  protected final String TEXT_387 = " + new java.io.File(pigLatin_";
  protected final String TEXT_388 = "[1]).getName() + \";\");" + NL + "\t\t\t\t\tlibjars_";
  protected final String TEXT_389 = ".append(pigLatin_";
  protected final String TEXT_390 = "[1] + \",\");";
  protected final String TEXT_391 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_392 = ".registerJar(pigLatin_";
  protected final String TEXT_393 = "[1]);";
  protected final String TEXT_394 = NL + "\t\t\t} else if(\"script\".equals(type_";
  protected final String TEXT_395 = ")) {";
  protected final String TEXT_396 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_397 = ".write(pigLatin_";
  protected final String TEXT_398 = "[1]);";
  protected final String TEXT_399 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_400 = ".registerScript(new java.io.ByteArrayInputStream(pigLatin_";
  protected final String TEXT_401 = "[1].getBytes()));";
  protected final String TEXT_402 = NL + "\t\t\t} else if(\"function\".equals(type_";
  protected final String TEXT_403 = ")) {";
  protected final String TEXT_404 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_405 = ".write(\"DEFINE \" + pigLatin_";
  protected final String TEXT_406 = "[1] + \" \" + pigLatin_";
  protected final String TEXT_407 = "[2] + \";\");";
  protected final String TEXT_408 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_409 = ".registerFunction(pigLatin_";
  protected final String TEXT_410 = "[1], new org.apache.pig.FuncSpec(pigLatin_";
  protected final String TEXT_411 = "[2]));";
  protected final String TEXT_412 = NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_413 = NL + "    \tpigScript_";
  protected final String TEXT_414 = ".addAll(pigLatins_";
  protected final String TEXT_415 = ");";
  protected final String TEXT_416 = NL + "\tpigLatins_";
  protected final String TEXT_417 = ".clear();";
  protected final String TEXT_418 = NL;

    private Map<String,Pattern> cache = new HashMap<String,Pattern>();
    
	public String convertExpression(Map<String,String> expressionConvertMap, String expression) {

		if(expression == null || "".equals(expression.trim())) {
			return "";
		}
		
		for(Map.Entry<String, String> entry : expressionConvertMap.entrySet()) {
			String key = entry.getKey();//like row1.column
			String value = entry.getValue();//like $0
			
            Pattern p = cache.get(key);
            
            if(p == null) {
                int index = key.indexOf(".");
                String tableName = key.substring(0, index);
                String columnName = key.substring(index + 1);
                
                StringBuffer pattern = new StringBuffer();
                pattern.append(tableName).append("\\s*\\.\\s*").append(columnName);
                p = Pattern.compile(pattern.toString());
                cache.put(key, p);
            }
            
            Matcher m = p.matcher(expression);

            StringBuffer sb = new StringBuffer();

            if (m.find()) {
                do {
                    if (m.start() > 0) {
                        char c = expression.charAt(m.start() - 1);
                        if (Character.isLetterOrDigit(c) || (c == '_')) {
                            continue;
                        }
                    }
                    
                    if (m.end() < expression.length()) {
                        char c = expression.charAt(m.end());
                        if (Character.isLetterOrDigit(c) || (c == '_')) {
                            continue;
                        }
                    }

                    m.appendReplacement(sb,value);
                } while (m.find());

                m.appendTail(sb);
                expression = sb.toString();
            }
			
		}
		
		return expression;
	
	}
	
   public static String getPigType(String talendType) {
        String typeToGenerate = JavaTypesManager.getTypeToGenerate(talendType, false);
        // Integers: int(Integer), long(Long), short(Short), byte(Byte), BigDecimal, double(Double).
        if (typeToGenerate.equals("int") || typeToGenerate.equals("short") || typeToGenerate.equals("byte")) { 
            return "int"; 
        } else if (typeToGenerate.equals("long")) { 
            return "long"; 
        } else if (typeToGenerate.equals("BigDecimal") || typeToGenerate.equals("double")) { 
            return "double";
        }
    
        // Floats: float(Float)
        if (typeToGenerate.equals("float")) { 
            return "float";
        }
    
        // others treat as string
        return "chararray"; 

   }

    public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	PigMapComponent node = (PigMapComponent) codeGenArgument.getArgument();
	PigMapData data = (PigMapData)ElementParameterParser.getObjectValueXMLTree(node);
	
	String cid = node.getUniqueName();
	
	List<InputTable> inputTablesWithUnvalid = data.getInputTables();
	List<OutputTable> outputTablesWithUnvalid = data.getOutputTables();
	
	List<IConnection> inputConnections = (List<IConnection>)node.getIncomingConnections();
	
	HashMap<String, IConnection> hNameToConnection = new HashMap<String, IConnection>();
	for(IConnection connection : inputConnections){
		hNameToConnection.put(connection.getName(), connection);
	}
	
	List<InputTable> inputTables = new ArrayList<InputTable>();
	for(int i=0; i<inputTablesWithUnvalid.size(); i++){
		InputTable  currentTable = inputTablesWithUnvalid.get(i);
		if(hNameToConnection.get(currentTable.getName()) != null){
			inputTables.add(currentTable);
		}
	}
	
	String previous_node="";
	String start_node="";

	boolean inMain = true;
	boolean isExecutedThroughWebHCat = false;

	Map<String,String> expressionConvertMap = new HashMap<String,String>();//like that : row1.id ==> $0
	int index = 0;
	
	//check whether reject or inner reject exist in output tables
	List<IConnection> outputConnections = (List<IConnection>)node.getOutgoingConnections();
	
	HashMap<String, IConnection> hNameToOutConnection = new HashMap<String, IConnection>();
	for(IConnection connection : outputConnections){
		hNameToOutConnection.put(connection.getName(), connection);
	}
	
	List<OutputTable> outputTables = new ArrayList<OutputTable>();
	for(int i=0; i<outputTablesWithUnvalid.size(); i++){
		OutputTable currentTable = outputTablesWithUnvalid.get(i);
		if(hNameToOutConnection.get(currentTable.getName()) != null){
			outputTables.add(currentTable);
		}
	}
	
	boolean innerReject = false;
	for(OutputTable outputTable : outputTables) {
		if(outputTable.isRejectInnerJoin()) {
			innerReject = true;
		}
	}

    stringBuffer.append(TEXT_2);
    stringBuffer.append(TEXT_3);
    
	//this util class use by set log4j debug paramters
	class DefaultLog4jFileUtil {
	
		INode node = null;
	    String cid = null;
 		boolean isLog4jEnabled = false;
 		String label = null;
 		
 		public DefaultLog4jFileUtil(){
 		}
 		public DefaultLog4jFileUtil(INode node) {
 			this.node = node;
 			this.cid = node.getUniqueName();
 			this.label = cid;
			this.isLog4jEnabled = ("true").equals(org.talend.core.model.process.ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
 		}
 		
 		public void setCid(String cid) {
 			this.cid = cid;
 		}
 		
		//for all tFileinput* components 
		public void startRetriveDataInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_4);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_5);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_6);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_9);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_12);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_20);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_22);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_23);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_26);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_29);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_31);
    
			}
		}
	}
	
	final DefaultLog4jFileUtil log4jFileUtil = new DefaultLog4jFileUtil((INode)(((org.talend.designer.codegen.config.CodeGeneratorArgument)argument).getArgument()));
	
    
	log4jFileUtil.componentStartInfo(node);
	final boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    
	boolean existInnerJoin = false;
	int lstSizeInputs = inputTables.size();
	//prepare the flat data
	for (int i = 0; i < lstSizeInputs; i++) {
	    InputTable inputTable = inputTables.get(i);
	    String tableName = inputTable.getName();
	    
	    List<TableNode> tableNodes = inputTable.getNodes();
	    
	    if((tableNodes == null) || (tableNodes.size()) == 0) {
	    	continue;
	    }
	    
	    for(TableNode tnode : tableNodes) {
	    	String nodeName = tnode.getName();
	    	expressionConvertMap.put(tableName+"."+nodeName,"\\$"+(index++));
	    }
	    
	    String model = inputTable.getJoinModel();
	    if("Inner Join".equals(model)) {
	    	existInnerJoin = true;
	    }
	    
	    if(i == 0) {//main table
	    	IConnection connection = hNameToConnection.get(tableName);
	  		previous_node = connection.getSource().getUniqueName();
	  		INode loadNode = node.getDesignSubjobStartNode();
			start_node = loadNode.getUniqueName();
			
			boolean isLocal = "true".equals(ElementParameterParser.getValue(loadNode, "__LOCAL__"));

			if(!isLocal) {
                String loadDistribution = ElementParameterParser.getValue(loadNode, "__DISTRIBUTION__");
                String loadPigVersion = ElementParameterParser.getValue(loadNode, "__PIG_VERSION__");
                org.talend.hadoop.distribution.component.PigComponent pigDistrib = null;
    			try {
    				pigDistrib = (org.talend.hadoop.distribution.component.PigComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(loadDistribution, loadPigVersion);
    			} catch (java.lang.Exception e) {
    				e.printStackTrace();
    				return "";
    			}
    
    			boolean isCustom = pigDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;
    			isExecutedThroughWebHCat = !isCustom && pigDistrib != null && pigDistrib.isExecutedThroughWebHCat();
			}

			inMain = loadNode.isSubtreeStart();

    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(previous_node);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    
	    } else {//lookup table

    stringBuffer.append(TEXT_46);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_75);
    
		}
	}
	
	//the join action
	boolean isFirstInnerReject = true;
	
	List<String> currentInnerRejectSchemaTypes = new ArrayList<String>();
	List<String> previousInnerRejectSchemaTypes = new ArrayList<String>();
	
	for (int i = 0; i < lstSizeInputs; i++) {
	    InputTable inputTable = inputTables.get(i);
	    String tableName = inputTable.getName();
	    
	    List<TableNode> tableNodes = inputTable.getNodes();
	    
	    if((tableNodes == null) || (tableNodes.size()) == 0) {
	    	continue;
	    }
	    
	    boolean isMainTable = (i == 0);
	    
	    boolean isInnerJoin = false;
	 	if (isMainTable) {//main
	 		
	 	} else {//lookup
	 		String model = inputTable.getJoinModel();
	 		String optimization = inputTable.getJoinOptimization();
	 		String partitioner = inputTable.getCustomPartitioner();
	 		String parallelism = inputTable.getIncreaseParallelism();
	 		
	 		String joinModel = "";
	 		if("Inner Join".equals(model)) {
	 			isInnerJoin = true;
	 		} else if("Left Outer Join".equals(model)) {
	 			joinModel = "LEFT";
	 		} else if("Right Outer Join".equals(model)) {
	 			joinModel = "RIGHT";
	 		} else if("Full Outer Join".equals(model)) {
	 			joinModel = "FULL";
	 		}
	 		
	 		boolean useOptimization = false;
	 		String optimizationModel = "";
	 		if("None".equalsIgnoreCase(optimization)) {
	 			//do nothing
	 		} else if("Replicated".equalsIgnoreCase(optimization)) {
	 			optimizationModel = "replicated";
	 			useOptimization = true;
	 		} else if("Skewed".equalsIgnoreCase(optimization)) {
	 			optimizationModel = "skewed";
	 			useOptimization = true;
	 		} else if("Merge".equalsIgnoreCase(optimization)) {
	 			optimizationModel = "merge";
	 			useOptimization = true;
	 		}
	 		
 			List<String> innerRejectLocationExpressions = new ArrayList<String>();
 			

    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    
			if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_89);
    
			}
			
			List<String> expressions = new ArrayList<String>();
			List<String> names = new ArrayList<String>();
	 		for(TableNode tnode : tableNodes) {
	 			String name = tnode.getName();
	 			String expression = tnode.getExpression();
	 			if(expression!=null && !"".equals(expression.trim())) {
					expressions.add(expression);
					names.add(name);
	 			}
	 		}
	 		
	 		for(int j=0;j<expressions.size();j++) {
	 			String expression = expressions.get(j);
	 			expression = convertExpression(expressionConvertMap,expression);
	 			String expressionStr = NodeUtil.replaceCRLFInMEMO_SQL("\""+expression+"\"");

    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_92);
    
				if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_95);
    
				}
				
				if(j<expressions.size()-1) {

    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    
					if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    
					}
				}
	 		}

    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(joinModel);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_104);
    
			if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_108);
    
			}
			
	 		for(int j=0;j<names.size();j++) {
	 			String name = names.get(j);

    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_111);
    
				if(existInnerJoin && innerReject && isInnerJoin) {
    				String innerRejectLocationExpression = convertExpression(expressionConvertMap,tableName + "." + name);
    				innerRejectLocationExpressions.add(innerRejectLocationExpression);

    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_114);
    
				}
				
				if(j<names.size()-1) {

    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    
					if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    
					}
				}
	 		}
	 		

    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    
	 		if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    
	 		}
	 		
	 		if(useOptimization) {

    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(optimizationModel);
    stringBuffer.append(TEXT_125);
    
				if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(optimizationModel);
    stringBuffer.append(TEXT_128);
    
				}
	 		}
	 		
	 		if(partitioner!=null && !"".equals(partitioner)) {

    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(partitioner);
    stringBuffer.append(TEXT_131);
    
				if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(partitioner);
    stringBuffer.append(TEXT_134);
    
				}
	 		}
	 		
	 		if(parallelism!=null && !"".equals(parallelism)) {

    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(parallelism);
    stringBuffer.append(TEXT_137);
    
				if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(parallelism);
    stringBuffer.append(TEXT_140);
    
				}
	 		}

    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    
			if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_155);
    
				for(int j=0;j<innerRejectLocationExpressions.size();j++) {
					String locationExpression = innerRejectLocationExpressions.get(j);

    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(locationExpression);
    stringBuffer.append(TEXT_158);
    
					if(j<innerRejectLocationExpressions.size()-1) {

    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_160);
    
					}
				}

    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    
			}
	 	}
	 	
	 	
	 	
	 	//filter action : first join,second filter
		boolean hasFilter = inputTable.isActivateExpressionFilter() 
	    	&& inputTable.getExpressionFilter()!=null 
	    	&& !"".equals(inputTable.getExpressionFilter());
	    
	    if(existInnerJoin && innerReject) {
	    
	 		for(TableNode tnode : tableNodes) {
	 			String type = tnode.getType();
            	currentInnerRejectSchemaTypes.add(type);
	 		}
	    }
	    
	    if(hasFilter) {
 			String expression = inputTable.getExpressionFilter();
 			//convert the expression to big expression(replace the java var to pig $n)
 			expression = convertExpression(expressionConvertMap,expression);
 			String expressionStr = NodeUtil.replaceCRLFInMEMO_SQL("\""+expression+"\"");
			if(existInnerJoin && innerReject && isInnerJoin) {

    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_179);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_181);
    
				for(int j=0;j<currentInnerRejectSchemaTypes.size();j++) {
					if(j<(currentInnerRejectSchemaTypes.size() - tableNodes.size())) {

    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_184);
    
					} else {//the lookup columns
						String type = currentInnerRejectSchemaTypes.get(j);

    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(tableName);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(getPigType(type));
    stringBuffer.append(TEXT_188);
    
						if(j < (currentInnerRejectSchemaTypes.size()-1)) {

    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    
						}
					}
				}

    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    
				if(isFirstInnerReject) {

    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_202);
    
				} else {

    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_206);
    
					for(int j=0;j<currentInnerRejectSchemaTypes.size();j++) {
						if(j < previousInnerRejectSchemaTypes.size()) {//the same part

    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_209);
    
						} else {//the new part
							String type = currentInnerRejectSchemaTypes.get(j);

    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(tableName);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(getPigType(type));
    stringBuffer.append(TEXT_213);
    
							if(j < (currentInnerRejectSchemaTypes.size()-1)) {

    stringBuffer.append(TEXT_214);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_215);
    
							}
						}
					}

    stringBuffer.append(TEXT_216);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_222);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_227);
    
				}
				isFirstInnerReject = false;
			}

    stringBuffer.append(TEXT_228);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_232);
    
	 	} else {
	 		if(existInnerJoin && innerReject && isInnerJoin) {
	 			if(isFirstInnerReject) {

    stringBuffer.append(TEXT_233);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_234);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_236);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_237);
    
				} else {

    stringBuffer.append(TEXT_238);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_241);
    
					for(int j=0;j<currentInnerRejectSchemaTypes.size();j++) {
						if(j < previousInnerRejectSchemaTypes.size()) {//the same part

    stringBuffer.append(TEXT_242);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_243);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_244);
    
						} else {//the new part
							String type = currentInnerRejectSchemaTypes.get(j);

    stringBuffer.append(TEXT_245);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(tableName);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(getPigType(type));
    stringBuffer.append(TEXT_248);
    
							if(j < (currentInnerRejectSchemaTypes.size()-1)) {

    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    
							}
						}
					}

    stringBuffer.append(TEXT_251);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_259);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_260);
    
				}
				isFirstInnerReject = false;
			}
	 	}
	 	
	 	if((i == (lstSizeInputs - 1)) && existInnerJoin && innerReject && !isInnerJoin) {//the last input table is not inner join and inner reject exist


    stringBuffer.append(TEXT_261);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_264);
    
			for(int j=0;j<currentInnerRejectSchemaTypes.size();j++) {
				if(j < previousInnerRejectSchemaTypes.size()) {//the same part

    stringBuffer.append(TEXT_265);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_267);
    
				} else {//the new part
					String type = currentInnerRejectSchemaTypes.get(j);

    stringBuffer.append(TEXT_268);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_269);
    stringBuffer.append(tableName);
    stringBuffer.append(j);
    stringBuffer.append(TEXT_270);
    stringBuffer.append(getPigType(type));
    stringBuffer.append(TEXT_271);
    
					if(j < (currentInnerRejectSchemaTypes.size()-1)) {

    stringBuffer.append(TEXT_272);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_273);
    
					}
				}
			}

    stringBuffer.append(TEXT_274);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_278);
    
	 	}
	 	
	 	if(existInnerJoin && innerReject && isInnerJoin) {
	 		previousInnerRejectSchemaTypes.clear();
	 		previousInnerRejectSchemaTypes.addAll(currentInnerRejectSchemaTypes);
	 	}
	 	
	}
	
	//output action
	List<OutputTable> outputTablesSortedByReject = new ArrayList<OutputTable>(outputTables);
    Collections.sort(outputTablesSortedByReject, new Comparator<OutputTable>() {

        public int compare(OutputTable o1, OutputTable o2) {
            if (o1.isReject() != o2.isReject()) {
                if (o1.isReject()) {
                    return 1;
                } else {
                    return -1;
                }
            }
            if (o1.isRejectInnerJoin() != o2.isRejectInnerJoin()) {
                if (o1.isRejectInnerJoin()) {
                    return 1;
                } else {
                    return -1;
                }
            }
            return 0;
        }

    });
    
	int size = outputTablesSortedByReject.size();
	List<String> previousFilterExpressions = new ArrayList<String>();
	boolean previousAtLeastOneTableWithoutFilterExpression = false;
	boolean previousAllOutputTableIsReject = true;
	for(int i=0;i<size;i++) {
		OutputTable outputTable = outputTablesSortedByReject.get(i);
		boolean isReject = outputTable.isReject();
		boolean isRejectInnerJoin = outputTable.isRejectInnerJoin();
		
		if(previousAllOutputTableIsReject) {
			isReject = false;//set it to code generate logic,no output reject for the first several output reject tables
		}
		
	    String tableName = outputTable.getName();
	    
	    List<TableNode> tableNodes = outputTable.getNodes();
	    
	    if((tableNodes == null) || (tableNodes.size()) == 0) {
	    	continue;
	    }
	    

    stringBuffer.append(TEXT_279);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_281);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_283);
    
		if(existInnerJoin && isRejectInnerJoin) {

    stringBuffer.append(TEXT_284);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_286);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_288);
    
		}
		
		boolean hasFilter = outputTable.isActivateExpressionFilter() 
	    	&& outputTable.getExpressionFilter()!=null 
	    	&& !"".equals(outputTable.getExpressionFilter());
	   	
	   	boolean noRejectDataByFilter = isReject && previousAtLeastOneTableWithoutFilterExpression;
	   	boolean hasRejectDataByFilter = isReject && !previousAtLeastOneTableWithoutFilterExpression && previousFilterExpressions.size() > 0;
	    boolean needFilterExpression = hasFilter || hasRejectDataByFilter || noRejectDataByFilter;

	    if(needFilterExpression) {

    stringBuffer.append(TEXT_289);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_293);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_294);
    
	    }
	    
		if(existInnerJoin && isRejectInnerJoin && hasFilter) {

    stringBuffer.append(TEXT_295);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_296);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_297);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_298);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_299);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_300);
    
		}
	    
	    String currentExpression = "";
	    if(hasFilter) {
 			String expression = outputTable.getExpressionFilter();
 			//convert the expression to big expression(replace the java var to pig $n)
 			expression = convertExpression(expressionConvertMap,expression);
 			String expressionStr = NodeUtil.replaceCRLFInMEMO_SQL("\""+expression+"\"");
 			currentExpression = expressionStr;

    stringBuffer.append(TEXT_301);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_302);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_303);
    
			if(existInnerJoin && isRejectInnerJoin) {

    stringBuffer.append(TEXT_304);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_305);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_306);
    
			}
	 	}
	 	
		if(hasRejectDataByFilter) {//the common reject action
			if(hasFilter) {

    stringBuffer.append(TEXT_307);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_308);
    
			}

    stringBuffer.append(TEXT_309);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_310);
    
			for(int j=0;j<previousFilterExpressions.size();j++) {
				String previousFilterExpression = previousFilterExpressions.get(j);

    stringBuffer.append(TEXT_311);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(previousFilterExpression);
    stringBuffer.append(TEXT_313);
    
				if(j<previousFilterExpressions.size()-1) {

    stringBuffer.append(TEXT_314);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_315);
    
				}
			}

    stringBuffer.append(TEXT_316);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_317);
    
		} else if(noRejectDataByFilter) {//no data
			if(hasFilter) {

    stringBuffer.append(TEXT_318);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_319);
    
			}

    stringBuffer.append(TEXT_320);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_321);
    
		}
		
		if(needFilterExpression) {

    stringBuffer.append(TEXT_322);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_323);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_324);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_326);
    
		}
		
		if(existInnerJoin && isRejectInnerJoin && hasFilter) {

    stringBuffer.append(TEXT_327);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_328);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_329);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_330);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_331);
    
		}
		
		isReject = outputTable.isReject();//reset it to ui settings
		
		previousAllOutputTableIsReject = previousAllOutputTableIsReject && isReject;
		
		if(hasFilter && !(isReject || isRejectInnerJoin)) {
			previousFilterExpressions.add(currentExpression);
		} 
		
		if(!hasFilter && !(isReject || isRejectInnerJoin)){
			previousAtLeastOneTableWithoutFilterExpression = true;
		}
		
		if(existInnerJoin && isRejectInnerJoin) {
			if(isReject) {//union reject and inner reject

    stringBuffer.append(TEXT_332);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_333);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_336);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_337);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_338);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_339);
    
			} else {//only inner reject

    stringBuffer.append(TEXT_340);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_341);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_343);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_344);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_345);
    
			}
		}

    stringBuffer.append(TEXT_346);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_347);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_348);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_349);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_350);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_351);
    
	 	
		List<String> expressions = new ArrayList<String>();
		List<String> names = new ArrayList<String>();
 		for(TableNode tnode : tableNodes) {
 			String name = tnode.getName();
 			String expression = tnode.getExpression();
 			if(expression!=null && !"".equals(expression.trim())) {
				expressions.add(expression);
				names.add(name);
 			}
 		}
 		
 		for(int j=0;j<expressions.size();j++) {
			String expression = expressions.get(j);
			String name = names.get(j);
			expression = convertExpression(expressionConvertMap,expression);
			String expressionStr = NodeUtil.replaceCRLFInMEMO_SQL("\""+expression+" AS "+name+"\"");

    stringBuffer.append(TEXT_352);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_353);
    stringBuffer.append(expressionStr);
    stringBuffer.append(TEXT_354);
    
			if(j<expressions.size()-1) {

    stringBuffer.append(TEXT_355);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_356);
    
			}
 		}
 		

    stringBuffer.append(TEXT_357);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_358);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_359);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_361);
    
	 
	}

    stringBuffer.append(TEXT_362);
    stringBuffer.append(TEXT_363);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_364);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_365);
    
	if(inMain) {

    stringBuffer.append(TEXT_366);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_369);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_370);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_371);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_372);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_373);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_374);
    }
    stringBuffer.append(TEXT_375);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_376);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_377);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_378);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_379);
    					
				} else {

    stringBuffer.append(TEXT_380);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_381);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_382);
    
				}

    stringBuffer.append(TEXT_383);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_384);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_385);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_386);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_387);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_388);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_389);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_390);
    					
				} else {

    stringBuffer.append(TEXT_391);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_392);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_393);
    
				}

    stringBuffer.append(TEXT_394);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_395);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_396);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_397);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_398);
    					
				} else {

    stringBuffer.append(TEXT_399);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_400);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_401);
    
				}

    stringBuffer.append(TEXT_402);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_403);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_404);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_405);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_406);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_407);
    					
				} else {

    stringBuffer.append(TEXT_408);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_409);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_410);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_411);
    
				}

    stringBuffer.append(TEXT_412);
    
	} else {

    stringBuffer.append(TEXT_413);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_414);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_415);
    
	}

    stringBuffer.append(TEXT_416);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_417);
    stringBuffer.append(TEXT_418);
    return stringBuffer.toString();
  }
}