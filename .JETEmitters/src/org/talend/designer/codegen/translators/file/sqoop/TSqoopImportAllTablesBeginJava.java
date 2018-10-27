package org.talend.designer.codegen.translators.file.sqoop;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.designer.runprocess.ProcessorException;
import org.talend.designer.runprocess.ProcessorUtilities;
import java.util.List;
import java.util.Map;

public class TSqoopImportAllTablesBeginJava
{
  protected static String nl;
  public static synchronized TSqoopImportAllTablesBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSqoopImportAllTablesBeginJava result = new TSqoopImportAllTablesBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_3 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_4 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_5 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_6 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_7 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_8 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_9 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_10 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_11 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_12 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_13 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_14 = " - Written records count: \" + nb_line_";
  protected final String TEXT_15 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_16 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_17 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_19 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_20 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_21 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_22 = " - Writing the record \" + nb_line_";
  protected final String TEXT_23 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_24 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_25 = " - Processing the record \" + nb_line_";
  protected final String TEXT_26 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_27 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_28 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_29 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_30 = NL;
  protected final String TEXT_31 = NL;
  protected final String TEXT_32 = NL + NL + "class SqoopAdditionalArgumentParser_";
  protected final String TEXT_33 = " {" + NL + "" + NL + "    public String[] parse(String command) {" + NL + "        final char QUOTE = '\\\"';" + NL + "        final char SINGLEQUOTE = '\\'';" + NL + "        final char SPACE = ' ';" + NL + "        final char BACKSLASH = '\\\\';" + NL + "        java.util.List<String> list = new java.util.ArrayList<String>();" + NL + "        if (command == null) {" + NL + "            return null;" + NL + "        }" + NL + "" + NL + "        StringBuilder sb = new StringBuilder();" + NL + "        char[] chars = command.trim().toCharArray();" + NL + "        boolean backslash = true;" + NL + "        boolean withinString = false;" + NL + "        for (char c : chars) {" + NL + "            switch (c) {" + NL + "            case SPACE:" + NL + "                if (!withinString) {" + NL + "                    if (sb.toString().trim().length() > 0) {" + NL + "                        list.add(sb.toString());" + NL + "                        sb = new StringBuilder();" + NL + "                    }" + NL + "                } else {" + NL + "                    sb.append(c);" + NL + "                }" + NL + "                break;" + NL + "            case SINGLEQUOTE:" + NL + "                if (!withinString) {" + NL + "                    withinString = true;" + NL + "                } else {" + NL + "                    withinString = false;" + NL + "                }" + NL + "                break;" + NL + "            case QUOTE:" + NL + "                if (backslash && !withinString) {" + NL + "                    withinString = true;" + NL + "                }" + NL + "                if (backslash && withinString) {" + NL + "                    withinString = false;" + NL + "                }" + NL + "                break;" + NL + "            case BACKSLASH:" + NL + "                if (!backslash) {" + NL + "                    backslash = true;" + NL + "                } else {" + NL + "                    backslash = false;" + NL + "                }" + NL + "                break;" + NL + "            default:" + NL + "                sb.append(c);" + NL + "            }" + NL + "        }" + NL + "        if (sb.toString().trim().length() > 0) {" + NL + "            list.add(sb.toString());" + NL + "        }" + NL + "        return list.toArray(new String[list.size()]);" + NL + "    }" + NL + "}" + NL + "SqoopAdditionalArgumentParser_";
  protected final String TEXT_34 = " parser_";
  protected final String TEXT_35 = " = new SqoopAdditionalArgumentParser_";
  protected final String TEXT_36 = "();" + NL + "" + NL + "//call the cmd part" + NL + "Runtime runtime_";
  protected final String TEXT_37 = " = Runtime.getRuntime();" + NL;
  protected final String TEXT_38 = NL;
  protected final String TEXT_39 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_40 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_41 = ");";
  protected final String TEXT_42 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_43 = " = ";
  protected final String TEXT_44 = "; ";
  protected final String TEXT_45 = NL + "\t\t" + NL + "" + NL + "String[] cmd_";
  protected final String TEXT_46 = " = new String[] {\"sqoop\", \"import-all-tables\", \"--connect\", ";
  protected final String TEXT_47 = ", \"--username\", ";
  protected final String TEXT_48 = NL + "\t, \"--password-file\", ";
  protected final String TEXT_49 = NL + "\t, \"--password\", decryptedPassword_";
  protected final String TEXT_50 = ", \"--verbose\"";
  protected final String TEXT_51 = NL + "\t, \"--direct\"";
  protected final String TEXT_52 = NL + "\t, \"--direct-split-size\", ";
  protected final String TEXT_53 = NL + "\t, \"--compress\"";
  protected final String TEXT_54 = NL + "\t, \"--compression-codec\", ";
  protected final String TEXT_55 = ", \"--as-sequencefile\"";
  protected final String TEXT_56 = ", \"--as-avrodatafile\"";
  protected final String TEXT_57 = ", \"--as-parquetfile\"";
  protected final String TEXT_58 = ", \"--mysql-delimiters\"";
  protected final String TEXT_59 = ", \"--num-mappers\", ";
  protected final String TEXT_60 = NL + "\t\t,\"--exclude-tables\"";
  protected final String TEXT_61 = NL + "\t\t\t\t,";
  protected final String TEXT_62 = NL + "\t\t\t\t+ \",\" + ";
  protected final String TEXT_63 = " " + NL + "            ,\"";
  protected final String TEXT_64 = "\", ";
  protected final String TEXT_65 = " ";
  protected final String TEXT_66 = " " + NL + "            ,\"";
  protected final String TEXT_67 = "\" ";
  protected final String TEXT_68 = NL + "\t\t,\"--map-column-hive\"";
  protected final String TEXT_69 = NL + "\t\t\t\t,";
  protected final String TEXT_70 = " + \"=\" + ";
  protected final String TEXT_71 = NL + "\t\t\t\t+\",\" + ";
  protected final String TEXT_72 = " + \"=\" + ";
  protected final String TEXT_73 = NL + "};" + NL + "" + NL + "String[] additionalArgs_";
  protected final String TEXT_74 = " = parser_";
  protected final String TEXT_75 = ".parse(";
  protected final String TEXT_76 = ");" + NL + "cmd_";
  protected final String TEXT_77 = " = (String[]) org.apache.commons.lang.ArrayUtils.addAll(cmd_";
  protected final String TEXT_78 = ", additionalArgs_";
  protected final String TEXT_79 = ");" + NL;
  protected final String TEXT_80 = NL + "java.lang.StringBuilder stringBuilder_";
  protected final String TEXT_81 = " = new java.lang.StringBuilder();" + NL + "for(String parameter_";
  protected final String TEXT_82 = " : cmd_";
  protected final String TEXT_83 = ") {" + NL + "\tstringBuilder_";
  protected final String TEXT_84 = ".append(parameter_";
  protected final String TEXT_85 = ");" + NL + "\tstringBuilder_";
  protected final String TEXT_86 = ".append(\" \");" + NL + "}" + NL + "log.info(\"";
  protected final String TEXT_87 = " - execute sqoop command: \" + stringBuilder_";
  protected final String TEXT_88 = ".toString());";
  protected final String TEXT_89 = NL + NL + "Process ps_";
  protected final String TEXT_90 = " = null;" + NL + "java.lang.StringBuilder sb_";
  protected final String TEXT_91 = " = null;" + NL + "" + NL + "try {" + NL + "\tps_";
  protected final String TEXT_92 = " = runtime_";
  protected final String TEXT_93 = ".exec(cmd_";
  protected final String TEXT_94 = ");" + NL + "\tbyte[] byteArray_";
  protected final String TEXT_95 = " = new byte[1024];" + NL + "\tint len_";
  protected final String TEXT_96 = " = 0;" + NL + "\t" + NL + "\tjava.io.InputStream errorStream_";
  protected final String TEXT_97 = " = new java.io.BufferedInputStream(ps_";
  protected final String TEXT_98 = ".getErrorStream());" + NL + "\tsb_";
  protected final String TEXT_99 = " = new java.lang.StringBuilder();" + NL + "\twhile ((len_";
  protected final String TEXT_100 = " = errorStream_";
  protected final String TEXT_101 = ".read(byteArray_";
  protected final String TEXT_102 = ")) != -1) {" + NL + "\t    String line_";
  protected final String TEXT_103 = " = new String(byteArray_";
  protected final String TEXT_104 = ", 0, len_";
  protected final String TEXT_105 = ", \"ISO-8859-1\");";
  protected final String TEXT_106 = NL + "\t        System.err.println(line_";
  protected final String TEXT_107 = ");";
  protected final String TEXT_108 = NL + "\t    sb_";
  protected final String TEXT_109 = ".append(line_";
  protected final String TEXT_110 = ");" + NL + "\t}" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_111 = "_ERROR_MESSAGE\", sb_";
  protected final String TEXT_112 = ".toString());" + NL + "\t";
  protected final String TEXT_113 = NL + "\t\tlog.error(\"";
  protected final String TEXT_114 = " - \" + sb_";
  protected final String TEXT_115 = ".toString());" + NL + "\t";
  protected final String TEXT_116 = NL + "\t" + NL + "\tbyteArray_";
  protected final String TEXT_117 = " = new byte[1024];" + NL + "\tlen_";
  protected final String TEXT_118 = " = 0;" + NL + "\tjava.io.InputStream inputStream_";
  protected final String TEXT_119 = " = new java.io.BufferedInputStream(ps_";
  protected final String TEXT_120 = ".getInputStream());" + NL + "\tsb_";
  protected final String TEXT_121 = " = new java.lang.StringBuilder();" + NL + "\twhile ((len_";
  protected final String TEXT_122 = " = inputStream_";
  protected final String TEXT_123 = ".read(byteArray_";
  protected final String TEXT_124 = ")) != -1) {" + NL + "\t    String line_";
  protected final String TEXT_125 = " = new String(byteArray_";
  protected final String TEXT_126 = ", 0, len_";
  protected final String TEXT_127 = ", \"ISO-8859-1\");";
  protected final String TEXT_128 = NL + "\t        System.out.println(line_";
  protected final String TEXT_129 = ");";
  protected final String TEXT_130 = NL + "\t    sb_";
  protected final String TEXT_131 = ".append(line_";
  protected final String TEXT_132 = ");" + NL + "\t}" + NL + "} catch (Exception e) {";
  protected final String TEXT_133 = NL + "\t\tthrow e;";
  protected final String TEXT_134 = NL + "\t\tSystem.err.println(e.getMessage());" + NL + "\t\t";
  protected final String TEXT_135 = NL + "\t\tlog.error(\"";
  protected final String TEXT_136 = " - \" + e.getMessage());" + NL + "\t\t";
  protected final String TEXT_137 = "\t" + NL + "}" + NL + "" + NL + "\tint result_";
  protected final String TEXT_138 = " = ps_";
  protected final String TEXT_139 = ".waitFor();" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_140 = "_EXIT_CODE\", result_";
  protected final String TEXT_141 = ");" + NL;
  protected final String TEXT_142 = NL + "\t\tif(ps_";
  protected final String TEXT_143 = ".exitValue()>0) {" + NL + "\t\t\tthrow new Exception(\"The Sqoop import has failed. Please check the logs.\");" + NL + "\t\t}";
  protected final String TEXT_144 = NL + NL + "if(sb_";
  protected final String TEXT_145 = "!=null) {" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_146 = "_OUTPUT_MESSAGE\", sb_";
  protected final String TEXT_147 = ".toString());" + NL + "\t";
  protected final String TEXT_148 = NL + "\t\tlog.info(\"";
  protected final String TEXT_149 = " - \" + sb_";
  protected final String TEXT_150 = ".toString());" + NL + "\t";
  protected final String TEXT_151 = NL + "}";
  protected final String TEXT_152 = NL + "\t";
  protected final String TEXT_153 = NL + "\tglobalMap.put(\"current_client_path_separator\", System.getProperty(\"path.separator\"));" + NL + "\tSystem.setProperty(\"path.separator\", ";
  protected final String TEXT_154 = ");" + NL + "" + NL + "\tglobalMap.put(\"HADOOP_USER_NAME_";
  protected final String TEXT_155 = "\", System.getProperty(\"HADOOP_USER_NAME\"));";
  protected final String TEXT_156 = NL + "\t\tString username_";
  protected final String TEXT_157 = " = ";
  protected final String TEXT_158 = ";" + NL + "\t\tif(username_";
  protected final String TEXT_159 = "!=null && !\"\".equals(username_";
  protected final String TEXT_160 = ".trim())) {" + NL + "\t\t\tSystem.setProperty(\"HADOOP_USER_NAME\",username_";
  protected final String TEXT_161 = ");" + NL + "\t\t}";
  protected final String TEXT_162 = NL + NL + "\torg.apache.hadoop.conf.Configuration configuration_";
  protected final String TEXT_163 = " = new org.apache.hadoop.conf.Configuration();" + NL + "\tconfiguration_";
  protected final String TEXT_164 = ".set(\"";
  protected final String TEXT_165 = "\", ";
  protected final String TEXT_166 = ");";
  protected final String TEXT_167 = NL + "\t\tconfiguration_";
  protected final String TEXT_168 = ".set(\"mapreduce.job.map.output.collector.class\", \"org.apache.hadoop.mapred.MapRFsOutputBuffer\");" + NL + "\t    configuration_";
  protected final String TEXT_169 = ".set(\"mapreduce.job.reduce.shuffle.consumer.plugin.class\", \"org.apache.hadoop.mapreduce.task.reduce.DirectShuffle\");";
  protected final String TEXT_170 = NL + "    \tconfiguration_";
  protected final String TEXT_171 = ".set(\"mapreduce.framework.name\", \"yarn\");" + NL + "    \tconfiguration_";
  protected final String TEXT_172 = ".set(\"yarn.resourcemanager.address\", ";
  protected final String TEXT_173 = ");";
  protected final String TEXT_174 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_175 = ".set(\"mapreduce.jobhistory.address\", ";
  protected final String TEXT_176 = ");" + NL + "\t\t\t";
  protected final String TEXT_177 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_178 = ".set(\"yarn.resourcemanager.scheduler.address\", ";
  protected final String TEXT_179 = ");";
  protected final String TEXT_180 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_181 = ".set(\"yarn.app.mapreduce.am.staging-dir\", ";
  protected final String TEXT_182 = ");";
  protected final String TEXT_183 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_184 = ".set(\"mapreduce.app-submission.cross-platform\",\"true\");";
  protected final String TEXT_185 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_186 = ".set(\"mapreduce.application.classpath\", \"";
  protected final String TEXT_187 = "\");";
  protected final String TEXT_188 = NL + "\t\tconfiguration_";
  protected final String TEXT_189 = ".set(\"yarn.application.classpath\", \"";
  protected final String TEXT_190 = "\");";
  protected final String TEXT_191 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_192 = ".set(\"mapreduce.map.memory.mb\", ";
  protected final String TEXT_193 = ");" + NL + "\t\t\tconfiguration_";
  protected final String TEXT_194 = ".set(\"mapreduce.reduce.memory.mb\", ";
  protected final String TEXT_195 = ");" + NL + "\t\t\tconfiguration_";
  protected final String TEXT_196 = ".set(\"yarn.app.mapreduce.am.resource.mb\", ";
  protected final String TEXT_197 = ");";
  protected final String TEXT_198 = NL + "\t\tconfiguration_";
  protected final String TEXT_199 = ".set(\"mapred.job.tracker\", ";
  protected final String TEXT_200 = ");";
  protected final String TEXT_201 = NL + "        configuration_";
  protected final String TEXT_202 = ".set(\"mapred.job.map.memory.mb\", ";
  protected final String TEXT_203 = ");" + NL + "        configuration_";
  protected final String TEXT_204 = ".set(\"mapred.job.reduce.memory.mb\", ";
  protected final String TEXT_205 = ");";
  protected final String TEXT_206 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_207 = ".set(";
  protected final String TEXT_208 = " ,";
  protected final String TEXT_209 = ");";
  protected final String TEXT_210 = NL + "\t\tconfiguration_";
  protected final String TEXT_211 = ".set(\"dfs.namenode.kerberos.principal\", ";
  protected final String TEXT_212 = ");";
  protected final String TEXT_213 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_214 = ".set(\"mapreduce.jobtracker.kerberos.principal\", ";
  protected final String TEXT_215 = ");";
  protected final String TEXT_216 = NL + "\t\t\tconfiguration_";
  protected final String TEXT_217 = ".set(\"yarn.resourcemanager.principal\", ";
  protected final String TEXT_218 = ");" + NL + "\t\t\tconfiguration_";
  protected final String TEXT_219 = ".set(\"mapreduce.jobhistory.principal\", ";
  protected final String TEXT_220 = ");";
  protected final String TEXT_221 = NL + "            System.setProperty(\"pname\", \"MapRLogin\");" + NL + "            System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "            System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_222 = ");" + NL + "            System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_223 = ");";
  protected final String TEXT_224 = NL + "\t\t\torg.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(";
  protected final String TEXT_225 = ", ";
  protected final String TEXT_226 = ");";
  protected final String TEXT_227 = NL + "            com.mapr.login.client.MapRLoginHttpsClient maprLogin_";
  protected final String TEXT_228 = " = new com.mapr.login.client.MapRLoginHttpsClient();" + NL + "            maprLogin_";
  protected final String TEXT_229 = ".getMapRCredentialsViaKerberos(";
  protected final String TEXT_230 = ", ";
  protected final String TEXT_231 = ");";
  protected final String TEXT_232 = NL + "            System.setProperty(\"pname\", \"MapRLogin\");" + NL + "            System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "            System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_233 = ");" + NL + "            com.mapr.login.client.MapRLoginHttpsClient maprLogin_";
  protected final String TEXT_234 = " = new com.mapr.login.client.MapRLoginHttpsClient();";
  protected final String TEXT_235 = NL + "                System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_236 = ");";
  protected final String TEXT_237 = NL + "                maprLogin_";
  protected final String TEXT_238 = ".setCheckUGI(false);";
  protected final String TEXT_239 = " " + NL + "                final String decryptedMapRPassword_";
  protected final String TEXT_240 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_241 = ");";
  protected final String TEXT_242 = NL + "                final String decryptedMapRPassword_";
  protected final String TEXT_243 = " = ";
  protected final String TEXT_244 = "; ";
  protected final String TEXT_245 = NL;
  protected final String TEXT_246 = NL + "            \tmaprLogin_";
  protected final String TEXT_247 = ".getMapRCredentialsViaPassword(";
  protected final String TEXT_248 = ", ";
  protected final String TEXT_249 = ", decryptedMapRPassword_";
  protected final String TEXT_250 = ", ";
  protected final String TEXT_251 = ", \"\");" + NL + "            \t";
  protected final String TEXT_252 = NL + "            \tmaprLogin_";
  protected final String TEXT_253 = ".getMapRCredentialsViaPassword(";
  protected final String TEXT_254 = ", ";
  protected final String TEXT_255 = ", decryptedMapRPassword_";
  protected final String TEXT_256 = ", ";
  protected final String TEXT_257 = ");" + NL + "            \t";
  protected final String TEXT_258 = NL + "        configuration_";
  protected final String TEXT_259 = ".set(\"dfs.client.use.datanode.hostname\", \"true\");";
  protected final String TEXT_260 = NL + "\t//check whether we can connect to the fs?" + NL + "\torg.apache.hadoop.fs.FileSystem fs_";
  protected final String TEXT_261 = " = org.apache.hadoop.fs.FileSystem.get(configuration_";
  protected final String TEXT_262 = ");" + NL + "\tcom.cloudera.sqoop.tool.SqoopTool sqoopTool_";
  protected final String TEXT_263 = " = com.cloudera.sqoop.tool.SqoopTool.getTool(\"import-all-tables\");" + NL + "" + NL + "\tcom.cloudera.sqoop.SqoopOptions sqoopOptions_";
  protected final String TEXT_264 = " = new com.cloudera.sqoop.SqoopOptions(configuration_";
  protected final String TEXT_265 = ");" + NL + "\t" + NL + "\tsqoopOptions_";
  protected final String TEXT_266 = ".setConnectString(";
  protected final String TEXT_267 = "); // __CONNECTION__" + NL + "\tsqoopOptions_";
  protected final String TEXT_268 = ".setUsername(";
  protected final String TEXT_269 = "); // __USERNAME__" + NL + "\t";
  protected final String TEXT_270 = NL + "\t\t";
  protected final String TEXT_271 = NL + "\t\t";
  protected final String TEXT_272 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_273 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_274 = ");";
  protected final String TEXT_275 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_276 = " = ";
  protected final String TEXT_277 = "; ";
  protected final String TEXT_278 = NL + "\t\tsqoopOptions_";
  protected final String TEXT_279 = ".setPassword(decryptedPassword_";
  protected final String TEXT_280 = "); // __PASSWORD__" + NL + "\t";
  protected final String TEXT_281 = NL + "\t\tsqoopOptions_";
  protected final String TEXT_282 = ".setPasswordFilePath(";
  protected final String TEXT_283 = ");" + NL + "\t\tsqoopOptions_";
  protected final String TEXT_284 = ".setPassword(org.apache.sqoop.util.CredentialsUtil.fetchPassword";
  protected final String TEXT_285 = "(sqoopOptions_";
  protected final String TEXT_286 = "));" + NL + "\t";
  protected final String TEXT_287 = NL + NL + "\t";
  protected final String TEXT_288 = NL + "\t\tsqoopOptions_";
  protected final String TEXT_289 = ".setDirectMode(true);" + NL + "\t\t";
  protected final String TEXT_290 = NL + "\t\t\tsqoopOptions_";
  protected final String TEXT_291 = ".setDirectSplitSize(Long.parseLong(";
  protected final String TEXT_292 = "));" + NL + "\t\t";
  protected final String TEXT_293 = NL + "\t";
  protected final String TEXT_294 = NL + "\t\tsqoopOptions_";
  protected final String TEXT_295 = ".setUseCompression(true);" + NL + "\t\t";
  protected final String TEXT_296 = NL + "\t\t\tsqoopOptions_";
  protected final String TEXT_297 = ".setCompressionCodec(";
  protected final String TEXT_298 = ");" + NL + "\t\t";
  protected final String TEXT_299 = NL + "\t";
  protected final String TEXT_300 = "sqoopOptions_";
  protected final String TEXT_301 = ".setFileLayout(com.cloudera.sqoop.SqoopOptions.FileLayout.SequenceFile);";
  protected final String TEXT_302 = "sqoopOptions_";
  protected final String TEXT_303 = ".setFileLayout(com.cloudera.sqoop.SqoopOptions.FileLayout.TextFile);";
  protected final String TEXT_304 = "sqoopOptions_";
  protected final String TEXT_305 = ".setFileLayout(com.cloudera.sqoop.SqoopOptions.FileLayout.ParquetFile);";
  protected final String TEXT_306 = "sqoopOptions_";
  protected final String TEXT_307 = ".setFileLayout(com.cloudera.sqoop.SqoopOptions.FileLayout.AvroDataFile);";
  protected final String TEXT_308 = NL + NL + "\t";
  protected final String TEXT_309 = "sqoopOptions_";
  protected final String TEXT_310 = ".setNumMappers(Integer.valueOf(";
  protected final String TEXT_311 = "));";
  protected final String TEXT_312 = NL + "\t" + NL + "\tjava.util.Properties additionalProperties_";
  protected final String TEXT_313 = " = new java.util.Properties();" + NL + "\t";
  protected final String TEXT_314 = NL + "\t\tadditionalProperties_";
  protected final String TEXT_315 = ".put(\"customtool.options.jsonmap\", \"{\\\"\\\":\\\"\\\"}\");" + NL + "\t";
  protected final String TEXT_316 = " " + NL + "\t\t\t\tadditionalProperties_";
  protected final String TEXT_317 = ".put(";
  protected final String TEXT_318 = ", \"\"+";
  protected final String TEXT_319 = ".codePointAt(0)); ";
  protected final String TEXT_320 = " " + NL + "\t\t\t\tadditionalProperties_";
  protected final String TEXT_321 = ".put(";
  protected final String TEXT_322 = ", ";
  protected final String TEXT_323 = "); ";
  protected final String TEXT_324 = NL + "\t\t\t\tStringBuilder sb_exclude_";
  protected final String TEXT_325 = " = new StringBuilder();";
  protected final String TEXT_326 = NL + "\t\t\t\t\t\tsb_exclude_";
  protected final String TEXT_327 = ".append(";
  protected final String TEXT_328 = ");";
  protected final String TEXT_329 = NL + "\t\t\t\t\t\tsb_exclude_";
  protected final String TEXT_330 = ".append(\",\" + ";
  protected final String TEXT_331 = ");";
  protected final String TEXT_332 = NL + "\t\t\t\tsqoopOptions_";
  protected final String TEXT_333 = ".setAllTablesExclude(sb_exclude_";
  protected final String TEXT_334 = ".toString());";
  protected final String TEXT_335 = NL + "\t\t\tStringBuilder sb_hive_";
  protected final String TEXT_336 = " = new StringBuilder();" + NL + "\t";
  protected final String TEXT_337 = NL + "\t\t\t\t\tsb_hive_";
  protected final String TEXT_338 = ".append(";
  protected final String TEXT_339 = " + \"=\" + ";
  protected final String TEXT_340 = ");" + NL + "\t";
  protected final String TEXT_341 = NL + "\t\t\t\t\tsb_hive_";
  protected final String TEXT_342 = ".append(\",\" + ";
  protected final String TEXT_343 = " + \"=\" + ";
  protected final String TEXT_344 = ");" + NL + "\t";
  protected final String TEXT_345 = NL + "\t\t\tsqoopOptions_";
  protected final String TEXT_346 = ".setMapColumnHive(sb_hive_";
  protected final String TEXT_347 = ".toString());" + NL + "\t";
  protected final String TEXT_348 = NL + "\t" + NL + "\tsqoopOptions_";
  protected final String TEXT_349 = ".loadProperties(additionalProperties_";
  protected final String TEXT_350 = ");" + NL + "" + NL + "\torg.apache.sqoop.Sqoop sqoop_";
  protected final String TEXT_351 = " = new org.apache.sqoop.Sqoop(sqoopTool_";
  protected final String TEXT_352 = ", configuration_";
  protected final String TEXT_353 = ", sqoopOptions_";
  protected final String TEXT_354 = ");" + NL + "" + NL + "\t";
  protected final String TEXT_355 = NL + "\t\troutines.system.GetJarsToRegister getJarsToRegister_";
  protected final String TEXT_356 = " = new routines.system.GetJarsToRegister();" + NL + "\t\tjava.lang.StringBuilder sb_";
  protected final String TEXT_357 = " = new java.lang.StringBuilder();";
  protected final String TEXT_358 = NL + "\t\t\t\t\tsb_";
  protected final String TEXT_359 = ".append(getJarsToRegister_";
  protected final String TEXT_360 = ".replaceJarPaths(\"";
  protected final String TEXT_361 = "\", \"file:///\") + \",\");";
  protected final String TEXT_362 = NL + "\t";
  protected final String TEXT_363 = NL + "\t\troutines.system.GetJarsToRegister getJarsToRegister_";
  protected final String TEXT_364 = " = new routines.system.GetJarsToRegister();" + NL + "\t\tjava.lang.StringBuilder sb_";
  protected final String TEXT_365 = " = new java.lang.StringBuilder();";
  protected final String TEXT_366 = NL + "\t\t\t\t\tsb_";
  protected final String TEXT_367 = ".append(getJarsToRegister_";
  protected final String TEXT_368 = ".replaceJarPaths(\"";
  protected final String TEXT_369 = "\", \"file:///\") + \",\");";
  protected final String TEXT_370 = NL + "\ttry {";
  protected final String TEXT_371 = NL + "\t\t\tint result_";
  protected final String TEXT_372 = " = org.apache.sqoop.Sqoop.runSqoop(sqoop_";
  protected final String TEXT_373 = ", new String[] {\"-libjars\", sb_";
  protected final String TEXT_374 = ".toString()});";
  protected final String TEXT_375 = NL + "\t\t\tint result_";
  protected final String TEXT_376 = " = org.apache.sqoop.Sqoop.runSqoop(sqoop_";
  protected final String TEXT_377 = ", new String[] {});";
  protected final String TEXT_378 = NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_379 = "_EXIT_CODE\", result_";
  protected final String TEXT_380 = ");" + NL + "\t\tif(result_";
  protected final String TEXT_381 = " != 0) {";
  protected final String TEXT_382 = NL + "\t\t\t\tthrow new Exception(\"The Sqoop import job has failed. Please check the logs.\");";
  protected final String TEXT_383 = NL + "\t\t\t\tSystem.err.println(\"The Sqoop import job has failed. Please check the logs.\");" + NL + "\t\t\t\t";
  protected final String TEXT_384 = NL + "\t\t\t\tlog.error(\"";
  protected final String TEXT_385 = " - The Sqoop import job has failed. Please check the logs.\");" + NL + "\t\t\t\t";
  protected final String TEXT_386 = NL + "\t\t}" + NL + "\t} catch (Exception e) {";
  protected final String TEXT_387 = NL + "\t\t\tthrow e;";
  protected final String TEXT_388 = NL + "\t\t\tSystem.err.println(e.getMessage());" + NL + "\t\t\t";
  protected final String TEXT_389 = NL + "\t\t\tlog.error(\"";
  protected final String TEXT_390 = " - \" + e.getMessage());" + NL + "\t\t\t";
  protected final String TEXT_391 = NL + "\t}" + NL + "\t" + NL + "\t";
  protected final String TEXT_392 = NL + "String currentClientPathSeparator_";
  protected final String TEXT_393 = " = (String)globalMap.get(\"current_client_path_separator\");" + NL + "if(currentClientPathSeparator_";
  protected final String TEXT_394 = "!=null) {" + NL + "\tSystem.setProperty(\"path.separator\", currentClientPathSeparator_";
  protected final String TEXT_395 = ");" + NL + "\tglobalMap.put(\"current_client_path_separator\", null);" + NL + "}" + NL + "" + NL + "String originalHadoopUsername_";
  protected final String TEXT_396 = " = (String)globalMap.get(\"HADOOP_USER_NAME_";
  protected final String TEXT_397 = "\");" + NL + "if(originalHadoopUsername_";
  protected final String TEXT_398 = "!=null) {" + NL + "\tSystem.setProperty(\"HADOOP_USER_NAME\", originalHadoopUsername_";
  protected final String TEXT_399 = ");" + NL + "\tglobalMap.put(\"HADOOP_USER_NAME_";
  protected final String TEXT_400 = "\", null);" + NL + "} else {" + NL + "\tSystem.clearProperty(\"HADOOP_USER_NAME\");" + NL + "}";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
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
			
    stringBuffer.append(TEXT_2);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_3);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_4);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_7);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_9);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_10);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_13);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_18);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_20);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_21);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_23);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_24);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_26);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_27);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_29);
    
			}
		}
	}
	
	final DefaultLog4jFileUtil log4jFileUtil = new DefaultLog4jFileUtil((INode)(((org.talend.designer.codegen.config.CodeGeneratorArgument)argument).getArgument()));
	
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    
	log4jFileUtil.componentStartInfo(node);
	final boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
    
    boolean useCommandLine = "true".equals(ElementParameterParser.getValue(node,"__USE_COMMANDLINE__"));
    
    if(useCommandLine) {

    stringBuffer.append(TEXT_30);
    
    String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
    String username = ElementParameterParser.getValue(node,"__USERNAME__");
    boolean passwordStoredInFile = "true".equals(ElementParameterParser.getValue(node, "__PASSWORD_STORED_IN_FILE__"));
    boolean printLog = "true".equals(ElementParameterParser.getValue(node,"__PRINT_LOG__"));
    boolean verbose = "true".equals(ElementParameterParser.getValue(node,"__VERBOSE__"));
    
    boolean direct = "true".equals(ElementParameterParser.getValue(node,"__DIRECT__"));
    boolean splitInputStreamDirect = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_DIRECT_SPLIT_SIZE__"));
	 String directSplitSize = ElementParameterParser.getValue(node, "__DIRECT_SPLIT_SIZE__");
    
    boolean compress = "true".equals(ElementParameterParser.getValue(node,"__COMPRESS__"));
    boolean useHadoopCodec = "true".equals(ElementParameterParser.getValue(node,"__DEFINE_HADOOP_CODEC__"));
	 String hadoopCodec = ElementParameterParser.getValue(node,"__HADOOP_CODEC__");
	 
    boolean mysqlDelimiters = "true".equals(ElementParameterParser.getValue(node,"__MYSQL_DELIMITERS__"));
    boolean useMappers = "true".equals(ElementParameterParser.getValue(node,"__USE_MAPPERS__"));
    String mappers = ElementParameterParser.getValue(node,"__MAPPERS__");
    String fileFormat = ElementParameterParser.getValue(node,"__FILE_FORMAT__");
    
    List<Map<String,String>> additionalList = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__ADDITIONAL_ARGUMENTS__");
    String additionalCommandLineArguments = ElementParameterParser.getValue(node,"__ADDITIONAL_COMMANDLINE_ARGUMENTS__");
    
    boolean overrideHiveMapping = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_HIVE_MAPPING__"));
    boolean excludeTable = "true".equals(ElementParameterParser.getValue(node, "__EXCLUDE_TABLE__"));

    boolean dieOnError = "true".equals(ElementParameterParser.getValue(node, "__DIE_ON_ERROR__"));

    stringBuffer.append(TEXT_31);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_37);
    
String passwordFieldName = "__PASSWORD__";

    stringBuffer.append(TEXT_38);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_41);
    } else {
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_44);
    }
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_46);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(username);
    if(passwordStoredInFile) {
    stringBuffer.append(TEXT_48);
    stringBuffer.append((String)ElementParameterParser.getValue(node, "__PASSWORD_FILE__"));
    } else {
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    }
    if(printLog && verbose){
    stringBuffer.append(TEXT_50);
    }
    if(direct){
    stringBuffer.append(TEXT_51);
    
	if(splitInputStreamDirect) {

    stringBuffer.append(TEXT_52);
    stringBuffer.append(directSplitSize);
    
	}
}
    if(compress){
    stringBuffer.append(TEXT_53);
    
	if(useHadoopCodec) {

    stringBuffer.append(TEXT_54);
    stringBuffer.append(hadoopCodec);
    
	}
}
    if(fileFormat.equals("sequencefile")){
    stringBuffer.append(TEXT_55);
    }
    if(fileFormat.equals("avrofile")){
    stringBuffer.append(TEXT_56);
    }
    if(fileFormat.equals("parquetfile")){
    stringBuffer.append(TEXT_57);
    }
    if(mysqlDelimiters){
    stringBuffer.append(TEXT_58);
    }
    if(useMappers){
    stringBuffer.append(TEXT_59);
    stringBuffer.append(mappers);
    }
    

if(excludeTable) {
	List<Map<String,String>> tablesToExclude = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__TABLE_TO_EXCLUDE__");
	if(tablesToExclude!=null && tablesToExclude.size()>0) {

    stringBuffer.append(TEXT_60);
    
		boolean first = true;	
		for(Map<String,String> tableToExclude : tablesToExclude) {
			if(first) {
				first = false;

    stringBuffer.append(TEXT_61);
    stringBuffer.append(tableToExclude.get("TABLE_NAME"));
    
			} else {

    stringBuffer.append(TEXT_62);
    stringBuffer.append(tableToExclude.get("TABLE_NAME"));
    
			}	
		}
	}
}

boolean isNull=false; 
if(additionalList != null){ 
    for(Map<String,String> additionalMap : additionalList){ 
        isNull=false; 
        if(additionalMap.get("ADDITIONAL_VALUE")==null || "".equals(additionalMap.get("ADDITIONAL_VALUE"))) { 
            isNull=true; 
        } 
        if(!isNull) { 

    stringBuffer.append(TEXT_63);
    stringBuffer.append(additionalMap.get("ADDITIONAL_ARGUMENT"));
    stringBuffer.append(TEXT_64);
    stringBuffer.append(additionalMap.get("ADDITIONAL_VALUE"));
    stringBuffer.append(TEXT_65);
     
        } else { 

    stringBuffer.append(TEXT_66);
    stringBuffer.append(additionalMap.get("ADDITIONAL_ARGUMENT"));
    stringBuffer.append(TEXT_67);
     
        } 
    } 
} 

if(overrideHiveMapping) {
	List<Map<String,String>> hiveMappings = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__HIVE_TYPE_MAPPING__");
	if(hiveMappings!=null && hiveMappings.size()>0) {

    stringBuffer.append(TEXT_68);
    	
		boolean first = true;	
		for(Map<String,String> hiveMapping : hiveMappings) {
			if(first) {
				first = false;

    stringBuffer.append(TEXT_69);
    stringBuffer.append(hiveMapping.get("COLUMN_NAME"));
    stringBuffer.append(TEXT_70);
    stringBuffer.append(hiveMapping.get("HIVE_TYPE"));
    
			} else {

    stringBuffer.append(TEXT_71);
    stringBuffer.append(hiveMapping.get("COLUMN_NAME"));
    stringBuffer.append(TEXT_72);
    stringBuffer.append(hiveMapping.get("HIVE_TYPE"));
    
			}	
		}
	}
}

    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(additionalCommandLineArguments);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    }
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid );
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
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    
	    if(printLog){

    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    
	    }

    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    }
    stringBuffer.append(TEXT_116);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    
	    if(printLog){

    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    
   	 }

    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    
	if(dieOnError) {

    stringBuffer.append(TEXT_133);
    		
	} else {

    stringBuffer.append(TEXT_134);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    }
	}

    stringBuffer.append(TEXT_137);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    
	if(dieOnError) {

    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    		
	}

    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    }
    stringBuffer.append(TEXT_151);
    
    } else {

    
	String distribution = ElementParameterParser.getValue(node, "__DISTRIBUTION__");
	String version = ElementParameterParser.getValue(node, "__DB_VERSION__");

	org.talend.hadoop.distribution.component.SqoopComponent sqoopDistrib = null;
	try {
		sqoopDistrib = (org.talend.hadoop.distribution.component.SqoopComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(distribution, version);
	} catch (java.lang.Exception e) {
		e.printStackTrace();
		return "";
	}

	boolean isCustom = sqoopDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;

	String processId = node.getProcess().getId();

	List<Map<String, String>> hadoopProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__HADOOP_ADVANCED_PROPERTIES__");

	String namenode = ElementParameterParser.getValue(node, "__FS_DEFAULT_NAME__");
	boolean passwordStoredInFile = "true".equals(ElementParameterParser.getValue(node, "__PASSWORD_STORED_IN_FILE__"));
	String jobtracker = ElementParameterParser.getValue(node, "__MAPRED_JOB_TRACKER__");
	boolean useKrb = "true".equals(ElementParameterParser.getValue(node, "__USE_KRB__"));
	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
	String username = ElementParameterParser.getValue(node,"__USERNAME__");
	boolean printLog = "true".equals(ElementParameterParser.getValue(node,"__PRINT_LOG__"));
	boolean verbose = "true".equals(ElementParameterParser.getValue(node,"__VERBOSE__"));
	
	boolean direct = "true".equals(ElementParameterParser.getValue(node,"__DIRECT__"));
	boolean splitInputStreamDirect = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_DIRECT_SPLIT_SIZE__"));
	String directSplitSize = ElementParameterParser.getValue(node, "__DIRECT_SPLIT_SIZE__");
	
	boolean compress = "true".equals(ElementParameterParser.getValue(node,"__COMPRESS__"));
	boolean useHadoopCodec = "true".equals(ElementParameterParser.getValue(node,"__DEFINE_HADOOP_CODEC__"));
	String hadoopCodec = ElementParameterParser.getValue(node,"__HADOOP_CODEC__");
	
	String fileFormat = ElementParameterParser.getValue(node,"__FILE_FORMAT__");
	boolean useMappers = "true".equals(ElementParameterParser.getValue(node,"__USE_MAPPERS__"));
	String mappers = ElementParameterParser.getValue(node,"__MAPPERS__");
	List<Map<String,String>> additionalList = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__ADDITIONAL_JAVA__");
	
	boolean overrideHiveMapping = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_HIVE_MAPPING__"));
	boolean excludeTable = "true".equals(ElementParameterParser.getValue(node, "__EXCLUDE_TABLE__"));
    boolean useDatanodeHostname = "true".equals(ElementParameterParser.getValue(node, "__USE_DATANODE_HOSTNAME__"));

    boolean useMapRTicket = ElementParameterParser.getBooleanValue(node, "__USE_MAPRTICKET__");
    String mapRTicketUsername = ElementParameterParser.getValue(node, "__MAPRTICKET_USERNAME__");
    String mapRTicketCluster = ElementParameterParser.getValue(node, "__MAPRTICKET_CLUSTER__");
    String mapRTicketDuration = ElementParameterParser.getValue(node, "__MAPRTICKET_DURATION__");

    boolean setMapRHomeDir = ElementParameterParser.getBooleanValue(node, "__SET_MAPR_HOME_DIR__");
    String mapRHomeDir = ElementParameterParser.getValue(node, "__MAPR_HOME_DIR__");

    boolean setMapRHadoopLogin = ElementParameterParser.getBooleanValue(node, "__SET_HADOOP_LOGIN__");
    String mapRHadoopLogin = ElementParameterParser.getValue(node, "__HADOOP_LOGIN__");

	String fsDefalutName = "fs.default.name";

	String passwordFieldName = "";
	
	boolean dieOnError = "true".equals(ElementParameterParser.getValue(node, "__DIE_ON_ERROR__"));	

    stringBuffer.append(TEXT_152);
    
	boolean useYarn = "true".equals(ElementParameterParser.getValue(node, "__USE_YARN__"));
	String resourceManager = ElementParameterParser.getValue(node, "__RESOURCE_MANAGER__");

	String yarnClasspathSeparator = ElementParameterParser.getValue(node, "__CLASSPATH_SEPARATOR__");

    stringBuffer.append(TEXT_153);
    stringBuffer.append(yarnClasspathSeparator);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_155);
    
	if(isCustom || (!isCustom && sqoopDistrib.doSupportImpersonation())) {
		String hadoopUser = ElementParameterParser.getValue(node, "__HADOOP_USER__");

    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_157);
    stringBuffer.append(hadoopUser);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_161);
    
	}

    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(fsDefalutName);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(namenode);
    stringBuffer.append(TEXT_166);
    

	if(!isCustom && ("MAPR401".equals(version) || "MAPR410".equals(version))) {//set the default properties

    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    
	}

	boolean isKerberosAvailableHadoop2 = !isCustom && sqoopDistrib.isHadoop2() && sqoopDistrib.doSupportKerberos();
	boolean isHadoop2 = !isCustom && sqoopDistrib.isHadoop2();
	boolean isKerberosAvailableHadoop1 = !isCustom && sqoopDistrib.isHadoop1() && sqoopDistrib.doSupportKerberos();

	if((isCustom && useYarn) || (!isCustom && isHadoop2)) {

    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(resourceManager);
    stringBuffer.append(TEXT_173);
    
		boolean setJobHistoryAddress = "true".equals(ElementParameterParser.getValue(node, "__SET_JOBHISTORY_ADDRESS__"));
		if(setJobHistoryAddress) {
			String jobHistoryAddress = ElementParameterParser.getValue(node,"__JOBHISTORY_ADDRESS__");
			
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(jobHistoryAddress);
    stringBuffer.append(TEXT_176);
    
		}

		boolean setSchedulerAddress = "true".equals(ElementParameterParser.getValue(node, "__SET_SCHEDULER_ADDRESS__"));
		if(setSchedulerAddress) {
			String schedulerAddress = ElementParameterParser.getValue(node,"__RESOURCEMANAGER_SCHEDULER_ADDRESS__");

    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(schedulerAddress);
    stringBuffer.append(TEXT_179);
    
		}
		boolean setStagingDirectory = "true".equals(ElementParameterParser.getValue(node, "__SET_STAGING_DIRECTORY__"));
		if(setStagingDirectory) {
			String stagingDirectory = ElementParameterParser.getValue(node, "__STAGING_DIRECTORY__");

    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(stagingDirectory);
    stringBuffer.append(TEXT_182);
    
		}

		boolean crossPlatformSubmission = "true".equals(ElementParameterParser.getValue(node, "__CROSS_PLATFORM_SUBMISSION__"));
		if((!isCustom && sqoopDistrib.doSupportCrossPlatformSubmission()) || (isCustom && useYarn && crossPlatformSubmission)) {

    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    
		}

		if(sqoopDistrib.doSupportCustomMRApplicationCP()){

    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(sqoopDistrib.getCustomMRApplicationCP());
    stringBuffer.append(TEXT_187);
    
		}


    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(sqoopDistrib.getYarnApplicationClasspath());
    stringBuffer.append(TEXT_190);
    

		boolean setMemory = "true".equals(ElementParameterParser.getValue(node, "__SET_MEMORY__"));
		if(setMemory) {
			String mapMemory = ElementParameterParser.getValue(node,"__MAPREDUCE_MAP_MEMORY_MB__");
			String reduceMemory = ElementParameterParser.getValue(node,"__MAPREDUCE_REDUCE_MEMORY_MB__");
			String amMemory = ElementParameterParser.getValue(node,"__YARN_APP_MAPREDUCE_AM_RESOURCE_MB__");

    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(mapMemory);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(reduceMemory);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(amMemory);
    stringBuffer.append(TEXT_197);
    
		}
	} else {

    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(jobtracker);
    stringBuffer.append(TEXT_200);
    
	}

    if(!isCustom && ("HDP_1_2".equals(version) || "HDP_1_3".equals(version))) {
        String mapMemory = ElementParameterParser.getValue(node,"__MAPRED_JOB_MAP_MEMORY_MB__");
        String reduceMemory = ElementParameterParser.getValue(node,"__MAPRED_JOB_REDUCE_MEMORY_MB__");

    stringBuffer.append(TEXT_201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(mapMemory);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(reduceMemory);
    stringBuffer.append(TEXT_205);
    
    }
	if(hadoopProps!=null && hadoopProps.size() > 0){
		for(Map<String, String> item : hadoopProps){

    stringBuffer.append(TEXT_206);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(item.get("PROPERTY") );
    stringBuffer.append(TEXT_208);
    stringBuffer.append(item.get("VALUE") );
    stringBuffer.append(TEXT_209);
    
		}
	}
	if(useKrb) {
		String namenodePrincipal = ElementParameterParser.getValue(node, "__NAMENODE_PRINCIPAL__");
		boolean useKeytab = "true".equals(ElementParameterParser.getValue(node, "__USE_KEYTAB__"));
		String userPrincipal = ElementParameterParser.getValue(node, "__PRINCIPAL__");
		String keytabPath = ElementParameterParser.getValue(node, "__KEYTAB_PATH__");

    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(namenodePrincipal);
    stringBuffer.append(TEXT_212);
    
		if(isKerberosAvailableHadoop1 || (isCustom && !useYarn)) {
			String jobTrackerPrincipal = ElementParameterParser.getValue(node, "__JOBTRACKER_PRINCIPAL__");

    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(jobTrackerPrincipal);
    stringBuffer.append(TEXT_215);
    
		}

		if(isKerberosAvailableHadoop2 || (isCustom && useYarn)) {
			String resourceManagerPrincipal = ElementParameterParser.getValue(node, "__RESOURCEMANAGER_PRINCIPAL__");
			String jobHistoryPrincipal = ElementParameterParser.getValue(node, "__JOBHISTORY_PRINCIPAL__");

    stringBuffer.append(TEXT_216);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(resourceManagerPrincipal);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(jobHistoryPrincipal);
    stringBuffer.append(TEXT_220);
    
		}
        if ((isCustom || sqoopDistrib.doSupportMapRTicket()) && useMapRTicket) {
            
    stringBuffer.append(TEXT_221);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_222);
    stringBuffer.append(setMapRHadoopLogin ? mapRHadoopLogin : "\"kerberos\"");
    stringBuffer.append(TEXT_223);
    
        }
		if(useKeytab) {

    stringBuffer.append(TEXT_224);
    stringBuffer.append(userPrincipal);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(keytabPath);
    stringBuffer.append(TEXT_226);
    
		}
		if ((isCustom || sqoopDistrib.doSupportMapRTicket()) && useMapRTicket) {
            
    stringBuffer.append(TEXT_227);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_231);
    
        }
	} else {
	    // MapR Ticket
	    if ((isCustom || sqoopDistrib.doSupportMapRTicket()) && useMapRTicket) {
            passwordFieldName = "__MAPRTICKET_PASSWORD__";
            
    stringBuffer.append(TEXT_232);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_233);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_234);
    
            if (setMapRHadoopLogin) {
                
    stringBuffer.append(TEXT_235);
    stringBuffer.append(mapRHadoopLogin);
    stringBuffer.append(TEXT_236);
    
            } else {
                
    stringBuffer.append(TEXT_237);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_238);
    
            }
            if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_239);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_241);
    } else {
    stringBuffer.append(TEXT_242);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_243);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_244);
    }
    stringBuffer.append(TEXT_245);
    
            if(sqoopDistrib.doSupportMaprTicketV52API()){
				
    stringBuffer.append(TEXT_246);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_251);
    
            } else {
				
    stringBuffer.append(TEXT_252);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_257);
    
            }
        }
	}

    if (useDatanodeHostname) {
        
    stringBuffer.append(TEXT_258);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_259);
    
    }

    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_264);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_265);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_267);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_269);
    if(!passwordStoredInFile || !sqoopDistrib.doJavaAPISupportStorePasswordInFile()) {
    stringBuffer.append(TEXT_270);
    
    		passwordFieldName = "__PASSWORD__";
    	
    stringBuffer.append(TEXT_271);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_272);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_273);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_274);
    } else {
    stringBuffer.append(TEXT_275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_276);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_277);
    }
    stringBuffer.append(TEXT_278);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_280);
    } else {
    stringBuffer.append(TEXT_281);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_282);
    stringBuffer.append((String)ElementParameterParser.getValue(node, "__PASSWORD_FILE__"));
    stringBuffer.append(TEXT_283);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(sqoopDistrib.doSupportFetchPasswordFromFile()?"FromFile":"");
    stringBuffer.append(TEXT_285);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_286);
    }
    stringBuffer.append(TEXT_287);
    if(direct){
    stringBuffer.append(TEXT_288);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_289);
    if(splitInputStreamDirect) {
    stringBuffer.append(TEXT_290);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(directSplitSize);
    stringBuffer.append(TEXT_292);
    }
	}
    stringBuffer.append(TEXT_293);
    if(compress){
    stringBuffer.append(TEXT_294);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_295);
    if(useHadoopCodec) {
    stringBuffer.append(TEXT_296);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_297);
    stringBuffer.append(hadoopCodec);
    stringBuffer.append(TEXT_298);
    } 
	}
    stringBuffer.append(TEXT_299);
    if(fileFormat.equals("sequencefile")){
    stringBuffer.append(TEXT_300);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_301);
    } else if(fileFormat.equals("textfile")){
    stringBuffer.append(TEXT_302);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_303);
    }
		else if(fileFormat.equals("parquetfile")){
    stringBuffer.append(TEXT_304);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_305);
    }
	 else {
    stringBuffer.append(TEXT_306);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_307);
    }
    stringBuffer.append(TEXT_308);
    if(useMappers){
    stringBuffer.append(TEXT_309);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_310);
    stringBuffer.append(mappers);
    stringBuffer.append(TEXT_311);
    }
    stringBuffer.append(TEXT_312);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_313);
    
	if(sqoopDistrib.isImpactedBySqoop2995()){
	//TODO, relate with https://issues.apache.org/jira/browse/SQOOP-2995, have to set a init value to the new map type key "customtool.options.jsonmap". But the key hasn't been used
	
    stringBuffer.append(TEXT_314);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_315);
    
	}
	if(additionalList != null){ 
		for(Map<String,String> additionalMap : additionalList){
			if(additionalMap.get("ADDITIONAL_VALUE")==null || "".equals(additionalMap.get("ADDITIONAL_VALUE"))) { 
				break; 
			}
	                String currentArgument = additionalMap.get("ADDITIONAL_ARGUMENT");
            		if(currentArgument.contains("delimiters") && !currentArgument.contains("required")) {

    stringBuffer.append(TEXT_316);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_317);
    stringBuffer.append(currentArgument);
    stringBuffer.append(TEXT_318);
    stringBuffer.append(additionalMap.get("ADDITIONAL_VALUE"));
    stringBuffer.append(TEXT_319);
    
			} else {

    stringBuffer.append(TEXT_320);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_321);
    stringBuffer.append(currentArgument);
    stringBuffer.append(TEXT_322);
    stringBuffer.append(additionalMap.get("ADDITIONAL_VALUE"));
    stringBuffer.append(TEXT_323);
    
			}
		} 
	}
	
	if(!isCustom && sqoopDistrib.doJavaAPISqoopImportAllTablesSupportExcludeTable()) {
		if(excludeTable) {
			List<Map<String,String>> tablesToExclude = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__TABLE_TO_EXCLUDE__");
			if(tablesToExclude!=null && tablesToExclude.size()>0) {

    stringBuffer.append(TEXT_324);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_325);
    
				boolean first = true;	
				for(Map<String,String> tableToExclude : tablesToExclude) {
					if(first) {
						first = false;

    stringBuffer.append(TEXT_326);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_327);
    stringBuffer.append(tableToExclude.get("TABLE_NAME"));
    stringBuffer.append(TEXT_328);
    
					} else {

    stringBuffer.append(TEXT_329);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_330);
    stringBuffer.append(tableToExclude.get("TABLE_NAME"));
    stringBuffer.append(TEXT_331);
    
					}	
				}

    stringBuffer.append(TEXT_332);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_333);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_334);
    
			}
		}
	} 

	if(overrideHiveMapping) {
		List<Map<String,String>> hiveMappings = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node,"__HIVE_TYPE_MAPPING__");
		if(hiveMappings!=null && hiveMappings.size()>0) {
	
    stringBuffer.append(TEXT_335);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_336);
    	
			boolean first = true;	
			for(Map<String,String> hiveMapping : hiveMappings) {
				if(first) {
					first = false;
	
    stringBuffer.append(TEXT_337);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_338);
    stringBuffer.append(hiveMapping.get("COLUMN_NAME"));
    stringBuffer.append(TEXT_339);
    stringBuffer.append(hiveMapping.get("HIVE_TYPE"));
    stringBuffer.append(TEXT_340);
    
				} else {
	
    stringBuffer.append(TEXT_341);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(hiveMapping.get("COLUMN_NAME"));
    stringBuffer.append(TEXT_343);
    stringBuffer.append(hiveMapping.get("HIVE_TYPE"));
    stringBuffer.append(TEXT_344);
    
				}	
			}
	
    stringBuffer.append(TEXT_345);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_346);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_347);
    
		}
	}
	
    stringBuffer.append(TEXT_348);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_349);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_350);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_351);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_352);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_353);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_354);
    
	// Register jars to handle the Avro format.
	
	boolean generateAddJarCodeForAll = "avrofile".equals(fileFormat);
	boolean emptyStringBuilder = true; // true when we don't import any dependency in the libjars.
		
	java.util.List<String> jarsToRegister = null;
	java.util.List<String> jars = null;
	if(generateAddJarCodeForAll) {
		String[] commandLine = new String[] {"<command>"};
		try {
			commandLine = ProcessorUtilities.getCommandLine("win32",true, processId, "",org.talend.designer.runprocess.IProcessor.NO_STATISTICS,org.talend.designer.runprocess.IProcessor.NO_TRACES, new String[]{});
		} catch (ProcessorException e) {
			e.printStackTrace();
		}
	
		jarsToRegister = new java.util.ArrayList();
		jarsToRegister.add("avro-");
	
		for (int j = 0; j < commandLine.length; j++) {
			if(commandLine[j].contains("jar")) {
				jars = java.util.Arrays.asList(commandLine[j].split(";"));
				break;
			}
		}
	}
		
	if(jarsToRegister!=null && jars!=null) {

    stringBuffer.append(TEXT_355);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_356);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_357);
    
		
		for(int i=0; i<jarsToRegister.size(); i++) {
			String jarToRegister = jarsToRegister.get(i);
			for(int j=0; j<jars.size(); j++) {
				if(jars.get(j).contains(jarToRegister)) {
					emptyStringBuilder = false;

    stringBuffer.append(TEXT_358);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_359);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_360);
    stringBuffer.append(jars.get(j));
    stringBuffer.append(TEXT_361);
    
				}
			}
		}
	}

    stringBuffer.append(TEXT_362);
    
	// Register jars to handle the Parquet format.
	
	boolean parquetGenerateAddJarCodeForAll = "parquetfile".equals(fileFormat);
	boolean parquetEmptyStringBuilder = true; // true when we don't import any dependency in the libjars.
		
	java.util.List<String> parquetJarsToRegister = null;
	java.util.List<String> parquetJars = null;
	if(parquetGenerateAddJarCodeForAll) {
		String[] commandLine = new String[] {"<command>"};
		try {
			commandLine = ProcessorUtilities.getCommandLine("win32",true, processId, "",org.talend.designer.runprocess.IProcessor.NO_STATISTICS,org.talend.designer.runprocess.IProcessor.NO_TRACES, new String[]{});
		} catch (ProcessorException e) {
			e.printStackTrace();
		}
	
		parquetJarsToRegister = new java.util.ArrayList();
		parquetJarsToRegister.add("kite-data-core-");
		parquetJarsToRegister.add("kite-data-mapreduce-");
		parquetJarsToRegister.add("kite-hadoop-compatibility-");
		parquetJarsToRegister.add("jackson-databind-");
		parquetJarsToRegister.add("jackson-core-");
		parquetJarsToRegister.add("parquet-");
	
		for (int j = 0; j < commandLine.length; j++) {
			if(commandLine[j].contains("jar")) {
				parquetJars = java.util.Arrays.asList(commandLine[j].split(";"));
				break;
			}
		}
	}
		
	if(parquetJarsToRegister!=null && parquetJars!=null) {

    stringBuffer.append(TEXT_363);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_364);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_365);
    
		
		for(int i=0; i<parquetJarsToRegister.size(); i++) {
			String parquetJarToRegister = parquetJarsToRegister.get(i);
			for(int j=0; j<parquetJars.size(); j++) {
				if(parquetJars.get(j).contains(parquetJarToRegister)) {
					emptyStringBuilder = false;

    stringBuffer.append(TEXT_366);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_368);
    stringBuffer.append(parquetJars.get(j));
    stringBuffer.append(TEXT_369);
    
				}
			}
		}
	}

    stringBuffer.append(TEXT_370);
    
		if(!emptyStringBuilder) {

    stringBuffer.append(TEXT_371);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_372);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_373);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_374);
    
		} else {

    stringBuffer.append(TEXT_375);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_376);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_377);
    
		}

    stringBuffer.append(TEXT_378);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_379);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_380);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_381);
    
			if(dieOnError) {

    stringBuffer.append(TEXT_382);
    
			} else {

    stringBuffer.append(TEXT_383);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_384);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_385);
    }
			}

    stringBuffer.append(TEXT_386);
    
		if(dieOnError) {

    stringBuffer.append(TEXT_387);
    
		} else {

    stringBuffer.append(TEXT_388);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_389);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_390);
    }
		}

    stringBuffer.append(TEXT_391);
    stringBuffer.append(TEXT_392);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_393);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_394);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_395);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_396);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_397);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_398);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_399);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_400);
    
    }
    

    return stringBuffer.toString();
  }
}
