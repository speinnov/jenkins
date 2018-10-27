package org.talend.designer.codegen.translators.databases.ms_sql_server;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.builder.database.ExtractMetaDataUtils;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import java.util.Map;
import java.util.List;

public class TMSSqlInvalidRowsBeginJava
{
  protected static String nl;
  public static synchronized TMSSqlInvalidRowsBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMSSqlInvalidRowsBeginJava result = new TMSSqlInvalidRowsBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_4 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_5 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_6 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_7 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_8 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_9 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_10 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_11 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_12 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_13 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_14 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_15 = " - Written records count: \" + nb_line_";
  protected final String TEXT_16 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_17 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_18 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_19 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_20 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_21 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_22 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_23 = " - Writing the record \" + nb_line_";
  protected final String TEXT_24 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_25 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_26 = " - Processing the record \" + nb_line_";
  protected final String TEXT_27 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_28 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_29 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_30 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_31 = NL + "\t\t\t\tif(conn_";
  protected final String TEXT_32 = " != null) {" + NL + "\t\t\t\t\tif(conn_";
  protected final String TEXT_33 = ".getMetaData() != null) {" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_34 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_35 = " - Uses an existing connection ";
  protected final String TEXT_36 = ".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_37 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_38 = " - Uses an existing connection with username '\" + conn_";
  protected final String TEXT_39 = ".getMetaData().getUserName() + \"'. Connection URL: \" + conn_";
  protected final String TEXT_40 = ".getMetaData().getURL() + \".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_41 = NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_42 = NL + "\t\t\tconn_";
  protected final String TEXT_43 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_44 = ", dbUser_";
  protected final String TEXT_45 = ", dbPwd_";
  protected final String TEXT_46 = ");" + NL + "\t\t\t";
  protected final String TEXT_47 = NL + "\t\t\tconn_";
  protected final String TEXT_48 = ".rollback();" + NL + "\t\t\t";
  protected final String TEXT_49 = NL + "\t\t\tconn_";
  protected final String TEXT_50 = ".commit();" + NL + "\t\t\t";
  protected final String TEXT_51 = NL + "\t\t\tconn_";
  protected final String TEXT_52 = ".close();" + NL + "\t\t\t";
  protected final String TEXT_53 = NL + "\t\t\t\tconn_";
  protected final String TEXT_54 = ".setAutoCommit(";
  protected final String TEXT_55 = ");" + NL + "\t\t\t";
  protected final String TEXT_56 = NL + "\t\t\t\tlog.";
  protected final String TEXT_57 = "(\"";
  protected final String TEXT_58 = " - \" + ";
  protected final String TEXT_59 = ".getMessage());" + NL + "\t\t\t";
  protected final String TEXT_60 = NL + "\t    \t\tlog.";
  protected final String TEXT_61 = "(\"";
  protected final String TEXT_62 = "\");" + NL + "\t\t\t";
  protected final String TEXT_63 = NL + "\t\t\t\tpstmt_";
  protected final String TEXT_64 = ".executeBatch();" + NL + "\t\t\t";
  protected final String TEXT_65 = NL + "\t\t\t\tint countSum_";
  protected final String TEXT_66 = " = 0;" + NL + "\t\t\t\tfor(int countEach_";
  protected final String TEXT_67 = ": pstmt_";
  protected final String TEXT_68 = ".executeBatch()) {" + NL + "\t\t\t\t\tcountSum_";
  protected final String TEXT_69 = " += (countEach_";
  protected final String TEXT_70 = " < 0 ? 0 : ";
  protected final String TEXT_71 = ");" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_72 = NL + "            java.util.Map<String, routines.system.TalendDataSource> dataSources_";
  protected final String TEXT_73 = " = (java.util.Map<String, routines.system.TalendDataSource>) globalMap.get(KEY_DB_DATASOURCES);" + NL + "            if (null != dataSources_";
  protected final String TEXT_74 = ") {";
  protected final String TEXT_75 = NL + "    \t\t\tString dsAlias_";
  protected final String TEXT_76 = " = ";
  protected final String TEXT_77 = ";" + NL + "        \t\tif (dataSources_";
  protected final String TEXT_78 = ".get(dsAlias_";
  protected final String TEXT_79 = ") == null) {" + NL + "           \t\t\tthrow new RuntimeException(\"No DataSource with alias: \" + dsAlias_";
  protected final String TEXT_80 = " + \" available!\");" + NL + "              \t}" + NL + "        \t\tconn_";
  protected final String TEXT_81 = " = dataSources_";
  protected final String TEXT_82 = ".get(dsAlias_";
  protected final String TEXT_83 = ").getConnection();" + NL + "            } else {";
  protected final String TEXT_84 = NL + "\t\t\t\tString driverClass_";
  protected final String TEXT_85 = " = \"";
  protected final String TEXT_86 = "\";" + NL + "\t\t\t    java.lang.Class.forName(driverClass_";
  protected final String TEXT_87 = ");" + NL + "\t\t\t   \tString dbUser_";
  protected final String TEXT_88 = " = ";
  protected final String TEXT_89 = ";" + NL + "\t\t\t   \t" + NL + "        \t\t";
  protected final String TEXT_90 = NL + "        \t\t" + NL + "        \t\t";
  protected final String TEXT_91 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_92 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_93 = ");";
  protected final String TEXT_94 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_95 = " = ";
  protected final String TEXT_96 = "; ";
  protected final String TEXT_97 = NL + "\t\t\t   \t" + NL + "\t\t        String dbPwd_";
  protected final String TEXT_98 = " = decryptedPassword_";
  protected final String TEXT_99 = ";" + NL + "\t\t        " + NL + "\t\t\t\t";
  protected final String TEXT_100 = NL + "\t\t\t\t";
  protected final String TEXT_101 = NL + "\t\t\t\tconn_";
  protected final String TEXT_102 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_103 = ",dbUser_";
  protected final String TEXT_104 = ",dbPwd_";
  protected final String TEXT_105 = ");";
  protected final String TEXT_106 = NL + "\t\t\t\tconn_";
  protected final String TEXT_107 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_108 = ");";
  protected final String TEXT_109 = NL + "\t\t        ";
  protected final String TEXT_110 = NL + "\t\t    }";
  protected final String TEXT_111 = NL + "\t\t\tjava.sql.Statement stmt_";
  protected final String TEXT_112 = " = conn_";
  protected final String TEXT_113 = ".createStatement();";
  protected final String TEXT_114 = NL + "\t\t\tjava.sql.Statement stmt_";
  protected final String TEXT_115 = " = conn_";
  protected final String TEXT_116 = ".createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY," + NL + "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tjava.sql.ResultSet.CONCUR_READ_ONLY);" + NL + "\t\t\t";
  protected final String TEXT_117 = NL + "\t\t\t\t\ttmpContent_";
  protected final String TEXT_118 = " = rs_";
  protected final String TEXT_119 = ".getString(";
  protected final String TEXT_120 = "column_index_";
  protected final String TEXT_121 = ");";
  protected final String TEXT_122 = NL + "                        if(tmpContent_";
  protected final String TEXT_123 = " != null) {" + NL + "                            tmpContent_";
  protected final String TEXT_124 = " = tmpContent_";
  protected final String TEXT_125 = ";" + NL + "                        }";
  protected final String TEXT_126 = NL + "                    if(tmpContent_";
  protected final String TEXT_127 = " != null && tmpContent_";
  protected final String TEXT_128 = ".length() > 0) {\t\t\t  \t";
  protected final String TEXT_129 = NL + "                        ";
  protected final String TEXT_130 = ".";
  protected final String TEXT_131 = " = tmpContent_";
  protected final String TEXT_132 = ".charAt(0);\t\t\t  \t\t" + NL + "                    } else {\t\t\t  \t";
  protected final String TEXT_133 = "\t\t\t  \t    " + NL + "                            if(tmpContent_";
  protected final String TEXT_134 = " == null) {\t\t\t  \t   \t";
  protected final String TEXT_135 = NL + "                                ";
  protected final String TEXT_136 = ".";
  protected final String TEXT_137 = " = null;\t\t\t  \t\t\t" + NL + "                            } else {\t\t\t  \t\t";
  protected final String TEXT_138 = NL + "                                ";
  protected final String TEXT_139 = ".";
  protected final String TEXT_140 = " = '\\0';\t\t\t  \t\t\t" + NL + "                            }";
  protected final String TEXT_141 = "\t\t\t  \t\t" + NL + "                            if((\"\").equals(tmpContent_";
  protected final String TEXT_142 = ")) {\t\t\t  \t\t";
  protected final String TEXT_143 = NL + "                                ";
  protected final String TEXT_144 = ".";
  protected final String TEXT_145 = " = '\\0';\t\t\t  \t\t\t" + NL + "                            } else {" + NL + "        \t\t\t  \t\t\tthrow new RuntimeException(" + NL + "        \t\t\t\t\t\t\t\"Value is empty for column : '";
  protected final String TEXT_146 = "' in '";
  protected final String TEXT_147 = "' connection, value is invalid or this column should be nullable or have a default value.\");\t\t\t\t\t\t\t" + NL + "                            }\t\t\t  \t\t";
  protected final String TEXT_148 = NL + "                    }";
  protected final String TEXT_149 = NL + "\t\t\t";
  protected final String TEXT_150 = ".";
  protected final String TEXT_151 = " = routines.system.JDBCUtil.getDate(rs_";
  protected final String TEXT_152 = ", ";
  protected final String TEXT_153 = "column_index_";
  protected final String TEXT_154 = ");";
  protected final String TEXT_155 = NL + "        \t";
  protected final String TEXT_156 = ".";
  protected final String TEXT_157 = " = routines.system.JDBCUtil.getString(rs_";
  protected final String TEXT_158 = ", ";
  protected final String TEXT_159 = "column_index_";
  protected final String TEXT_160 = ", ";
  protected final String TEXT_161 = "false";
  protected final String TEXT_162 = "true";
  protected final String TEXT_163 = ");";
  protected final String TEXT_164 = NL + "            if(rs_";
  protected final String TEXT_165 = ".getObject(";
  protected final String TEXT_166 = "column_index_";
  protected final String TEXT_167 = ") != null) {";
  protected final String TEXT_168 = NL + "                ";
  protected final String TEXT_169 = ".";
  protected final String TEXT_170 = " = rs_";
  protected final String TEXT_171 = ".get";
  protected final String TEXT_172 = "(";
  protected final String TEXT_173 = "column_index_";
  protected final String TEXT_174 = ");" + NL + "            } else {";
  protected final String TEXT_175 = NL + "                    ";
  protected final String TEXT_176 = ".";
  protected final String TEXT_177 = " = null;";
  protected final String TEXT_178 = NL + "                    throw new RuntimeException(\"Null value in non-Nullable column\");";
  protected final String TEXT_179 = NL + "            }";
  protected final String TEXT_180 = NL;
  protected final String TEXT_181 = NL;
  protected final String TEXT_182 = NL + "\t\t\tjava.util.Calendar calendar_";
  protected final String TEXT_183 = " = java.util.Calendar.getInstance();" + NL + "\t\t\tcalendar_";
  protected final String TEXT_184 = ".set(0, 0, 0, 0, 0, 0);" + NL + "\t\t\tjava.util.Date year0_";
  protected final String TEXT_185 = " = calendar_";
  protected final String TEXT_186 = ".getTime();";
  protected final String TEXT_187 = NL + "\t\t\t\tString url_";
  protected final String TEXT_188 = " = \"";
  protected final String TEXT_189 = "://\" + ";
  protected final String TEXT_190 = " + \":\" + ";
  protected final String TEXT_191 = " + \"";
  protected final String TEXT_192 = "\"+";
  protected final String TEXT_193 = ";";
  protected final String TEXT_194 = NL + "\t\t\t\tString url_";
  protected final String TEXT_195 = " = \"";
  protected final String TEXT_196 = "://\" + ";
  protected final String TEXT_197 = " + \":\" + ";
  protected final String TEXT_198 = " +  \"";
  protected final String TEXT_199 = "\"+";
  protected final String TEXT_200 = " + \";\" + ";
  protected final String TEXT_201 = ";";
  protected final String TEXT_202 = NL + "\t\t\t\t((com.mysql.jdbc.Statement)stmt_";
  protected final String TEXT_203 = ").enableStreamingResults();";
  protected final String TEXT_204 = NL + "\t\t\tif(rs_";
  protected final String TEXT_205 = ".getString(";
  protected final String TEXT_206 = ") != null) {" + NL + "\t\t\t\tString dateString_";
  protected final String TEXT_207 = " = rs_";
  protected final String TEXT_208 = ".getString(";
  protected final String TEXT_209 = ");" + NL + "\t\t\t\tif (!(\"0000-00-00\").equals(dateString_";
  protected final String TEXT_210 = ") && !(\"0000-00-00 00:00:00\").equals(dateString_";
  protected final String TEXT_211 = ")) {" + NL + "\t\t\t\t\t";
  protected final String TEXT_212 = ".";
  protected final String TEXT_213 = " = rs_";
  protected final String TEXT_214 = ".getTimestamp(";
  protected final String TEXT_215 = ");" + NL + "\t\t\t\t} else {" + NL + "\t\t\t\t\t";
  protected final String TEXT_216 = ".";
  protected final String TEXT_217 = " = (java.util.Date) year0_";
  protected final String TEXT_218 = ".clone();" + NL + "\t\t\t\t}" + NL + "\t\t\t} else {" + NL + "\t\t\t\t";
  protected final String TEXT_219 = ".";
  protected final String TEXT_220 = " =  null;" + NL + "\t\t\t}\t\t\t";
  protected final String TEXT_221 = "\t" + NL;
  protected final String TEXT_222 = NL + "\t\t    int nb_line_";
  protected final String TEXT_223 = " = 0;" + NL + "\t\t    java.sql.Connection conn_";
  protected final String TEXT_224 = " = null;";
  protected final String TEXT_225 = NL + "\t\t        conn_";
  protected final String TEXT_226 = " = (java.sql.Connection)globalMap.get(\"";
  protected final String TEXT_227 = "\");" + NL + "\t\t\t\t";
  protected final String TEXT_228 = NL + "\t\t\t\t";
  protected final String TEXT_229 = NL + "             boolean currentAutoCommitMode";
  protected final String TEXT_230 = " =  conn_";
  protected final String TEXT_231 = ".getAutoCommit();" + NL + "             conn_";
  protected final String TEXT_232 = ".setAutoCommit(true);" + NL + "             try{";
  protected final String TEXT_233 = NL + "                           try{" + NL + "                               conn_";
  protected final String TEXT_234 = ".createStatement().execute(\"BEGIN QUERY LOGGING WITH SQL,OBJECTS ON \" + ";
  protected final String TEXT_235 = ");" + NL + "                           }catch(Exception e){" + NL + "                                 System.err.println(e.getMessage());" + NL + "                           }";
  protected final String TEXT_236 = NL + "                 conn_";
  protected final String TEXT_237 = ".createStatement().execute(" + NL + "              \"SET QUERY_BAND='\" +";
  protected final String TEXT_238 = NL + "               ";
  protected final String TEXT_239 = " + \"=\" + ";
  protected final String TEXT_240 = " + \";\" +";
  protected final String TEXT_241 = " \"' FOR SESSION\"" + NL + "           );" + NL + "              }catch(Exception e){" + NL + "               System.err.println(e.getMessage());" + NL + "      }" + NL + "      conn_";
  protected final String TEXT_242 = ".setAutoCommit(currentAutoCommitMode";
  protected final String TEXT_243 = ");";
  protected final String TEXT_244 = NL + "\t\t    ";
  protected final String TEXT_245 = NL + NL + "\t\t    String dbquery_";
  protected final String TEXT_246 = " = ";
  protected final String TEXT_247 = ";" + NL + "\t\t\t";
  protected final String TEXT_248 = NL + "                log.debug(\"";
  protected final String TEXT_249 = " - Executing the query: '\"+dbquery_";
  protected final String TEXT_250 = "+\"'.\");" + NL + "\t\t\t";
  protected final String TEXT_251 = NL;
  protected final String TEXT_252 = NL + "                       globalMap.put(\"";
  protected final String TEXT_253 = "_QUERY\",dbquery_";
  protected final String TEXT_254 = ");";
  protected final String TEXT_255 = NL + "                       globalMap.put(\"";
  protected final String TEXT_256 = "_QUERY\",dbquery_";
  protected final String TEXT_257 = ");";
  protected final String TEXT_258 = NL + NL + "\t\t    java.sql.ResultSet rs_";
  protected final String TEXT_259 = " = null;" + NL + "\t\ttry{" + NL + "\t\t    rs_";
  protected final String TEXT_260 = " = stmt_";
  protected final String TEXT_261 = ".executeQuery(dbquery_";
  protected final String TEXT_262 = ");" + NL + "\t\t    java.sql.ResultSetMetaData rsmd_";
  protected final String TEXT_263 = " = rs_";
  protected final String TEXT_264 = ".getMetaData();" + NL + "\t\t    int colQtyInRs_";
  protected final String TEXT_265 = " = rsmd_";
  protected final String TEXT_266 = ".getColumnCount();" + NL;
  protected final String TEXT_267 = NL + "\t\t    routines.system.Dynamic dcg_";
  protected final String TEXT_268 = " =  new routines.system.Dynamic();" + NL + "\t\t    dcg_";
  protected final String TEXT_269 = ".setDbmsId(\"";
  protected final String TEXT_270 = "\");" + NL + "\t\t    List<String> listSchema_";
  protected final String TEXT_271 = "=new java.util.ArrayList<String>();" + NL + "\t\t    ";
  protected final String TEXT_272 = NL + "\t\t    \t    listSchema_";
  protected final String TEXT_273 = ".add(\"";
  protected final String TEXT_274 = "\");" + NL + "\t    \t    ";
  protected final String TEXT_275 = NL + NL + "\t\t\tint fixedColumnCount_";
  protected final String TEXT_276 = " = ";
  protected final String TEXT_277 = ";" + NL + "" + NL + "            for (int i = ";
  protected final String TEXT_278 = "; i <= rsmd_";
  protected final String TEXT_279 = ".getColumnCount()-";
  protected final String TEXT_280 = "; i++) {" + NL + "                if (!(listSchema_";
  protected final String TEXT_281 = ".contains(rsmd_";
  protected final String TEXT_282 = ".getColumnLabel(i).toUpperCase()) )) {" + NL + "                \troutines.system.DynamicMetadata dcm_";
  protected final String TEXT_283 = "=new routines.system.DynamicMetadata();" + NL + "                \tdcm_";
  protected final String TEXT_284 = ".setName(rsmd_";
  protected final String TEXT_285 = ".getColumnLabel(i));" + NL + "                \tdcm_";
  protected final String TEXT_286 = ".setDbName(rsmd_";
  protected final String TEXT_287 = ".getColumnName(i));" + NL + "                \tdcm_";
  protected final String TEXT_288 = ".setType(routines.system.Dynamic.getTalendTypeFromDBType(\"";
  protected final String TEXT_289 = "\", rsmd_";
  protected final String TEXT_290 = ".getColumnTypeName(i).toUpperCase(), rsmd_";
  protected final String TEXT_291 = ".getPrecision(i), rsmd_";
  protected final String TEXT_292 = ".getScale(i)));" + NL + "                \tdcm_";
  protected final String TEXT_293 = ".setDbType(rsmd_";
  protected final String TEXT_294 = ".getColumnTypeName(i));" + NL + "                \tdcm_";
  protected final String TEXT_295 = ".setDbTypeId(rsmd_";
  protected final String TEXT_296 = ".getColumnType(i));";
  protected final String TEXT_297 = NL + "                \tdcm_";
  protected final String TEXT_298 = ".setFormat(";
  protected final String TEXT_299 = ");";
  protected final String TEXT_300 = NL + "\t\t\tif(\"LONG\".equals(rsmd_";
  protected final String TEXT_301 = ".getColumnTypeName(i).toUpperCase())) {" + NL + "\t\t\t\tString length = MetadataTalendType.getDefaultDBTypes(\"oracle_id\", \"LONG\", MetadataTalendType.DEFAULT_LENGTH);" + NL + "\t\t\t\tif(length!=null && !(\"\".equals(length))) {" + NL + "\t\t\t\t\tdcm_";
  protected final String TEXT_302 = ".setLength(Integer.parseInt(length));" + NL + "\t\t\t\t} else {" + NL + "\t\t\t\t\tdcm_";
  protected final String TEXT_303 = ".setLength(rsmd_";
  protected final String TEXT_304 = ".getPrecision(i));" + NL + "\t\t\t\t}" + NL + "\t\t\t} else {" + NL + "\t\t\t\tdcm_";
  protected final String TEXT_305 = ".setLength(rsmd_";
  protected final String TEXT_306 = ".getPrecision(i));" + NL + "\t\t\t}";
  protected final String TEXT_307 = NL + "\t\t\tdcm_";
  protected final String TEXT_308 = ".setLength(rsmd_";
  protected final String TEXT_309 = ".getPrecision(i));";
  protected final String TEXT_310 = NL + "                \tdcm_";
  protected final String TEXT_311 = ".setPrecision(rsmd_";
  protected final String TEXT_312 = ".getScale(i));" + NL + "                \tdcm_";
  protected final String TEXT_313 = ".setNullable(rsmd_";
  protected final String TEXT_314 = ".isNullable(i) == 0 ? false : true);" + NL + "                \tdcm_";
  protected final String TEXT_315 = ".setKey(false);" + NL + "                \tdcm_";
  protected final String TEXT_316 = ".setSourceType(DynamicMetadata.sourceTypes.database);" + NL + "                \tdcm_";
  protected final String TEXT_317 = ".setColumnPosition(i);" + NL + "                \tdcg_";
  protected final String TEXT_318 = ".metadatas.add(dcm_";
  protected final String TEXT_319 = ");" + NL + "                }" + NL + "            }";
  protected final String TEXT_320 = NL + "\t\t    String tmpContent_";
  protected final String TEXT_321 = " = null;" + NL + "\t\t    ";
  protected final String TEXT_322 = NL + "\t\t    \tint column_index_";
  protected final String TEXT_323 = " =1;" + NL + "\t\t    ";
  protected final String TEXT_324 = NL + "\t\t    ";
  protected final String TEXT_325 = NL + "\t\t    \tlog.debug(\"";
  protected final String TEXT_326 = " - Retrieving records from the database.\");" + NL + "\t\t    ";
  protected final String TEXT_327 = NL + "\t\t    while (rs_";
  protected final String TEXT_328 = ".next()) {" + NL + "\t\t        nb_line_";
  protected final String TEXT_329 = "++;" + NL + "\t\t        ";
  protected final String TEXT_330 = NL + "\t\t\t\t\t\t\t\t\tcolumn_index_";
  protected final String TEXT_331 = " = ";
  protected final String TEXT_332 = " + dcg_";
  protected final String TEXT_333 = ".getColumnCount();" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_334 = NL + "\t\t\t\t\t\t\t\t\tcolumn_index_";
  protected final String TEXT_335 = " = ";
  protected final String TEXT_336 = ";" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_337 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_338 = NL + "\t\t\t\t\t\t\tif(colQtyInRs_";
  protected final String TEXT_339 = " < ";
  protected final String TEXT_340 = "column_index_";
  protected final String TEXT_341 = ") {" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_342 = ".";
  protected final String TEXT_343 = " = ";
  protected final String TEXT_344 = ";" + NL + "\t\t\t\t\t\t\t} else {";
  protected final String TEXT_345 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_346 = NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_347 = NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_348 = NL + "\t\t                            ";
  protected final String TEXT_349 = ".";
  protected final String TEXT_350 = " = (List)rs_";
  protected final String TEXT_351 = ".getObject(";
  protected final String TEXT_352 = "column_index_";
  protected final String TEXT_353 = ");";
  protected final String TEXT_354 = NL + "\t                         \t\t";
  protected final String TEXT_355 = NL + "\t                         \t\t";
  protected final String TEXT_356 = NL + "\t                         \t\t";
  protected final String TEXT_357 = NL + "                                    oracle.sql.STRUCT jGeomStruct = (oracle.sql.STRUCT) rs_";
  protected final String TEXT_358 = ".getObject(";
  protected final String TEXT_359 = "column_index_";
  protected final String TEXT_360 = ");" + NL + "                                    if (jGeomStruct != null) {" + NL + "                                    oracle.spatial.geometry.JGeometry jGeom = oracle.spatial.geometry.JGeometry.load(jGeomStruct);" + NL + "                                    oracle.spatial.util.WKT wkt = new oracle.spatial.util.WKT();" + NL + "                                    String wktValue = new String(wkt.fromJGeometry(jGeom));" + NL;
  protected final String TEXT_361 = NL + "                                    ";
  protected final String TEXT_362 = ".";
  protected final String TEXT_363 = " = new Geometry(wktValue);" + NL + "                                    } else {";
  protected final String TEXT_364 = NL + "                                      ";
  protected final String TEXT_365 = ".";
  protected final String TEXT_366 = " = null;" + NL + "                                    }";
  protected final String TEXT_367 = NL + "                                        if (";
  protected final String TEXT_368 = ".";
  protected final String TEXT_369 = " != null) {";
  protected final String TEXT_370 = NL + "                                        ";
  protected final String TEXT_371 = ".";
  protected final String TEXT_372 = ".setEPSG(";
  protected final String TEXT_373 = ");" + NL + "                                        }";
  protected final String TEXT_374 = NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_375 = NL + "                                  ";
  protected final String TEXT_376 = ".";
  protected final String TEXT_377 = "=dcg_";
  protected final String TEXT_378 = ";";
  protected final String TEXT_379 = NL + "\t\t\t\t\t\t\t\t\t\tList<String> list_";
  protected final String TEXT_380 = " = new java.util.ArrayList<String>();" + NL + "\t\t\t\t\t    \t\t\t\tfor(int i_";
  protected final String TEXT_381 = " = ";
  protected final String TEXT_382 = "; i_";
  protected final String TEXT_383 = "  <= rsmd_";
  protected final String TEXT_384 = ".getColumnCount(); i_";
  protected final String TEXT_385 = " ++){" + NL + "\t\t\t\t\t    \t\t\t\t\t";
  protected final String TEXT_386 = NL + "\t\t\t\t\t\t \t\t\t\t\tif (\"NTEXT\".equals(rsmd_";
  protected final String TEXT_387 = ".getColumnTypeName(i_";
  protected final String TEXT_388 = " ).toUpperCase())) {" + NL + "\t\t\t\t\t\t\t\t\t\t\t\tnet.sourceforge.jtds.jdbc.ClobImpl clob_";
  protected final String TEXT_389 = " = (net.sourceforge.jtds.jdbc.ClobImpl) rs_";
  protected final String TEXT_390 = ".getClob(i_";
  protected final String TEXT_391 = ");" + NL + "\t\t\t\t\t\t\t\t\t\t\t\tif(clob_";
  protected final String TEXT_392 = "!=null){" + NL + "\t\t\t\t\t\t\t\t\t\t\t\t\tnet.sourceforge.jtds.jdbc.TalendNTextImpl tNTextImpl_";
  protected final String TEXT_393 = " = new net.sourceforge.jtds.jdbc.TalendNTextImpl(clob_";
  protected final String TEXT_394 = ");" + NL + "\t\t\t\t\t\t\t  \t\t\t\t\t\tlist_";
  protected final String TEXT_395 = ".add(tNTextImpl_";
  protected final String TEXT_396 = ".getValue());" + NL + "\t\t\t\t\t\t\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t\t\t\t\t\t\tlist_";
  protected final String TEXT_397 = ".add(null);" + NL + "\t\t\t\t\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t   \t\t\t\t\t}" + NL + "\t\t\t\t\t\t   \t\t\t\t\t";
  protected final String TEXT_398 = " " + NL + "\t\t\t\t\t\t   \t\t\t\t\tif(\"NTEXT\".equals(rsmd_";
  protected final String TEXT_399 = ".getColumnTypeName(i_";
  protected final String TEXT_400 = " ).toUpperCase())) {" + NL + "\t\t\t\t\t\t   \t\t\t\t\t\tlist_";
  protected final String TEXT_401 = ".add(rs_";
  protected final String TEXT_402 = ".getString(i_";
  protected final String TEXT_403 = "));" + NL + "\t\t\t\t\t\t   \t\t\t\t\t}" + NL + "\t\t\t\t\t\t   \t\t\t\t\t";
  protected final String TEXT_404 = NL + "   \t \t\t\t\t    \t\t\t\t}" + NL + "                                 \t\t routines.system.DynamicUtils.readColumnsFromDatabase_Mssql(";
  protected final String TEXT_405 = ".";
  protected final String TEXT_406 = ", rs_";
  protected final String TEXT_407 = ", fixedColumnCount_";
  protected final String TEXT_408 = ",list_";
  protected final String TEXT_409 = ",";
  protected final String TEXT_410 = ");";
  protected final String TEXT_411 = NL + "\t\t\t\t\t\t\t\t\t\troutines.system.DynamicUtils.readColumnsFromDatabase_Access(";
  protected final String TEXT_412 = ".";
  protected final String TEXT_413 = ", rs_";
  protected final String TEXT_414 = ", fixedColumnCount_";
  protected final String TEXT_415 = ",";
  protected final String TEXT_416 = ");";
  protected final String TEXT_417 = NL + "                                \t\t routines.system.DynamicUtils.readColumnsFromDatabase(";
  protected final String TEXT_418 = ".";
  protected final String TEXT_419 = ", rs_";
  protected final String TEXT_420 = ", fixedColumnCount_";
  protected final String TEXT_421 = ",";
  protected final String TEXT_422 = ");";
  protected final String TEXT_423 = NL + "                                  Object geom = rs_";
  protected final String TEXT_424 = ".getObject(";
  protected final String TEXT_425 = "column_index_";
  protected final String TEXT_426 = ");" + NL + "                                  if (geom != null) {" + NL + "                                  \torg.postgis.Geometry o =" + NL + "                                      org.postgis.PGgeometry.geomFromString(geom.toString());" + NL + "                                \tStringBuffer sb = new StringBuffer();" + NL + "                                \to.outerWKT(sb, false);" + NL + "                                \t";
  protected final String TEXT_427 = ".";
  protected final String TEXT_428 = " = new Geometry(sb.toString());" + NL + "                                  } else {" + NL + "                                  \t";
  protected final String TEXT_429 = ".";
  protected final String TEXT_430 = " = null;" + NL + "                                  }";
  protected final String TEXT_431 = NL + "\t\t                          ";
  protected final String TEXT_432 = NL + "\t\t                    }";
  protected final String TEXT_433 = NL + "\t\t\t\t\t";
  protected final String TEXT_434 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_435 = " - Retrieving the record \" + nb_line_";
  protected final String TEXT_436 = " + \".\");" + NL + "\t\t\t\t\t";
  protected final String TEXT_437 = NL + "\t\t                            ";
  protected final String TEXT_438 = ".";
  protected final String TEXT_439 = " = ";
  protected final String TEXT_440 = ".";
  protected final String TEXT_441 = ";" + NL + "\t\t                            ";
  protected final String TEXT_442 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
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
			
    stringBuffer.append(TEXT_3);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_4);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_5);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_8);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_11);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_14);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_19);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_21);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_22);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_24);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_25);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_27);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_28);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_30);
    
			}
		}
	}
	
	final DefaultLog4jFileUtil log4jFileUtil = new DefaultLog4jFileUtil((INode)(((org.talend.designer.codegen.config.CodeGeneratorArgument)argument).getArgument()));
	
    
	class DefaultLog4jCodeGenerateUtil extends DefaultLog4jFileUtil{

 		String connection = "";
 		boolean hasInit = false;
 		String dataAction ;
 		String dataOperationPrefix;
		String useBatchSize;
		String batchSize;
		String dbSchema;
 		boolean logCommitCounter = false;

		public DefaultLog4jCodeGenerateUtil(){
		}

		public DefaultLog4jCodeGenerateUtil(INode node) {
			super(node);
	    	init();
		}

	    public void beforeComponentProcess(INode node){
	    	this.node = node;
	    	init();
	    }

		private void init() {
			if(hasInit){
				return;
			}
 			this.cid = node.getUniqueName();
			this.isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
			String useConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
			if(useConn == null || "".equals(useConn) || "true".equals(useConn)){
				connection = ElementParameterParser.getValue(node,"__CONNECTION__");
				if(!"".equals(connection)){
					connection = "'" + connection+"' ";
				}
			}
			//for output
			dataAction = ElementParameterParser.getValue(node,"__DATA_ACTION__");
			if(dataAction != null && !("").equals(dataAction)){
				logCommitCounter=true;
			}
			useBatchSize = ElementParameterParser.getValue(node, "__USE_BATCH_SIZE__");
			batchSize =ElementParameterParser.getValue(node, "__BATCH_SIZE__");
			hasInit = true;
		}

		public void debugDriverClassName() {
			logInfo(node,"debug",cid+" - Driver ClassName: \"+driverClass_"+cid+"+\".");
		}

		public void debugConnectionParams(INode node) {
			beforeComponentProcess(node);
			debugDriverClassName();
		}

		public void useExistConnection(INode node){
			beforeComponentProcess(node);
			if(isLog4jEnabled) {
			
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    if (cid.startsWith("tImpala") || cid.startsWith("tHive")) {
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_36);
    } else {
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    }
    stringBuffer.append(TEXT_41);
    
			}
		}

		public void connect(INode node){
			beforeComponentProcess(node);
			connect();
		}

		public void connect(){
			connect_begin();
			
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    
			connect_end();
		}

		public void connect_begin(){
			logInfo(node,"debug",cid+" - Connection attempt to '\" + url_"+cid+" + \"' with the username '\" + dbUser_"+cid+" + \"'.");
		}

		public void connect_begin_noUser(){
			logInfo(node,"debug",cid+" - Connection attempt to '\" + url_"+cid+" + \"'.");
		}

		public void connect_end(){
			logInfo(node,"debug",cid+" - Connection to '\" + url_"+cid+" + \"' has succeeded.");
		}

		public void rollback(INode node){
			beforeComponentProcess(node);
			logInfo(node,"debug",cid+" - Connection "+connection+"starting to rollback.");
			
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    
			logInfo(node,"debug",cid+" - Connection "+connection+"rollback has succeeded.");
		}

		public void commit(INode node){
			beforeComponentProcess(node);
			commit();
		}

		private void commit(){
			commit_begin();
			
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    
			commit_end();
		}

		private void commit_begin(){
			if(logCommitCounter){
				logInfo(node,"debug",cid+" - Connection "+connection+"starting to commit \" + commitCounter_"+cid+"+ \" records.");
			}else{
				logInfo(node,"debug",cid+" - Connection "+connection+"starting to commit.");
			}
		}
		private void commit_end(){
			logInfo(node,"debug",cid+" - Connection "+connection+"commit has succeeded.");
		}

		public void close(INode node){
			beforeComponentProcess(node);
			close();
		}

		private void close(){
			close_begin();
			
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    
			close_end();
		}

		public void close_begin(){
			logInfo(node,"debug",cid+" - Closing the connection "+connection+"to the database.");
		}
		public void close_end(){
			logInfo(node,"debug",cid+" - Connection "+connection+"to the database closed.");
		}

		public void autoCommit(INode node,boolean autoCommit){
			beforeComponentProcess(node);
			logInfo(node,"debug",cid+" - Connection is set auto commit to '"+autoCommit+"'.");
			
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(autoCommit);
    stringBuffer.append(TEXT_55);
    
		}

		public void query(INode node){
			beforeComponentProcess(node);
			//for input
	 		String dbquery= ElementParameterParser.getValue(node, "__QUERY__");
			dbquery = dbquery.replaceAll("\n"," ");
			dbquery = dbquery.replaceAll("\r"," ");
			logInfo(node,"debug",cid+" - Executing the query: '\" + "+dbquery +" + \"'.");
		}

		public void retrieveRecordsCount(INode node){
			beforeComponentProcess(node);
			logInfo(node,"debug",cid+" - Retrieved records count: \"+nb_line_"+cid+" + \" .");
		}

		public void logError(INode node,String logLevel,String exception){
	    	beforeComponentProcess(node);
	    	if(isLog4jEnabled){
	    	
    stringBuffer.append(TEXT_56);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(exception);
    stringBuffer.append(TEXT_59);
    
			}
	    }

	    public void logError(INode node,String logLevel){
	    	logError(node,logLevel,"e");
	    }
	    
	    public void logInfo(INode node,String logLevel,String message){
	    	beforeComponentProcess(node);
	    	if(isLog4jEnabled){
	    	
    stringBuffer.append(TEXT_60);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(message);
    stringBuffer.append(TEXT_62);
    
			}
	    }
		/**
		*batchType :
		*			1: do not get return value of executeBatch();
		*			2: get return value of executeBatch();
		*
		*/
		public void executeBatch(INode node,int batchType){
			beforeComponentProcess(node);
			boolean logBatch = ("true").equals(useBatchSize) && !("").equals(batchSize) && !("0").equals(batchSize);
			if(logBatch){
				logInfo(node,"debug",cid+" - Executing the "+dataAction+" batch.");
			}
			if(batchType==1){
			
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_64);
    
			}else if(batchType==2){
				boolean isMysqlBatchInsert = false;
				if ((node.getUniqueName().contains("tMysqlOutput")||node.getUniqueName().contains("tAmazonMysqlOutput")) && ("INSERT").equals(dataAction)) {
					isMysqlBatchInsert = true;
				}
			
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(isMysqlBatchInsert? "1" : "countEach_"+cid );
    stringBuffer.append(TEXT_71);
    
			}
			if(logBatch){
				logInfo(node,"debug",cid+" - The "+dataAction+" batch execution has succeeded.");
			}
		}
	}

	DefaultLog4jCodeGenerateUtil log4jCodeGenerateUtil = new DefaultLog4jCodeGenerateUtil();

    
	//this util class use by connection component
	class DefaultDBInputUtil {
	
		protected String cid ;
		protected String dbproperties ;
		protected String dbhost;
		protected String dbport;
		protected String dbname;
		protected String dbuser;
		protected String dbpwd ;
		protected boolean isLog4jEnabled;
		protected boolean isDynamic;
		protected DefaultLog4jCodeGenerateUtil log4jCodeGenerateUtil;

		public void beforeComponentProcess(INode node){
			cid = node.getUniqueName();
			List<IMetadataTable> metadatas = node.getMetadataList();
			if ((metadatas != null) && (metadatas.size()>0)) {
				IMetadataTable metadata = metadatas.get(0);
				if (metadata != null) {
					isDynamic = metadata.isDynamicSchema();
				}
			}
			log4jCodeGenerateUtil = new DefaultLog4jCodeGenerateUtil();
		}

		public void afterUseExistConnection(INode node) {
		}
		
		public String getDirverClassName(INode node){
			return "";
		}
	    
	    public void afterUseNewConnection(INode node) {
	    }
	    
	    public void setURL(INode node) {
	    }
	    
		public void createConnection(INode node) {
			this.createConnection(node, true);
		}

		public void createConnection(INode node, boolean needUserAndPassword) {
			cid = node.getUniqueName();
			dbproperties = ElementParameterParser.getValue(node, "__PROPERTIES__");
			dbhost = ElementParameterParser.getValue(node, "__HOST__");
	    	dbport = ElementParameterParser.getValue(node, "__PORT__");
	    	dbname = ElementParameterParser.getValue(node, "__DBNAME__");
	    	dbuser = ElementParameterParser.getValue(node, "__USER__");

	 		boolean specify_alias = "true".equals(ElementParameterParser.getValue(node, "__SPECIFY_DATASOURCE_ALIAS__"));
 		if(specify_alias){

    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    
                String alias = ElementParameterParser.getValue(node, "__DATASOURCE_ALIAS__");

    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append((null != alias && !("".equals(alias)))?alias:"\"\"");
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    
		}

    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(this.getDirverClassName(node) );
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_88);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_89);
    
        		String passwordFieldName = "__PASS__";
        		
    stringBuffer.append(TEXT_90);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_93);
    } else {
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_96);
    }
    stringBuffer.append(TEXT_97);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    this.setURL(node);
    stringBuffer.append(TEXT_100);
    
				log4jCodeGenerateUtil.debugConnectionParams(node);
				log4jCodeGenerateUtil.connect_begin();
				
    
			if(needUserAndPassword) {

    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_103);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    
			} else {

    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_108);
    
			}
			log4jCodeGenerateUtil.connect_end();

    stringBuffer.append(TEXT_109);
    this.afterUseNewConnection(node);
    
			if(specify_alias){

    stringBuffer.append(TEXT_110);
    
			}
		}
		
		public String getQueryString(INode node) {
			String dbquery= ElementParameterParser.getValue(node, "__QUERY__");
			dbquery = org.talend.core.model.utils.NodeUtil.replaceCRLFInMEMO_SQL(dbquery);
			
			return dbquery;
		}
		
		public void createStatement(INode node) {

    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_112);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_113);
    
		}
		public void createMinValueStatement(INode node){

    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_115);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_116);
    }
		public String mappingType(String typeToGenerate) {
		
            if(("byte[]").equals(typeToGenerate)) {
                return "Bytes";
            } else if(("java.util.Date").equals(typeToGenerate)) {
                return "Timestamp";
            } else if(("Integer").equals(typeToGenerate)) {
               return "Int";
            } else {
                return typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
            }
		}
		//-----------according schema type to generate ResultSet
		public void generateStringCharAndCharacterSet(String firstConnName, IMetadataColumn column, int currentColNo,
					String trimMethod, String typeToGenerate, boolean whetherTrimAllCol, boolean whetherTrimCol) {

    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_119);
    if(isDynamic){
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_121);
    
                    if(whetherTrimAllCol || whetherTrimCol) {

    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(trimMethod);
    stringBuffer.append(TEXT_125);
    
                    }

    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    
                        if(("Character").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_137);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_140);
    
                        } else {

    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_145);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_146);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_147);
    
                        }

    stringBuffer.append(TEXT_148);
    
		}
		
	    public void generateTimestampResultSet(String firstConnName, IMetadataColumn column, int currentColNo) {

    stringBuffer.append(TEXT_149);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_151);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_152);
    if(isDynamic){
    stringBuffer.append(TEXT_153);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_154);
    
	    }
	    
	    public void generateStringResultSet(String firstConnName, IMetadataColumn column, int currentColNo, String trimMethod) {

    stringBuffer.append(TEXT_155);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_158);
    if(isDynamic){
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_160);
    if(trimMethod==null||trimMethod.isEmpty()){
    stringBuffer.append(TEXT_161);
    } else {
    stringBuffer.append(TEXT_162);
    }
    stringBuffer.append(TEXT_163);
    
	    }
	    
	    public void generateDoubleResultSet(String firstConnName, IMetadataColumn column, int currentColNo) {
	    	generateOthersResultSet(firstConnName, column, currentColNo, "Double");
	    }
	    
	    public void generateBooleanResultSet(String firstConnName, IMetadataColumn column, int currentColNo) {
	    	generateOthersResultSet(firstConnName, column, currentColNo, "Boolean");
	    }
	    
	    public void generateBytesResultSet(String firstConnName, IMetadataColumn column, int currentColNo) {
	    }
	    
	    public void generateOthersResultSet(String firstConnName, IMetadataColumn column, int currentColNo, String typeToGenerate) {

    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_165);
    if(isDynamic){
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_167);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_171);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_172);
    if(isDynamic){
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_174);
    
                if(column.isNullable()) {
                    
    stringBuffer.append(TEXT_175);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_176);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_177);
    
                } else {
                    
    stringBuffer.append(TEXT_178);
        
                }
                
    stringBuffer.append(TEXT_179);
    
	    }
	    //---------end according schema type to generate ResultSet
	    
		public void afterComponentProcess(INode node){
	    }
	}//end DefaultDBInputUtil class
	
	DefaultDBInputUtil dbInputBeginUtil = new DefaultDBInputUtil();
	
	

    stringBuffer.append(TEXT_180);
    
	// This class should be extended by the DB specific tXXXInvalid(Valid)Rows components.
	// The father class DefaultDBInputUtil exists in tXXXInput component. So the tXXXInput_Begin.javajet template
	// should be included in tXXXInvalid(Valid)Rows components as well as the current file.
	abstract class DefaultDBValidInputUtil extends DefaultDBInputUtil{

		protected INode node = null;
		
		protected String getIdentifierQuote(){
			return "";
		}
		
		public void beforeComponentProcess(INode node){
			super.beforeComponentProcess(node);
			this.node = node;
		}
		
		public String getQueryString(INode node){
			String queryString = "";
			String validationType = ElementParameterParser.getValue(node, "__VALIDATION_TYPE__");
			if("PATTERN".equals(validationType)){	
				queryString = getQueryStringForPattern();
			}else if("DQ_RULE".equals(validationType)){
				queryString = ElementParameterParser.getValue(node, "__QUERY__");
				queryString = queryString.replaceAll("\r"," ").replaceAll("\n"," ");
			}
			return queryString;
		}
		
		private String getQueryStringForPattern(){
			StringBuilder sb = new StringBuilder();
			sb.append('\"');
			sb.append(getSelectString());
			sb.append(getWhereString());
			sb.append(getDataFilter());
			sb.append('\"');
			return sb.toString();
		}
		  
		protected String getSelectString(){
		    Boolean useExistingConn = (Boolean)ElementParameterParser.getObjectValue(node, "__USE_EXISTING_CONNECTION__");
		    String schema = ElementParameterParser.getValue(node, "__SCHEMA_DB__");
            String table = ElementParameterParser.getValue(node, "__TABLE__");
		    if(useExistingConn) {
                String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
                INode connectionNode=null;
                for (INode processNode : node.getProcess().getGeneratingNodes()) { 
                    if(connection.equals(processNode.getUniqueName())) { 
                        connectionNode = processNode; 
                        break; 
                    } 
                }
                schema = ElementParameterParser.getValue(connectionNode, "__SCHEMA_DB__");
            } 
			
			String columnNames = null;
            StringBuilder sb = new StringBuilder("SELECT ");
			for(IMetadataColumn column : getAllColumnsFromSchema()) {
				String ColumnLabel = column.getOriginalDbColumnName().isEmpty()?column.getLabel():column.getOriginalDbColumnName();
				if(schema != null && !schema.trim().equals("") && !schema.trim().equals("\"\"")){
                    sb.append(handleContextModeOrAddQuotes(schema)).append(".");
                }	            
	            sb.append(handleContextModeOrAddQuotes(table)).append(".");
	            sb.append(getIdentifierQuote()).append(ColumnLabel).append(getIdentifierQuote());
				sb.append(", ");
			}
			if(sb.length() > 2){
			    sb.deleteCharAt(sb.length() - 2);
			}
			sb.append(" FROM ");
			if(schema != null && !schema.trim().equals("") && !schema.trim().equals("\"\"")){
				sb.append(handleContextModeOrAddQuotes(schema)).append(".");
			}
			sb.append(handleContextModeOrAddQuotes(table));
			return sb.toString();
		}

		protected abstract String getWhereString();
		
		protected String getAnalyzedColumn(){
            String schema = ElementParameterParser.getValue(node, "__SCHEMA_DB__");
            String table = ElementParameterParser.getValue(node, "__TABLE__");
            String analyzedColumn = ElementParameterParser.getValue(node, "__ANALYZED_COLUMN__");
            
			StringBuilder sb = new StringBuilder();
            if(schema != null && !schema.trim().equals("") && !schema.trim().equals("\"\"")){
                sb.append(handleContextModeOrAddQuotes(schema)).append(".");
            }
            sb.append(handleContextModeOrAddQuotes(table)).append(".");
            sb.append(getIdentifierQuote()).append(getOriginalDBColumnName(analyzedColumn)).append(getIdentifierQuote());
			return sb.toString();
		}
		
		protected String getPatternString(){
			boolean bCustomPattern = "true".equals(ElementParameterParser.getValue(node, "__CUSTOM_PATTERN__"));
			String dqPattern = "";
			if (bCustomPattern){
				dqPattern = ElementParameterParser.getValue(node, "__DQ_PATTERN__");
				// prepare pattern string for quoted query.				
				if(dqPattern.startsWith("\"\'")){ // if starts with a double quote and a single quote, keep only the single quotes
					dqPattern = dqPattern.substring(1);
					dqPattern = dqPattern.endsWith("\"")?dqPattern.substring(0,dqPattern.length()-1):dqPattern;
				}else{
					dqPattern = dqPattern.replace("\\", "\\\\");
				}
				dqPattern = dqPattern.replace("\"", "\\\""); // replace JAVA escape characters
			}else{
				dqPattern = ElementParameterParser.getValue(node, "__PATTERN_LIST__");
			}
			return dqPattern;
		}		

		private String getDataFilter(){
			String strWhereClause = ElementParameterParser.getValue(node, "__WHERE_CLAUSE__");
			
			StringBuilder sb = new StringBuilder();
			if (null != strWhereClause){
				//Remove first & last quote symbol.
				strWhereClause=strWhereClause.trim();
				if (strWhereClause.startsWith("\"")) {
					strWhereClause = strWhereClause.substring(1);
				}
				if (strWhereClause.endsWith("\"")) {
					strWhereClause = strWhereClause.substring(0, strWhereClause.length() - 1);
				}
				if(!strWhereClause.equals("")){
					sb.append(" AND ").append(strWhereClause);
				}
			}
			return sb.toString();
		}
		
		protected String getOriginalDBColumnName(String analyzedColumn){
			List<IMetadataTable> metadatas = node.getMetadataList();
			if ((metadatas != null) && (metadatas.size()>0)) {
				IMetadataTable metadata = metadatas.get(0);
				if (metadata != null) {
					// use original db column name
					for(IMetadataColumn column : metadata.getListColumns()) {
						String dbColumnName = null;
						if (analyzedColumn.equals(column.getLabel())){
							dbColumnName = column.getOriginalDbColumnName();
							if (null != dbColumnName && !"".equals(dbColumnName)){
									analyzedColumn = dbColumnName;
							}
							break;
						}
					}
				}
			}
			return analyzedColumn;
		}
		
		protected List<IMetadataColumn> getAllColumnsFromSchema(){
			List<IMetadataTable> metadatas = node.getMetadataList();
			if ((metadatas!=null) && (metadatas.size() > 0)) {
				IMetadataTable metadata = metadatas.get(0);
				if (metadata != null) {
					return metadata.getListColumns();
	  			}
	  		}
	  		return null;
		}
		
		protected String handleContextModeOrAddQuotes(String param) { 
			if (param.startsWith("context.")) { 
				return getIdentifierQuote() + "\" + " + param + " + \"" + getIdentifierQuote(); 
			}else{
				return param.replace("\"", getIdentifierQuote());
			}
		}
		
	}//end DefaultDBValidInputUtil class
		
		

    stringBuffer.append(TEXT_181);
    
	class ValidRowsInputBeginUtil extends DefaultDBValidInputUtil{
	
		protected String getIdentifierQuote(){
			return "`";
		}
	
		public void beforeComponentProcess(INode node){
			super.beforeComponentProcess(node);

    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    
		}
		
		public void setURL(INode node) {
		    //jdbc:jtds:sqlserver://<host>:<port>/<sid>;<property>   normal
		    //jdbc:sqlserver://<host>:<port>;DatabaseName=<sid>    2008
			String driver =  ElementParameterParser.getValue(node, "__DRIVER__"); 
			String jdbcURL = "jdbc:jtds:sqlserver";
			if("MSSQL_PROP".equals(driver)){
			    jdbcURL = "jdbc:sqlserver";
			}
			String DatabasePart = "/";
			if("MSSQL_PROP".equals(driver)){
			    DatabasePart = ";DatabaseName=";
			}
			if(dbproperties == null || ("\"\"").equals(dbproperties) || ("").equals(dbproperties)) {

    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(jdbcURL);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(DatabasePart);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_193);
    
			} else {

    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(jdbcURL);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(DatabasePart);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(dbproperties);
    stringBuffer.append(TEXT_201);
    
			}	   
		}
		
		public String getDirverClassName(INode node){
		    //net.sourceforge.jtds.jdbc.Driver normal
		    //com.microsoft.sqlserver.jdbc.SQLServerDriver 2008
		    String driver =  ElementParameterParser.getValue(node, "__DRIVER__"); 
            if("MSSQL_PROP".equals(driver)){
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver";  
            }
			return "net.sourceforge.jtds.jdbc.Driver";
		}
		
		protected String getWhereString(){
			StringBuilder sb = new StringBuilder(" WHERE (");
			sb.append(getAnalyzedColumn()).append(" NOT REGEXP ").append(getPatternString());
			sb.append(" OR ").append(getAnalyzedColumn()).append(" IS NULL)");
			return sb.toString();
		}
		
		public void createStatement(INode node) {
			super.createStatement(node);
			String enableStream = ElementParameterParser.getValue(node, "__ENABLE_STREAM__");
			if(("true").equals(enableStream)) {

    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_203);
    
			}			
		}
		
		//-----------according schema type to generate ResultSet
		public void generateTimestampResultSet(String firstConnName, IMetadataColumn column, int currentColNo) {

    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_205);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_214);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_215);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_217);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_220);
    
		}
		
		//---------end according schema type to generate ResultSet
	}//end class
	
	dbInputBeginUtil = new ValidRowsInputBeginUtil();

    stringBuffer.append(TEXT_221);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	String type = ElementParameterParser.getValue(node, "__TYPE__");
	String dbhost = ElementParameterParser.getValue(node, "__HOST__");
	String dbport = ElementParameterParser.getValue(node, "__PORT__");
	String dbname = ElementParameterParser.getValue(node, "__DBNAME__");
	String dbproperties = ElementParameterParser.getValue(node, "__PROPERTIES__");
	String dbuser = ElementParameterParser.getValue(node, "__USER__");

	String dbencoding = ElementParameterParser.getValue(node, "__ENCODING__");
	String enableStream = ElementParameterParser.getValue(node, "__ENABLE_STREAM__");
	String dbms=ElementParameterParser.getValue(node, "__MAPPING__");
	if("tJDBCInput".equals(node.getComponent().getName())) {
		boolean isEnableMapping = ("true").equals(ElementParameterParser.getValue(node, "__ENABLE_MAPPING__"));
		if (!isEnableMapping) {
			dbms = "";
		}
	}
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    boolean whetherTrimAllCol = ("true").equals(ElementParameterParser.getValue(node, "__TRIM_ALL_COLUMN__"));
    List<Map<String, String>> trimColumnList = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__TRIM_COLUMN__");

    dbInputBeginUtil.beforeComponentProcess(node);
    int dynamic_index=-1;
	List<IMetadataTable> metadatas = node.getMetadataList();
	if ((metadatas != null) && (metadatas.size()>0)) {
		IMetadataTable metadata = metadatas.get(0);
		if (metadata != null) {

    stringBuffer.append(TEXT_222);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_224);
    
		    String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
		    if(("true").equals(useExistingConn)) {
		        String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
		        String conn = "conn_" + connection;
                if(connection.matches("^.*?tAmazonAuroraConnection_\\d+$")){
                      conn += "_in";
                }

    stringBuffer.append(TEXT_225);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(conn);
    stringBuffer.append(TEXT_227);
    log4jCodeGenerateUtil.useExistConnection(node);
    
				dbInputBeginUtil.afterUseExistConnection(node);
		    } else {
				dbInputBeginUtil.createConnection(node);
				if ("teradata_id".equalsIgnoreCase(dbms)) {

    stringBuffer.append(TEXT_228);
    
      boolean queryBand = "true".equals(ElementParameterParser.getValue(node, "__QUERY_BAND__"));
      boolean activateQueryLogging = "true".equals(ElementParameterParser.getValue(node, "__ACTIVATE_QUERY_LOGGING__"));
      String usrName = ElementParameterParser.getValue(node, "__USER__");
      List<Map<String, String>> queryBandList = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__QUERY_BAND_PARAMETERS__");
      if(queryBand){
    stringBuffer.append(TEXT_229);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_230);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_232);
    
                    if(activateQueryLogging){
                        
    stringBuffer.append(TEXT_233);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_234);
    stringBuffer.append( usrName );
    stringBuffer.append(TEXT_235);
    
                    }
                  
    stringBuffer.append(TEXT_236);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_237);
    
            for(Map<String, String> map : queryBandList) {

    stringBuffer.append(TEXT_238);
    stringBuffer.append( map.get("KEY") );
    stringBuffer.append(TEXT_239);
    stringBuffer.append( map.get("VALUE") );
    stringBuffer.append(TEXT_240);
    
            }

    stringBuffer.append(TEXT_241);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_242);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_243);
    }
    
				}
		    }

    stringBuffer.append(TEXT_244);
    dbInputBeginUtil.createStatement(node);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(dbInputBeginUtil.getQueryString(node));
    stringBuffer.append(TEXT_247);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_248);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    }
    stringBuffer.append(TEXT_251);
     
                if(cid.matches("^.*?tAmazonAuroraInput_\\d+_in$")){
                    // why 3: ==> "_in".length()
                    
    stringBuffer.append(TEXT_252);
    stringBuffer.append( cid.substring(0,cid.length() - 3) );
    stringBuffer.append(TEXT_253);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_254);
    
                }else{
                    
    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_256);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_257);
    
            }
             
    stringBuffer.append(TEXT_258);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_259);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_261);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_263);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_264);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_265);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_266);
    
		    List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
		    List<IMetadataColumn> columnList = metadata.getListColumns();
		    boolean isDynamic = metadata.isDynamicSchema();
		    if(isDynamic){
		    	String DynamicDatePattern = "\"dd-MM-yyyy\"";
		    	for(IMetadataColumn column : columnList) {
		    		if("id_Dynamic".equals(column.getTalendType())) {
		    			DynamicDatePattern = column.getPattern();
		    			break;
		    		}
		    	}
		    
    stringBuffer.append(TEXT_267);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_269);
    stringBuffer.append(dbms );
    stringBuffer.append(TEXT_270);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_271);
    

		    for(int i=0; i< columnList.size(); i++) {
		        if(columnList.get(i).getTalendType().equals("id_Dynamic")){
		            dynamic_index = i+1;
		        }else{
		            IMetadataColumn column = columnList.get(i);
		            String columName = column.getOriginalDbColumnName();
		            if(columName==null || "".equals(columName)){
		                columName = column.getLabel();
		            }
	            
    stringBuffer.append(TEXT_272);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_273);
    stringBuffer.append(columName.toUpperCase());
    stringBuffer.append(TEXT_274);
    
		    	}
			}
		    
    stringBuffer.append(TEXT_275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(dynamic_index-1);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(dynamic_index);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(columnList.size()-dynamic_index);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_281);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_286);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_288);
    stringBuffer.append(dbms );
    stringBuffer.append(TEXT_289);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_293);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_294);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_295);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_296);
     if((DynamicDatePattern!=null) && (!"".equals(DynamicDatePattern)) && (!"\"\"".equals(DynamicDatePattern))) {
    stringBuffer.append(TEXT_297);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_298);
    stringBuffer.append(DynamicDatePattern);
    stringBuffer.append(TEXT_299);
     } 
    
		if (("oracle_id".equalsIgnoreCase(dbms))) {

    stringBuffer.append(TEXT_300);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_301);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_302);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_303);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_304);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_305);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_306);
    
		} else {

    stringBuffer.append(TEXT_307);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_309);
    
		}

    stringBuffer.append(TEXT_310);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_311);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_313);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_315);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_316);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_317);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_318);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_319);
    
		    }
		    
    stringBuffer.append(TEXT_320);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_321);
    if(isDynamic){
    stringBuffer.append(TEXT_322);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_323);
    }
    stringBuffer.append(TEXT_324);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_325);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_326);
    }
    stringBuffer.append(TEXT_327);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_328);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_329);
    
		        if(conns != null && conns.size() > 0) {
		            IConnection conn = conns.get(0);
		            String firstConnName = conn.getName();
		            int currentColNo = 1;
		            for(IMetadataColumn column : columnList) {
		                boolean whetherTrimCol = false;
		                if((trimColumnList != null && trimColumnList.size() > 0) && !whetherTrimAllCol) {
		                    for(Map<String, String> trimColumn : trimColumnList) {
		                        if(column.getLabel().equals(trimColumn.get("SCHEMA_COLUMN"))) {
		                            if(("true").equals(trimColumn.get("TRIM"))) {
		                                whetherTrimCol = true;
		                                break;
		                            }
		                        }
		                    }
		                }
		                String trimMethod = "";
		                if(whetherTrimAllCol || whetherTrimCol) {
		                    trimMethod = ".trim()";
		                }
		                String columnType = column.getType();

		                String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
		                String defVal = JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
		                if(conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
							if(isDynamic){
								if(!("Dynamic").equals(typeToGenerate) && dynamic_index < currentColNo) {
								
    stringBuffer.append(TEXT_330);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_331);
    stringBuffer.append(currentColNo-1);
    stringBuffer.append(TEXT_332);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_333);
    }else{
    stringBuffer.append(TEXT_334);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_336);
    }
    stringBuffer.append(TEXT_337);
    
							}
							
    stringBuffer.append(TEXT_338);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_339);
    if(isDynamic){
    stringBuffer.append(TEXT_340);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_341);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_343);
    stringBuffer.append(defVal);
    stringBuffer.append(TEXT_344);
    
								typeToGenerate = dbInputBeginUtil.mappingType(typeToGenerate);

								if(("Char").equals(typeToGenerate) || ("Character").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_345);
    dbInputBeginUtil.generateStringCharAndCharacterSet(firstConnName, column,currentColNo, trimMethod, typeToGenerate, whetherTrimAllCol, whetherTrimCol);
    
								} else if(("Timestamp").equals(typeToGenerate)) {
									if("vertica_id".equalsIgnoreCase(dbms) && "DATE".equalsIgnoreCase(column.getType())){

    stringBuffer.append(TEXT_346);
    dbInputBeginUtil.generateOthersResultSet(firstConnName, column,  currentColNo,  "Date");
    
									}else{

    stringBuffer.append(TEXT_347);
    dbInputBeginUtil.generateTimestampResultSet(firstConnName, column, currentColNo);
    
									}
		                         } else if (("List").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_348);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_349);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_350);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_351);
    if(isDynamic){
    stringBuffer.append(TEXT_352);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_353);
    
		                        } else if(("String").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_354);
    dbInputBeginUtil.generateStringResultSet(firstConnName, column, currentColNo,trimMethod);
    
								} else if(("Double").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_355);
    dbInputBeginUtil.generateDoubleResultSet(firstConnName, column, currentColNo);
    
								} else if(("Boolean").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_356);
    dbInputBeginUtil.generateBooleanResultSet(firstConnName, column, currentColNo);
    
								} else if("Geometry".equals(typeToGenerate) && type.indexOf("ORACLE") >= 0) {

    stringBuffer.append(TEXT_357);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_358);
    if(isDynamic){
    stringBuffer.append(TEXT_359);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_360);
    stringBuffer.append(TEXT_361);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_362);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_363);
    stringBuffer.append(TEXT_364);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_365);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_366);
    
                                    String sourceCRS = ElementParameterParser.getValue(node,"__CRS__");
                                    String forceCRS = ElementParameterParser.getValue(node,"__FORCE_CRS__");
                                    if (forceCRS.equals("true")) {

    stringBuffer.append(TEXT_367);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_369);
    stringBuffer.append(TEXT_370);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_371);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_372);
    stringBuffer.append(sourceCRS);
    stringBuffer.append(TEXT_373);
    
                                    }
								} else if(("Bytes").equals(typeToGenerate) && (columnType != null && (("LONG RAW").equals(columnType) || ("RAW").equals(columnType)))) {//oracle

    stringBuffer.append(TEXT_374);
    dbInputBeginUtil.generateBytesResultSet(firstConnName, column, currentColNo);
    
								} else if(("Dynamic").equals(typeToGenerate)) {

    stringBuffer.append(TEXT_375);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_376);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_377);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_378);
     //for bug TDI-20886
									boolean trim = whetherTrimAllCol || whetherTrimCol;
									if ("id_MSSQL".equalsIgnoreCase(dbms)) {
										String driver = "";
										if(("true").equals(useExistingConn)) {
											String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
											INode connectionNode = null; 
											for (INode processNode : node.getProcess().getGeneratingNodes()) { 
												if(connection.equals(processNode.getUniqueName())) { 
													connectionNode = processNode; 
													break; 
												} 
											}
											driver = ElementParameterParser.getValue(connectionNode,"__DRIVER__");
										} else {
											driver = ElementParameterParser.getValue(node,"__DRIVER__");
										}

    stringBuffer.append(TEXT_379);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_380);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_381);
    stringBuffer.append(metadata.getListColumns().size());
    stringBuffer.append(TEXT_382);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_383);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_384);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_385);
     if("JTDS".equals(driver)) { 
    stringBuffer.append(TEXT_386);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_387);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_388);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_389);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_390);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_391);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_392);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_393);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_394);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_395);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_396);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_397);
     } else {
    stringBuffer.append(TEXT_398);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_399);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_400);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_401);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_402);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_403);
     } 
    stringBuffer.append(TEXT_404);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_405);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_406);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_407);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_408);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_409);
    stringBuffer.append(trim);
    stringBuffer.append(TEXT_410);
    
									}else if("access_id".equalsIgnoreCase(dbms)){

    stringBuffer.append(TEXT_411);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_412);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_413);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_414);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_415);
    stringBuffer.append(trim);
    stringBuffer.append(TEXT_416);
    
									}else{

    stringBuffer.append(TEXT_417);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_418);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_419);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_420);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_421);
    stringBuffer.append(trim);
    stringBuffer.append(TEXT_422);
    
									}
								} else if(typeToGenerate.equals("Geometry")) {

    stringBuffer.append(TEXT_423);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_424);
    if(isDynamic){
    stringBuffer.append(TEXT_425);
    stringBuffer.append(cid);
    }else{
    stringBuffer.append(currentColNo);
    }
    stringBuffer.append(TEXT_426);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_427);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_428);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_429);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_430);
    
                 	            } else {

    stringBuffer.append(TEXT_431);
    dbInputBeginUtil.generateOthersResultSet( firstConnName, column,  currentColNo,  typeToGenerate);
    
		                        }

    stringBuffer.append(TEXT_432);
    
		                    currentColNo++;
		                }
		            }

    stringBuffer.append(TEXT_433);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_434);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_435);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_436);
    }
    
		            if(conns.size() > 1) {
		                for(int connNO = 1 ; connNO < conns.size() ; connNO++) {
		                    IConnection conn2 = conns.get(connNO);
		                    if((conn2.getName().compareTo(firstConnName) != 0) && (conn2.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA))) {
		                        for(IMetadataColumn column:columnList){
		                            
    stringBuffer.append(TEXT_437);
    stringBuffer.append(conn2.getName());
    stringBuffer.append(TEXT_438);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_439);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_440);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_441);
    
		                        }
		                    }
		                }
		            }
		        }
		}
	}

    stringBuffer.append(TEXT_442);
    return stringBuffer.toString();
  }
}
