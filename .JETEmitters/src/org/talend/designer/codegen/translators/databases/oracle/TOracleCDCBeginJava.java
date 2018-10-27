package org.talend.designer.codegen.translators.databases.oracle;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.utils.TalendTextUtils;
import org.talend.core.database.EDatabaseTypeName;
import org.talend.core.model.metadata.types.JavaType;
import java.util.Map;
import java.util.List;

public class TOracleCDCBeginJava
{
  protected static String nl;
  public static synchronized TOracleCDCBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TOracleCDCBeginJava result = new TOracleCDCBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t\tlog.debug(\"";
  protected final String TEXT_2 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_3 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_4 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_5 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_7 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_8 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_10 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_11 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_13 = " - Written records count: \" + nb_line_";
  protected final String TEXT_14 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_16 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_17 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_18 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_19 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_20 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_21 = " - Writing the record \" + nb_line_";
  protected final String TEXT_22 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_24 = " - Processing the record \" + nb_line_";
  protected final String TEXT_25 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_26 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_27 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_28 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_29 = NL + "\t\t\t\tif(conn_";
  protected final String TEXT_30 = " != null) {" + NL + "\t\t\t\t\tif(conn_";
  protected final String TEXT_31 = ".getMetaData() != null) {" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_32 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_33 = " - Uses an existing connection ";
  protected final String TEXT_34 = ".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_35 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_36 = " - Uses an existing connection with username '\" + conn_";
  protected final String TEXT_37 = ".getMetaData().getUserName() + \"'. Connection URL: \" + conn_";
  protected final String TEXT_38 = ".getMetaData().getURL() + \".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_39 = NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_40 = NL + "\t\t\tconn_";
  protected final String TEXT_41 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_42 = ", dbUser_";
  protected final String TEXT_43 = ", dbPwd_";
  protected final String TEXT_44 = ");" + NL + "\t\t\t";
  protected final String TEXT_45 = NL + "\t\t\tconn_";
  protected final String TEXT_46 = ".rollback();" + NL + "\t\t\t";
  protected final String TEXT_47 = NL + "\t\t\tconn_";
  protected final String TEXT_48 = ".commit();" + NL + "\t\t\t";
  protected final String TEXT_49 = NL + "\t\t\tconn_";
  protected final String TEXT_50 = ".close();" + NL + "\t\t\t";
  protected final String TEXT_51 = NL + "\t\t\t\tconn_";
  protected final String TEXT_52 = ".setAutoCommit(";
  protected final String TEXT_53 = ");" + NL + "\t\t\t";
  protected final String TEXT_54 = NL + "\t\t\t\tlog.";
  protected final String TEXT_55 = "(\"";
  protected final String TEXT_56 = " - \" + ";
  protected final String TEXT_57 = ".getMessage());" + NL + "\t\t\t";
  protected final String TEXT_58 = NL + "\t    \t\tlog.";
  protected final String TEXT_59 = "(\"";
  protected final String TEXT_60 = "\");" + NL + "\t\t\t";
  protected final String TEXT_61 = NL + "\t\t\t\tpstmt_";
  protected final String TEXT_62 = ".executeBatch();" + NL + "\t\t\t";
  protected final String TEXT_63 = NL + "\t\t\t\tint countSum_";
  protected final String TEXT_64 = " = 0;" + NL + "\t\t\t\tfor(int countEach_";
  protected final String TEXT_65 = ": pstmt_";
  protected final String TEXT_66 = ".executeBatch()) {" + NL + "\t\t\t\t\tcountSum_";
  protected final String TEXT_67 = " += (countEach_";
  protected final String TEXT_68 = " < 0 ? 0 : ";
  protected final String TEXT_69 = ");" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_70 = NL + "\t\t\tint nb_line_";
  protected final String TEXT_71 = " = 0;" + NL + "\t\t\tjava.sql.Connection conn_";
  protected final String TEXT_72 = " = null;" + NL + "\t\t\t";
  protected final String TEXT_73 = NL + "\t\t\t\t";
  protected final String TEXT_74 = "\t\t    ";
  protected final String TEXT_75 = NL + "            \tconn_";
  protected final String TEXT_76 = " = (java.sql.Connection)globalMap.get(\"";
  protected final String TEXT_77 = "\");" + NL + "            \t";
  protected final String TEXT_78 = NL + "\t\t\t    ";
  protected final String TEXT_79 = NL + "\t\t\t        String driverClass_";
  protected final String TEXT_80 = " = \"oracle.jdbc.OracleDriver\";" + NL + "\t\t\t    ";
  protected final String TEXT_81 = NL + "\t\t\t   \t\tthrow RuntimeException(\"XStream is new feature of Oracle 11.2\");" + NL + "\t\t\t   \t";
  protected final String TEXT_82 = NL + "\t\t\t\tjava.lang.Class.forName(driverClass_";
  protected final String TEXT_83 = ");\t" + NL + "            \t" + NL + "            \tString url_";
  protected final String TEXT_84 = " = \"jdbc:oracle:oci8:@\" + ";
  protected final String TEXT_85 = ";" + NL + "            \t" + NL + "            \tString dbUser_";
  protected final String TEXT_86 = " = ";
  protected final String TEXT_87 = ";" + NL + "            \t";
  protected final String TEXT_88 = NL + "                ";
  protected final String TEXT_89 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_90 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_91 = ");";
  protected final String TEXT_92 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_93 = " = ";
  protected final String TEXT_94 = "; ";
  protected final String TEXT_95 = NL + "              \t" + NL + "            \t" + NL + "            \tString dbPwd_";
  protected final String TEXT_96 = " = decryptedPassword_";
  protected final String TEXT_97 = ";" + NL + "            \t";
  protected final String TEXT_98 = NL + "            class LCRToXML_";
  protected final String TEXT_99 = "{" + NL + "            \tpublic org.dom4j.Document converLcrToDoc(oracle.streams.LCR lcrObject)throws java.sql.SQLException,oracle.streams.StreamsException{" + NL + "            \t\torg.dom4j.Document document = org.dom4j.DocumentHelper.createDocument();" + NL + "            \t\torg.dom4j.Element lcrElement = document.addElement(\"LCR\");" + NL + "            \t\t" + NL + "            \t\tif(lcrObject instanceof oracle.streams.RowLCR){" + NL + "            \t\t\toracle.streams.RowLCR rowLCR = (oracle.streams.RowLCR)lcrObject;" + NL + "            \t\t\t" + NL + "            \t\t\tgenPropertyElement(lcrElement,\"operation\",rowLCR.getCommandType());" + NL + "            \t\t\t" + NL + "            \t\t\tgenPropertyElement(lcrElement,\"objectName\",rowLCR.getObjectName());" + NL + "            \t\t\t" + NL + "            \t\t\tgenPropertyElement(lcrElement,\"objectOwner\",rowLCR.getObjectOwner());" + NL + "            \t\t\t" + NL + "            \t\t\tgenPropertyElement(lcrElement,\"sourceDB\",rowLCR.getSourceDatabaseName());" + NL + "            \t\t\t" + NL + "            \t\t\tjava.util.Date sourceDate=new java.util.Date(rowLCR.getSourceTime().timestampValue().getTime());" + NL + "            \t\t\tgenPropertyElement(lcrElement,\"sourceTime\",TalendDate.formatDate(\"yyyy-MM-dd'T'HH:mm:ss\",sourceDate)) ;" + NL + "            \t\t\t//new java.util.Date(lcr_";
  protected final String TEXT_100 = ".getSourceTime());" + NL + "            \t\t\t" + NL + "            \t\t\tif(rowLCR instanceof oracle.streams.DefaultRowLCR){" + NL + "            \t\t\t\tgenPropertyElement(lcrElement,\"statement\",((oracle.streams.DefaultRowLCR)rowLCR).getStatement(false));" + NL + "            \t\t\t}" + NL + "            \t\t\t" + NL + "            \t\t\t" + NL + "            \t\t\tif(\"UPDATE\".equals(rowLCR.getCommandType()) || \"DELETE\".equals(rowLCR.getCommandType())){" + NL + "            \t\t\t\toracle.streams.ColumnValue[] oldValues =rowLCR.getOldValues();" + NL + "            \t\t\t\torg.dom4j.Element oldValuesElement = lcrElement.addElement(\"oldValues\");" + NL + "            \t\t\t\tgenColumnListElement(oldValuesElement,oldValues);" + NL + "            \t\t\t}" + NL + "            \t\t\tif(\"INSERT\".equals(rowLCR.getCommandType()) || \"UPDATE\".equals(rowLCR.getCommandType())){" + NL + "            \t\t\t\toracle.streams.ColumnValue[] newValues =rowLCR.getNewValues();" + NL + "            \t\t\t\torg.dom4j.Element newValuesElement = lcrElement.addElement(\"newValues\");" + NL + "            \t\t\t\tgenColumnListElement(newValuesElement,newValues);" + NL + "            \t\t\t}" + NL + "            \t\t\treturn document;" + NL + "            \t\t}else{" + NL + "            \t\t\treturn null;" + NL + "            \t\t}" + NL + "            \t\t" + NL + "            \t}" + NL + "            \tpublic void genPropertyElement(org.dom4j.Element lcrElement,String name,String content){" + NL + "            \t\tif(content!=null){" + NL + "            \t\t\tlcrElement.addElement(name).addText(content);" + NL + "            \t\t}" + NL + "            \t}" + NL + "            \tpublic void genColumnListElement(org.dom4j.Element valuesElement,oracle.streams.ColumnValue[] columnValues)throws java.sql.SQLException{" + NL + "    \t\t\t\tfor (int i = 0; i < columnValues.length; i++) {" + NL + "    \t\t\t\t\torg.dom4j.Element columnElement = valuesElement.addElement(\"columnValue\").addAttribute(\"type\", columnValues[i].getColumnData().getClass().getSimpleName());" + NL + "    \t\t\t\t\tcolumnElement.addElement(\"columnName\").addText(columnValues[i].getColumnName());" + NL + "    \t\t\t\t\tcolumnElement.addElement(\"columnData\").addText(columnValues[i].getColumnData().stringValue());" + NL + "    \t\t\t\t}" + NL + "            \t}" + NL + "            }" + NL + "           \tLCRToXML_";
  protected final String TEXT_101 = " lcrToXML_";
  protected final String TEXT_102 = " = new LCRToXML_";
  protected final String TEXT_103 = "();";
  protected final String TEXT_104 = NL + "            " + NL + "            byte[] lcrPosition_";
  protected final String TEXT_105 = "=null;" + NL + "            oracle.streams.XStreamOut xStreamOut_";
  protected final String TEXT_106 = " = null;" + NL + "            try {" + NL + "            \t";
  protected final String TEXT_107 = NL + "             \txStreamOut_";
  protected final String TEXT_108 = " = oracle.streams.XStreamOut.attach((oracle.jdbc.OracleConnection) conn_";
  protected final String TEXT_109 = " , ";
  protected final String TEXT_110 = "," + NL + "\t\t\t\t\tnull,";
  protected final String TEXT_111 = " ,";
  protected final String TEXT_112 = " ,oracle.streams.XStreamOut.DEFAULT_MODE);" + NL + "\t\t\t\t";
  protected final String TEXT_113 = NL + "\t\t\t} catch (oracle.streams.StreamsException e_";
  protected final String TEXT_114 = ") {" + NL + "\t\t\t\tSystem.err.println(\"Cannot attach to the outbound server: \" + ";
  protected final String TEXT_115 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_116 = NL + "\t\t\t\tthrow e_";
  protected final String TEXT_117 = ";" + NL + "\t\t\t}" + NL + "\t\t\tboolean flag_";
  protected final String TEXT_118 = "=true;" + NL + "\t\t\t";
  protected final String TEXT_119 = NL + "\t\t\twhile(flag_";
  protected final String TEXT_120 = "){" + NL + "\t\t\t\toracle.streams.LCR lcr_";
  protected final String TEXT_121 = " = xStreamOut_";
  protected final String TEXT_122 = ".receiveLCR(oracle.streams.XStreamOut.DEFAULT_MODE);" + NL + "\t\t\t\tif(lcr_";
  protected final String TEXT_123 = " == null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_124 = NL + "\t\t\t\t\t\tcontinue;" + NL + "\t\t\t\t\t";
  protected final String TEXT_125 = NL + "\t\t\t\t\t\tif(xStreamOut_";
  protected final String TEXT_126 = ".getBatchStatus() == oracle.streams.XStreamOut.EXECUTING){" + NL + "\t\t\t\t\t\t\tcontinue;" + NL + "\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_127 = NL + "\t\t\t\t\t\t\tbreak;" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_128 = NL + "\t\t\t\t}else{" + NL + "\t\t\t\t\tlcrPosition_";
  protected final String TEXT_129 = " = lcr_";
  protected final String TEXT_130 = ".getPosition();" + NL + "\t\t\t\t}" + NL + "\t\t\t\tif (xStreamOut_";
  protected final String TEXT_131 = ".getBatchStatus() == oracle.streams.XStreamOut.EXECUTING){" + NL + "\t\t\t\t\tString actionType_";
  protected final String TEXT_132 = " = lcr_";
  protected final String TEXT_133 = ".getCommandType();" + NL + "\t\t\t\t\tif ((lcr_";
  protected final String TEXT_134 = " instanceof oracle.streams.DefaultRowLCR)";
  protected final String TEXT_135 = "&&";
  protected final String TEXT_136 = ".equalsIgnoreCase(lcr_";
  protected final String TEXT_137 = ".getObjectName())";
  protected final String TEXT_138 = "){" + NL + "\t\t\t\t\t\tif ((actionType_";
  protected final String TEXT_139 = ".equalsIgnoreCase(\"INSERT\") ||actionType_";
  protected final String TEXT_140 = ".equalsIgnoreCase(\"UPDATE\") || actionType_";
  protected final String TEXT_141 = ".equalsIgnoreCase(\"DELETE\"))" + NL + "\t\t\t\t\t\t\t) {" + NL + "\t\t\t\t\t\t\t\tnb_line_";
  protected final String TEXT_142 = "++;" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_143 = NL + "\t\t\t\t\t\t            \t\t\t";
  protected final String TEXT_144 = ".";
  protected final String TEXT_145 = " = lcr_";
  protected final String TEXT_146 = ";" + NL + "\t\t\t\t\t\t            \t\t";
  protected final String TEXT_147 = NL + "\t\t\t\t\t\t            \t\t\t";
  protected final String TEXT_148 = ".";
  protected final String TEXT_149 = " = ParserUtils.parseTo_Document(lcrToXML_";
  protected final String TEXT_150 = ".converLcrToDoc(lcr_";
  protected final String TEXT_151 = ").asXML());" + NL + "\t\t\t\t\t\t\t            \t";
  protected final String TEXT_152 = NL + "\t\t\t";
  protected final String TEXT_153 = NL + "\t\t\t\t";
  protected final String TEXT_154 = "\t\t    ";
  protected final String TEXT_155 = NL + "            \tconn_";
  protected final String TEXT_156 = " = (java.sql.Connection)globalMap.get(\"";
  protected final String TEXT_157 = "\");" + NL + "            \t";
  protected final String TEXT_158 = NL + "\t\t\t    ";
  protected final String TEXT_159 = NL + "\t\t\t        String driverClass_";
  protected final String TEXT_160 = " = \"oracle.jdbc.OracleDriver\";" + NL + "\t\t\t    ";
  protected final String TEXT_161 = NL + "    \t\t\t    String driverClass_";
  protected final String TEXT_162 = " = \"oracle.jdbc.driver.OracleDriver\";" + NL + "\t\t\t\t";
  protected final String TEXT_163 = NL + "\t\t\t\tjava.lang.Class.forName(driverClass_";
  protected final String TEXT_164 = ");\t" + NL + "            \t";
  protected final String TEXT_165 = NL + "            \tString url_";
  protected final String TEXT_166 = " = null;" + NL + "            \t";
  protected final String TEXT_167 = NL + "            \t\turl_";
  protected final String TEXT_168 = " = ";
  protected final String TEXT_169 = ";" + NL + "            \t\t";
  protected final String TEXT_170 = NL + "            \t\turl_";
  protected final String TEXT_171 = " = \"jdbc:oracle:thin:@\" + ";
  protected final String TEXT_172 = " + \":\" + ";
  protected final String TEXT_173 = " + \":\" + ";
  protected final String TEXT_174 = ";" + NL + "            \t\t";
  protected final String TEXT_175 = NL + "            \t\turl_";
  protected final String TEXT_176 = " = \"jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=\" + ";
  protected final String TEXT_177 = " + \")(port=\" + ";
  protected final String TEXT_178 = " + \"))(connect_data=(service_name=\" + ";
  protected final String TEXT_179 = " + \")))\";" + NL + "            \t\t";
  protected final String TEXT_180 = NL + "            \t    url_";
  protected final String TEXT_181 = " = \"jdbc:oracle:oci8:@\" + ";
  protected final String TEXT_182 = ";" + NL + "            \t    ";
  protected final String TEXT_183 = NL + "            \tString dbUser_";
  protected final String TEXT_184 = " = ";
  protected final String TEXT_185 = ";" + NL + "            \t";
  protected final String TEXT_186 = NL + "                ";
  protected final String TEXT_187 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_188 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_189 = ");";
  protected final String TEXT_190 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_191 = " = ";
  protected final String TEXT_192 = "; ";
  protected final String TEXT_193 = NL + "              \t" + NL + "            \t" + NL + "            \tString dbPwd_";
  protected final String TEXT_194 = " = decryptedPassword_";
  protected final String TEXT_195 = ";" + NL + "            \t";
  protected final String TEXT_196 = NL + "            java.sql.Statement stmt_";
  protected final String TEXT_197 = " = conn_";
  protected final String TEXT_198 = ".createStatement();" + NL + "            " + NL + "            String dbschema_";
  protected final String TEXT_199 = " = ";
  protected final String TEXT_200 = ";" + NL + "            String subscriberTable_";
  protected final String TEXT_201 = " = \"\";";
  protected final String TEXT_202 = NL + "\t\t\tif(dbschema_";
  protected final String TEXT_203 = " == null || dbschema_";
  protected final String TEXT_204 = ".trim().length() == 0) {" + NL + "    \t\t\tsubscriberTable_";
  protected final String TEXT_205 = " = ";
  protected final String TEXT_206 = " + \"TSUBSCRIBERS\" + ";
  protected final String TEXT_207 = ";" + NL + "\t\t\t} else {" + NL + "    \t\t\tsubscriberTable_";
  protected final String TEXT_208 = " = ";
  protected final String TEXT_209 = " + dbschema_";
  protected final String TEXT_210 = " + ";
  protected final String TEXT_211 = " + \".\" + ";
  protected final String TEXT_212 = " + \"TSUBSCRIBERS\" + ";
  protected final String TEXT_213 = ";" + NL + "\t\t\t}" + NL + "            " + NL + "            String cdcTableId = null;" + NL + "            java.sql.ResultSet rs_";
  protected final String TEXT_214 = " = null;" + NL + "            try {";
  protected final String TEXT_215 = NL + "            \trs_";
  protected final String TEXT_216 = " = stmt_";
  protected final String TEXT_217 = ".executeQuery(\"SELECT TALEND_CDC_TABLE_ID FROM \"+ subscriberTable_";
  protected final String TEXT_218 = " + \" WHERE TALEND_CDC_TABLE_TO_WATCH \"+";
  protected final String TEXT_219 = "\"='\"+";
  protected final String TEXT_220 = "+\".\"";
  protected final String TEXT_221 = "\"like'%.\"";
  protected final String TEXT_222 = "+";
  protected final String TEXT_223 = "+\"'\");";
  protected final String TEXT_224 = NL + "\t\t\t\tif (rs_";
  protected final String TEXT_225 = ".next()) {" + NL + "            \tcdcTableId = rs_";
  protected final String TEXT_226 = ".getString(1);" + NL + "            \t}" + NL + "\t\t\t} catch (java.sql.SQLException e) {" + NL + "    \t        // do nothing, table id will be initialized later" + NL + "\t\t\t    ";
  protected final String TEXT_227 = NL + "            }" + NL + "            " + NL + "            if (cdcTableId == null) {" + NL + "            \tcdcTableId = ";
  protected final String TEXT_228 = ";" + NL + "            }" + NL + "            ";
  protected final String TEXT_229 = NL + "                stmt_";
  protected final String TEXT_230 = ".setFetchSize(";
  protected final String TEXT_231 = ");";
  protected final String TEXT_232 = NL + "            String sql_log_";
  protected final String TEXT_233 = "=\"begin dbms_cdc_subscribe.extend_window( subscription_name=>'sub_\"+cdcTableId+\"'); end;\";";
  protected final String TEXT_234 = NL + "            stmt_";
  protected final String TEXT_235 = ".execute(sql_log_";
  protected final String TEXT_236 = ");";
  protected final String TEXT_237 = NL + "            String dataTableName_";
  protected final String TEXT_238 = " = \"\";" + NL + "            String changeTableName_";
  protected final String TEXT_239 = " = \"\";" + NL + "\t\t\tif(dbschema_";
  protected final String TEXT_240 = " == null || dbschema_";
  protected final String TEXT_241 = ".trim().length() == 0) {" + NL + "    \t\t\tdataTableName_";
  protected final String TEXT_242 = " =";
  protected final String TEXT_243 = "+ \"TCDC_\" + cdcTableId +";
  protected final String TEXT_244 = ";" + NL + "    \t\t\tchangeTableName_";
  protected final String TEXT_245 = " =";
  protected final String TEXT_246 = "+ \"TCDC_VIEW_\" + cdcTableId +";
  protected final String TEXT_247 = ";" + NL + "\t\t\t} else {" + NL + "    \t\t\tdataTableName_";
  protected final String TEXT_248 = " = ";
  protected final String TEXT_249 = "+dbschema_";
  protected final String TEXT_250 = " +";
  protected final String TEXT_251 = "+ \".\"+";
  protected final String TEXT_252 = "+ \"TCDC_\" + cdcTableId +";
  protected final String TEXT_253 = ";" + NL + "    \t\t\tchangeTableName_";
  protected final String TEXT_254 = " = ";
  protected final String TEXT_255 = "+dbschema_";
  protected final String TEXT_256 = " + ";
  protected final String TEXT_257 = "+\".\"+";
  protected final String TEXT_258 = "+ \"TCDC_VIEW_\" + cdcTableId +";
  protected final String TEXT_259 = ";" + NL + "\t\t\t}" + NL + "            String queryUpdatePrefix_";
  protected final String TEXT_260 = "= \"UPDATE \"+dataTableName_";
  protected final String TEXT_261 = "+\" SET TALEND_CDC_STATE='1'\";" + NL + "            String queryLockUpdate_";
  protected final String TEXT_262 = " =\"\";" + NL + "            String getRowsToConsume_";
  protected final String TEXT_263 = " = \"SELECT ";
  protected final String TEXT_264 = ", ";
  protected final String TEXT_265 = " OPERATION$ TALEND_CDC_TYPE,COMMIT_TIMESTAMP$ TALEND_CDC_CREATION_DATE FROM \"+dataTableName_";
  protected final String TEXT_266 = " ;";
  protected final String TEXT_267 = " FROM \"+changeTableName_";
  protected final String TEXT_268 = " +\" WHERE TALEND_CDC_SUBSCRIBERS_NAME='\"+";
  protected final String TEXT_269 = "+\"'\";";
  protected final String TEXT_270 = NL + "            ";
  protected final String TEXT_271 = NL + "            \tgetRowsToConsume_";
  protected final String TEXT_272 = " += \"";
  protected final String TEXT_273 = "\"; " + NL + "            \tgetRowsToConsume_";
  protected final String TEXT_274 = " += \" TALEND_CDC_STATE='1'\";";
  protected final String TEXT_275 = NL + "            \tgetRowsToConsume_";
  protected final String TEXT_276 = " += \"";
  protected final String TEXT_277 = "\";";
  protected final String TEXT_278 = NL + "            " + NL + "            getRowsToConsume_";
  protected final String TEXT_279 = " += \" ORDER BY TALEND_CDC_CREATION_DATE ASC\";" + NL + "            " + NL + "\t\t\tqueryLockUpdate_";
  protected final String TEXT_280 = " += \"";
  protected final String TEXT_281 = "\";" + NL;
  protected final String TEXT_282 = NL + "            String limitRequest = \"\";" + NL + "            if (\"\".compareTo(";
  protected final String TEXT_283 = ")!=0) {" + NL + "            \tlimitRequest = \"ROWNUM<=\"+";
  protected final String TEXT_284 = " +\" AND\";" + NL + "            }" + NL + "            queryLockUpdate_";
  protected final String TEXT_285 = " += limitRequest;";
  protected final String TEXT_286 = NL + "            ";
  protected final String TEXT_287 = NL + "            \tstmt_";
  protected final String TEXT_288 = ".execute(sql_log_";
  protected final String TEXT_289 = ");" + NL + "            \t";
  protected final String TEXT_290 = NL + "                stmt_";
  protected final String TEXT_291 = ".executeUpdate(queryUpdatePrefix_";
  protected final String TEXT_292 = "+queryLockUpdate_";
  protected final String TEXT_293 = "+\" TALEND_CDC_STATE IS NULL OR TALEND_CDC_STATE <>'x'\");";
  protected final String TEXT_294 = NL + "            \tstmt_";
  protected final String TEXT_295 = ".executeUpdate(queryUpdatePrefix_";
  protected final String TEXT_296 = "+queryLockUpdate_";
  protected final String TEXT_297 = "+\" TALEND_CDC_STATE <>'x' AND TALEND_CDC_SUBSCRIBERS_NAME='\"+";
  protected final String TEXT_298 = "+\"'\");";
  protected final String TEXT_299 = NL + "            rs_";
  protected final String TEXT_300 = " = stmt_";
  protected final String TEXT_301 = ".executeQuery(getRowsToConsume_";
  protected final String TEXT_302 = ");";
  protected final String TEXT_303 = NL + "            java.sql.ResultSetMetaData rsmd_";
  protected final String TEXT_304 = " = rs_";
  protected final String TEXT_305 = ".getMetaData();" + NL + "            int colQtyInRs_";
  protected final String TEXT_306 = " = rsmd_";
  protected final String TEXT_307 = ".getColumnCount();";
  protected final String TEXT_308 = NL + "            String tmpContent_";
  protected final String TEXT_309 = " = null;";
  protected final String TEXT_310 = NL + "            while (rs_";
  protected final String TEXT_311 = ".next()) {" + NL + "                nb_line_";
  protected final String TEXT_312 = "++;";
  protected final String TEXT_313 = " \t" + NL + "                 \t        if(colQtyInRs_";
  protected final String TEXT_314 = " < ";
  protected final String TEXT_315 = ") { \t\t" + NL + "                 \t            ";
  protected final String TEXT_316 = ".";
  protected final String TEXT_317 = " = ";
  protected final String TEXT_318 = "; \t\t\t" + NL + "                 \t        } else {" + NL + "                 \t            ";
  protected final String TEXT_319 = NL + "                 \t                tmpContent_";
  protected final String TEXT_320 = " = rs_";
  protected final String TEXT_321 = ".getString(";
  protected final String TEXT_322 = ");";
  protected final String TEXT_323 = NL + "                                        if(tmpContent_";
  protected final String TEXT_324 = " != null) {" + NL + "                                            tmpContent_";
  protected final String TEXT_325 = " = tmpContent_";
  protected final String TEXT_326 = ";" + NL + "                                        }";
  protected final String TEXT_327 = "                 \t                " + NL + "                 \t                if(tmpContent_";
  protected final String TEXT_328 = " != null && tmpContent_";
  protected final String TEXT_329 = ".length() > 0) {\t\t\t  \t" + NL + "                 \t                    ";
  protected final String TEXT_330 = ".";
  protected final String TEXT_331 = " = tmpContent_";
  protected final String TEXT_332 = ".charAt(0);\t\t\t  \t\t" + NL + "                 \t                } else {\t\t\t  \t" + NL + "                 \t                    ";
  protected final String TEXT_333 = "\t\t\t  \t    " + NL + "                 \t                        if(tmpContent_";
  protected final String TEXT_334 = " == null) {\t\t\t  \t   \t" + NL + "                 \t                            ";
  protected final String TEXT_335 = ".";
  protected final String TEXT_336 = " = null;\t\t\t  \t\t\t" + NL + "                 \t                        } else {\t\t\t  \t\t" + NL + "                 \t                            ";
  protected final String TEXT_337 = ".";
  protected final String TEXT_338 = " = '\\0';\t\t\t  \t\t\t" + NL + "                 \t                        }" + NL + "                 \t                        ";
  protected final String TEXT_339 = "\t\t\t  \t\t" + NL + "                 \t                        if(\"\".equals(tmpContent_";
  protected final String TEXT_340 = ")) {\t\t\t  \t\t" + NL + "                 \t                            ";
  protected final String TEXT_341 = ".";
  protected final String TEXT_342 = " = '\\0';\t\t\t  \t\t\t" + NL + "                 \t                        } else {\t\t\t  \t\t" + NL + "                        \t\t\t  \t\t\tthrow new RuntimeException(" + NL + "                        \t\t\t\t\t\t\t\"Value is empty for column : '";
  protected final String TEXT_343 = "', value is invalid or this column should be nullable or have a default value.\");\t\t\t\t\t\t\t" + NL + "                 \t                        }" + NL + "                 \t                        ";
  protected final String TEXT_344 = NL + "                 \t                }\t\t\t" + NL + "                 \t                ";
  protected final String TEXT_345 = NL + "                 \t                if(rs_";
  protected final String TEXT_346 = ".getTimestamp(";
  protected final String TEXT_347 = ") != null) {" + NL + "                 \t                    ";
  protected final String TEXT_348 = ".";
  protected final String TEXT_349 = " = new java.util.Date(rs_";
  protected final String TEXT_350 = ".getTimestamp(";
  protected final String TEXT_351 = ").getTime());" + NL + "                 \t                } else {" + NL + "                 \t                    ";
  protected final String TEXT_352 = ".";
  protected final String TEXT_353 = " = null;" + NL + "                 \t                }" + NL + "                 \t                ";
  protected final String TEXT_354 = NL + "                 \t                ";
  protected final String TEXT_355 = ".";
  protected final String TEXT_356 = " = (List)rs_";
  protected final String TEXT_357 = ".getObject(";
  protected final String TEXT_358 = ");" + NL + "                 \t                ";
  protected final String TEXT_359 = NL + "                                    tmpContent_";
  protected final String TEXT_360 = " = rs_";
  protected final String TEXT_361 = ".getString(";
  protected final String TEXT_362 = ");" + NL + "                                    if(tmpContent_";
  protected final String TEXT_363 = " != null) {";
  protected final String TEXT_364 = NL + "                                        ";
  protected final String TEXT_365 = ".";
  protected final String TEXT_366 = " = tmpContent_";
  protected final String TEXT_367 = ";" + NL + "                                    } else {";
  protected final String TEXT_368 = NL + "                                        ";
  protected final String TEXT_369 = ".";
  protected final String TEXT_370 = " = null;" + NL + "                                    }                 \t                " + NL + "                 \t                ";
  protected final String TEXT_371 = NL + "                 \t                if(rs_";
  protected final String TEXT_372 = ".getObject(";
  protected final String TEXT_373 = ") != null) {" + NL + "                 \t                    ";
  protected final String TEXT_374 = ".";
  protected final String TEXT_375 = " = rs_";
  protected final String TEXT_376 = ".get";
  protected final String TEXT_377 = "(";
  protected final String TEXT_378 = ");" + NL + "                 \t                } else {" + NL + "                 \t                    ";
  protected final String TEXT_379 = NL + "                 \t                        ";
  protected final String TEXT_380 = ".";
  protected final String TEXT_381 = " = null;" + NL + "                 \t                        ";
  protected final String TEXT_382 = "    " + NL + "                 \t                        throw new RuntimeException(\"Null value in non-Nullable column\");" + NL + "                 \t                        ";
  protected final String TEXT_383 = NL + "                 \t                }" + NL + "                 \t                ";
  protected final String TEXT_384 = NL + "                 \t        } \t\t" + NL + "                 \t        ";
  protected final String TEXT_385 = NL + "                 \t                ";
  protected final String TEXT_386 = ".";
  protected final String TEXT_387 = "=";
  protected final String TEXT_388 = ".";
  protected final String TEXT_389 = ";" + NL + "                 \t                ";
  protected final String TEXT_390 = NL + "\t\t\t";
  protected final String TEXT_391 = NL + "\t\t" + NL + "\t\t";
  protected final String TEXT_392 = NL;
  protected final String TEXT_393 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
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
			
    stringBuffer.append(TEXT_1);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_2);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_3);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_6);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_9);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_12);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_17);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_19);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_20);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_23);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_26);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    
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
			
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    if (cid.startsWith("tImpala") || cid.startsWith("tHive")) {
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_34);
    } else {
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    }
    stringBuffer.append(TEXT_39);
    
			}
		}

		public void connect(INode node){
			beforeComponentProcess(node);
			connect();
		}

		public void connect(){
			connect_begin();
			
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    
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
			
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    
			logInfo(node,"debug",cid+" - Connection "+connection+"rollback has succeeded.");
		}

		public void commit(INode node){
			beforeComponentProcess(node);
			commit();
		}

		private void commit(){
			commit_begin();
			
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    
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
			
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    
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
			
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(autoCommit);
    stringBuffer.append(TEXT_53);
    
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
	    	
    stringBuffer.append(TEXT_54);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(exception);
    stringBuffer.append(TEXT_57);
    
			}
	    }

	    public void logError(INode node,String logLevel){
	    	logError(node,logLevel,"e");
	    }
	    
	    public void logInfo(INode node,String logLevel,String message){
	    	beforeComponentProcess(node);
	    	if(isLog4jEnabled){
	    	
    stringBuffer.append(TEXT_58);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(message);
    stringBuffer.append(TEXT_60);
    
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
			
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_62);
    
			}else if(batchType==2){
				boolean isMysqlBatchInsert = false;
				if ((node.getUniqueName().contains("tMysqlOutput")||node.getUniqueName().contains("tAmazonMysqlOutput")) && ("INSERT").equals(dataAction)) {
					isMysqlBatchInsert = true;
				}
			
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    stringBuffer.append(isMysqlBatchInsert? "1" : "countEach_"+cid );
    stringBuffer.append(TEXT_69);
    
			}
			if(logBatch){
				logInfo(node,"debug",cid+" - The "+dataAction+" batch execution has succeeded.");
			}
		}
	}

	DefaultLog4jCodeGenerateUtil log4jCodeGenerateUtil = new DefaultLog4jCodeGenerateUtil();

    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();	
	String dbhost = ElementParameterParser.getValue(node, "__HOST__");
	String dbport = ElementParameterParser.getValue(node, "__PORT__");
	String dbname= ElementParameterParser.getValue(node, "__DBNAME__");
	String localServiceName = ElementParameterParser.getValue(node, "__LOCAL_SERVICE_NAME__");
	String dbuser= ElementParameterParser.getValue(node, "__USER__");
	String useCursor= ElementParameterParser.getValue(node, "__USE_CURSOR__");
	String cursorSize= ElementParameterParser.getValue(node, "__CURSOR_SIZE__");
	String suscriber = ElementParameterParser.getValue(node, "__SUSCRIBER__");
	String dataTable = ElementParameterParser.getValue(node, "__DATATABLE__");
	String tableSchema = ElementParameterParser.getValue(node,"__SCHEMA_DB__");
	String limit = ElementParameterParser.getValue(node, "__LIMIT__");
	boolean eventsTypeInsert = "true".compareTo(ElementParameterParser.getValue(node,"__EVENTS_TYPE_INSERT__"))==0;
	boolean eventsTypeUpdate = "true".compareTo(ElementParameterParser.getValue(node,"__EVENTS_TYPE_UPDATE__"))==0;
	boolean eventsTypeDelete = "true".compareTo(ElementParameterParser.getValue(node,"__EVENTS_TYPE_DELETE__"))==0;
	boolean whetherTrimAllCol = "true".equals(ElementParameterParser.getValue(node, "__TRIM_ALL_COLUMN__"));
	String cdcMode= ElementParameterParser.getValue(node, "__CDC_MODE__");
    List<Map<String, String>> trimColumnList = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__TRIM_COLUMN__");
    String dbVersion =  ElementParameterParser.getValue(node, "__DB_VERSION__");
    boolean setDataSchema = "true".equals(ElementParameterParser.getValue(node, "__SET_DATA_TABLE_SCHEMA__"));
	String dataSchema = ElementParameterParser.getValue(node, "__DATA_TABLE_SCHEMA__");
	    
	List<IMetadataTable> metadatas = node.getMetadataList();
	if ((metadatas != null) && (metadatas.size() > 0)) {
		IMetadataTable metadata = metadatas.get(0);
		if (metadata != null) {
		
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    
			if("XSTREAM".equals(cdcMode)){
			
    stringBuffer.append(TEXT_73);
    stringBuffer.append(TEXT_74);
    
		    String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
		    boolean isKeepListen = "true".equalsIgnoreCase(ElementParameterParser.getValue(node,"__KEEP_LISTENING__"));
		    String ackInterval = ElementParameterParser.getValue(node,"__ACK_INTERVAL__");
		    String idleTimeout = ElementParameterParser.getValue(node,"__IDLE_TIMEOUT__");
		    String outBoundServer = ElementParameterParser.getValue(node,"__OUTBOUND_SERVER__");
		    String outputLCRType = ElementParameterParser.getValue(node,"__GENERATE_LCRTYPE__");
		    String cdcTable = ElementParameterParser.getValue(node,"__DATATABLE__");
		    boolean isReadAll = true;
		    if(cdcTable!=null && cdcTable.length()>0 && !"\"\"".equals(cdcTable)){
		    	isReadAll = false;
		    }
            if("true".equalsIgnoreCase(useExistingConn)) {
            	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
            	String conn = "conn_" + connection;
            	
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(conn);
    stringBuffer.append(TEXT_77);
    
            	log4jCodeGenerateUtil.useExistConnection(node);
            } else {
            	
    stringBuffer.append(TEXT_78);
    if("ORACLE_11".equals(dbVersion) || "ORACLE_11-6".equals(dbVersion) || "ORACLE_12".equals(dbVersion) ){
    stringBuffer.append(TEXT_79);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_80);
    
			    }else{
			   	
    stringBuffer.append(TEXT_81);
    
			    }
			    
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_84);
    stringBuffer.append(localServiceName);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_87);
    
                String passwordFieldName = "__PASS__";
                
    stringBuffer.append(TEXT_88);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_91);
    } else {
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_94);
    }
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    
            	log4jCodeGenerateUtil.debugConnectionParams(node);
            	log4jCodeGenerateUtil.connect(node);
            }
            if("LCR_DOC".equals(outputLCRType) ){
            
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
            
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    
            	log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Try to attach to outbound server:\"+"+ outBoundServer+"+\"");
            	
    stringBuffer.append(TEXT_107);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(outBoundServer);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(ackInterval);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(idleTimeout);
    stringBuffer.append(TEXT_112);
    
				log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Attached successfully.");
				
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(outBoundServer);
    stringBuffer.append(TEXT_115);
    
				log4jCodeGenerateUtil.logInfo(node,"error",cid+" - Cannot attach to the outbound server:  \"+"+ outBoundServer+"+\"");
				
    stringBuffer.append(TEXT_116);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    
			log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Start to recieve LCR Object from the outbound server:  \"+"+ outBoundServer+"+\"");
			
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    if(isKeepListen){
    stringBuffer.append(TEXT_124);
    }else{
    stringBuffer.append(TEXT_125);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    
							log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Exit loop of receive LCR from the outbound server: \"+"+ outBoundServer+"+\"");
							
    stringBuffer.append(TEXT_127);
    }
    stringBuffer.append(TEXT_128);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_134);
    if(!isReadAll){
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cdcTable);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    }
    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    
							List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
							if(conns != null && conns.size() > 0) {
		                 		IConnection conn = conns.get(0);
			                 	if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			                 		String firstConnName = conn.getName();
			                 		List<IMetadataColumn> columnList = metadata.getListColumns();
			                 		for (int i=0;i< columnList.size();i++) {
										IMetadataColumn column= columnList.get(i);
						            	if(column.isCustom()){
							            	if("LCR_OBJECT".equals(outputLCRType) && "id_Object".equals(column.getTalendType()) && "LCR_OBJECT".equals(column.getLabel())){
						            		
    stringBuffer.append(TEXT_143);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    
						            			break;
						            		}
											if("LCR_DOC".equals(outputLCRType) && "id_Document".equals(column.getTalendType()) && "LCR_DOC".equals(column.getLabel())){
						            		
    stringBuffer.append(TEXT_147);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    
							            		break;
						            		}
						            	}
						            }
						    	}
		                 	}
							
    stringBuffer.append(TEXT_152);
    
			}else{
			
    stringBuffer.append(TEXT_153);
    stringBuffer.append(TEXT_154);
    
		    String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
            if("true".equalsIgnoreCase(useExistingConn)) {
            	String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
            	String conn = "conn_" + connection;
            	
    stringBuffer.append(TEXT_155);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(conn);
    stringBuffer.append(TEXT_157);
    
            	log4jCodeGenerateUtil.useExistConnection(node);
            } else {   
            	
    stringBuffer.append(TEXT_158);
    if("ORACLE_11".equals(dbVersion) || "ORACLE_11-6".equals(dbVersion) || "ORACLE_12".equals(dbVersion) ){
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_160);
    }else {
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    }
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    
            	String connectionType = ElementParameterParser.getValue(node, "__CONNECTION_TYPE__");
            	
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    
            	if("ORACLE_RAC".equals(connectionType)) {
            		
            	    
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_168);
    stringBuffer.append(ElementParameterParser.getValue(node, "__RAC_URL__"));
    stringBuffer.append(TEXT_169);
    
            	} else if("ORACLE_SID".equals(connectionType)) {
            	    
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_171);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_174);
    
            	} else if("ORACLE_SERVICE_NAME".equals(connectionType)) {
            	    
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_176);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_179);
    
            	} else if("ORACLE_OCI".equals(connectionType)) {
            	    
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_181);
    stringBuffer.append(localServiceName);
    stringBuffer.append(TEXT_182);
    
            	}
            	
    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_185);
    
                String passwordFieldName = "__PASS__";
                
    stringBuffer.append(TEXT_186);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_189);
    } else {
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_192);
    }
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    
            	log4jCodeGenerateUtil.debugConnectionParams(node);
            	log4jCodeGenerateUtil.connect(node);
            }
            
    stringBuffer.append(TEXT_196);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_197);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(tableSchema);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_201);
    String quote=TalendTextUtils.getQuoteByDBType(EDatabaseTypeName.ORACLEFORSID.getXmlName(),false);
              String oneQuote=TalendTextUtils.addQuotes(quote);
            
    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_214);
    if(setDataSchema) {
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:"+"SELECT TALEND_CDC_TABLE_ID FROM \"+ subscriberTable_"+cid+" + \" WHERE TALEND_CDC_TABLE_TO_WATCH ='\"+"+dataSchema+"+\".\"+"+dataTable+"+\"'");
    } else {
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:"+"SELECT TALEND_CDC_TABLE_ID FROM \"+ subscriberTable_"+cid+" + \" WHERE TALEND_CDC_TABLE_TO_WATCH "+ "like'%.\" +"+dataTable+"+\"'");
    }
    stringBuffer.append(TEXT_215);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_216);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_217);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_218);
    if(setDataSchema) {
    stringBuffer.append(TEXT_219);
    stringBuffer.append(dataSchema);
    stringBuffer.append(TEXT_220);
    } else {
    stringBuffer.append(TEXT_221);
    }
    stringBuffer.append(TEXT_222);
    stringBuffer.append(dataTable);
    stringBuffer.append(TEXT_223);
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
    stringBuffer.append(TEXT_224);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_225);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_226);
    log4jCodeGenerateUtil.logError(node,"warn");
    stringBuffer.append(TEXT_227);
    stringBuffer.append(dataTable);
    stringBuffer.append(TEXT_228);
    
            if("true".equals(useCursor)) {
                
    stringBuffer.append(TEXT_229);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_230);
    stringBuffer.append(cursorSize );
    stringBuffer.append(TEXT_231);
    
            }
            
    
            	if("LOG".equals(cdcMode)){
            
    stringBuffer.append(TEXT_232);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_233);
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:\"+sql_log_"+cid+"+\"");
    stringBuffer.append(TEXT_234);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_235);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_236);
    
                    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
            	}
            
    stringBuffer.append(TEXT_237);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_238);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_243);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_244);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_251);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_257);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_258);
    stringBuffer.append(oneQuote);
    stringBuffer.append(TEXT_259);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_263);
    
            for (IMetadataColumn column: metadata.getListColumns()) {
            	if(column.isCustom()&&"LOG".equals(cdcMode)){
            		continue;
            	}
            		
    stringBuffer.append(TalendTextUtils.addSQLFieldQuotes(column.getOriginalDbColumnName(),quote));
    
            		if (metadata.getListColumns().indexOf(column) != (metadata.getListColumns().size() -1)) {
            		
    stringBuffer.append(TEXT_264);
    
            		}
            }
            if("LOG".equals(cdcMode)){
            
    stringBuffer.append(TEXT_265);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_266);
    
            }else{
            
    stringBuffer.append(TEXT_267);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(suscriber);
    stringBuffer.append(TEXT_269);
    
            }
            	String filterOnEvent = "";
            	String filterOnEvent_QueryLockUpdate = " WHERE ";
            	if (eventsTypeInsert&&eventsTypeUpdate&&eventsTypeDelete) {
            		// do nothing
            	} else if (!eventsTypeInsert&&!eventsTypeUpdate&&!eventsTypeDelete) {
            		// do nothing
            	} else {
            		if("LOG".equals(cdcMode)){
            			filterOnEvent += " WHERE (OPERATION$";
            			filterOnEvent_QueryLockUpdate += "(OPERATION$ IN (";
            		} else {
            			filterOnEvent += " AND (TALEND_CDC_TYPE";
            			filterOnEvent_QueryLockUpdate += "(TALEND_CDC_TYPE IN (";
            		}
            		filterOnEvent += " IN (";
            		
            		boolean prec = false; 
            		if (eventsTypeInsert) {
            			filterOnEvent += "'I'";
            			filterOnEvent_QueryLockUpdate += "'I'";
            			prec=true;
            		}
            		if (eventsTypeUpdate) {
            			if (prec) {
            				filterOnEvent += ",";
            				filterOnEvent_QueryLockUpdate += ",";
            			}
            			filterOnEvent += "'UN','UU','U'";
            			filterOnEvent_QueryLockUpdate += "'UN','UU','U'";
            			prec=true;
            		}
            		if (eventsTypeDelete) {
            			if (prec) {
            				filterOnEvent += ",";
            				filterOnEvent_QueryLockUpdate +=",";
            			}
            			filterOnEvent += "'D'";
            			filterOnEvent_QueryLockUpdate +="'D'";
            		}
            		filterOnEvent += "))";
            		filterOnEvent_QueryLockUpdate +=")) AND ";
            	}
            
    stringBuffer.append(TEXT_270);
    if("LOG".equals(cdcMode)){
    stringBuffer.append(TEXT_271);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(filterOnEvent_QueryLockUpdate );
    stringBuffer.append(TEXT_273);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_274);
    }else{
    stringBuffer.append(TEXT_275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(filterOnEvent );
    stringBuffer.append(TEXT_277);
    }
    stringBuffer.append(TEXT_278);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(filterOnEvent_QueryLockUpdate );
    stringBuffer.append(TEXT_281);
     if (!"LOG".equals(cdcMode) && (limit!=null)&&(limit.compareTo("")!=0)) { 
    stringBuffer.append(TEXT_282);
    stringBuffer.append(limit);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(limit);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_285);
     } 
    stringBuffer.append(TEXT_286);
     if("LOG".equals(cdcMode)){ 
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:\"+sql_log_"+cid+"+\"");
    stringBuffer.append(TEXT_287);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_288);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_289);
    
            	log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
                log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:\"+queryUpdatePrefix_"+cid+"+queryLockUpdate_"+cid+"+\" TALEND_CDC_STATE IS NULL OR TALEND_CDC_STATE <>'x'.");
                
    stringBuffer.append(TEXT_290);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_291);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_293);
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
     }else {
                log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:\"+queryUpdatePrefix_"+cid+"+queryLockUpdate_"+cid+"+\" TALEND_CDC_STATE <>'x' AND TALEND_CDC_SUBSCRIBERS_NAME='\"+"+suscriber+"+\"'.");
            
    stringBuffer.append(TEXT_294);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_295);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_296);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_297);
    stringBuffer.append(suscriber);
    stringBuffer.append(TEXT_298);
    
            log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
            }
            log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executing SQL:\"+getRowsToConsume_"+cid+"+\"");
            
    stringBuffer.append(TEXT_299);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_300);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_301);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_302);
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Executed successfully.");
    stringBuffer.append(TEXT_303);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_304);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_305);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_306);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_307);
    
            List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
            List<IMetadataColumn> columnList = metadata.getListColumns();
            
    stringBuffer.append(TEXT_308);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_309);
    log4jCodeGenerateUtil.logInfo(node,"info",cid+" - Retrieving records from the database.");
    stringBuffer.append(TEXT_310);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_311);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_312);
    
                if(conns != null && conns.size() > 0) {
                 	IConnection conn = conns.get(0);
                 	String firstConnName = conn.getName();
                 	int currentColNo = 1;
                 	for(IMetadataColumn column : columnList) { 	
                        boolean whetherTrimCol = false;
                        if((trimColumnList != null && trimColumnList.size() > 0) && !whetherTrimAllCol) {
                            for(Map<String, String> trimColumn : trimColumnList) {
                                if(column.getLabel().equals(trimColumn.get("SCHEMA_COLUMN"))) {
                                    if("true".equals(trimColumn.get("TRIM"))) {
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
                 	    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                 	    String defVal = JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate); 	
                 	    if(conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                 	        
    stringBuffer.append(TEXT_313);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_315);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_316);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_317);
    stringBuffer.append(defVal);
    stringBuffer.append(TEXT_318);
    
                 	            if("byte[]".equals(typeToGenerate)) {
                 	                typeToGenerate = "Bytes";
                 	            } else if("java.util.Date".equals(typeToGenerate)) {
                 	                typeToGenerate = "Timestamp";
                 	            } else if("Integer".equals(typeToGenerate)) {
                 	                typeToGenerate = "Int";
                 	            } else {
                 	                typeToGenerate=typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
                 	            }		  
                 	            if("Char".equals(typeToGenerate) || "Character".equals(typeToGenerate)) {
                 	                
    stringBuffer.append(TEXT_319);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_320);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_321);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_322);
    
                                    if(whetherTrimAllCol || whetherTrimCol) {
                                        
    stringBuffer.append(TEXT_323);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_324);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(cid);
    stringBuffer.append(trimMethod);
    stringBuffer.append(TEXT_326);
    
                                    }
                                    
    stringBuffer.append(TEXT_327);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_328);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_329);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_330);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_331);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_332);
    
                 	                    if("Character".equals(typeToGenerate)) {
                 	                        
    stringBuffer.append(TEXT_333);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_336);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_337);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_338);
    
                 	                    } else {
                 	                        
    stringBuffer.append(TEXT_339);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_340);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_341);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_342);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_343);
    
                 	                    }
                 	                    
    stringBuffer.append(TEXT_344);
     	
                 	            } else if("Timestamp".equals(typeToGenerate)) {
                 	                
    stringBuffer.append(TEXT_345);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_346);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_347);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_348);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_349);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_350);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_351);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_352);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_353);
    
                 	            } else if ("List".equals(typeToGenerate)) {
                 	                
    stringBuffer.append(TEXT_354);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_355);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_356);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_357);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_358);
     	
                 	            } else if("String".equals(typeToGenerate)) {
                 	                
    stringBuffer.append(TEXT_359);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_361);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_362);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_363);
    stringBuffer.append(TEXT_364);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_365);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_366);
    stringBuffer.append(cid);
    stringBuffer.append(trimMethod);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_369);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_370);
    
                 	            } else {
                 	                
    stringBuffer.append(TEXT_371);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_372);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_373);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_374);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_375);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_376);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_377);
    stringBuffer.append(currentColNo);
    stringBuffer.append(TEXT_378);
    
                 	                    if(column.isNullable()) {
                 	                        
    stringBuffer.append(TEXT_379);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_380);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_381);
    
                 	                    } else {
                 	                        
    stringBuffer.append(TEXT_382);
        
                 	                    }
                 	                    
    stringBuffer.append(TEXT_383);
    
                 	            }
                 	            
    stringBuffer.append(TEXT_384);
      
                 	        currentColNo++;
                 	    }
                 	}
                 	log4jCodeGenerateUtil.logInfo(node,"debug",cid+" - Retrieving the record \"+nb_line_"+cid+"+\".");
                    if(conns.size() > 1) {
                 	    for(int connNO = 1 ; connNO < conns.size() ; connNO++) {
                 	        IConnection conn2 = conns.get(connNO);
                 	        if((conn2.getName().compareTo(firstConnName) != 0) && (conn2.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA))) {
                 	            for(IMetadataColumn column : columnList) {
                 	                
    stringBuffer.append(TEXT_385);
    stringBuffer.append(conn2.getName());
    stringBuffer.append(TEXT_386);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_387);
    stringBuffer.append(firstConnName);
    stringBuffer.append(TEXT_388);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_389);
     
                 	            }
                 	        }
                 	    }
                 	}
                }
                
    stringBuffer.append(TEXT_390);
    
			}
			
    stringBuffer.append(TEXT_391);
    
		}
	}
	
    stringBuffer.append(TEXT_392);
    stringBuffer.append(TEXT_393);
    return stringBuffer.toString();
  }
}
