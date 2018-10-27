package org.talend.designer.codegen.translators.bigdata.impala;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import java.util.List;
import java.util.Map;

public class TImpalaCreateTableBeginJava
{
  protected static String nl;
  public static synchronized TImpalaCreateTableBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TImpalaCreateTableBeginJava result = new TImpalaCreateTableBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\tjava.sql.Connection conn_";
  protected final String TEXT_2 = " = (java.sql.Connection)globalMap.get(\"conn_";
  protected final String TEXT_3 = "\");" + NL + "\t\tString dbname_";
  protected final String TEXT_4 = " = (String)globalMap.get(\"";
  protected final String TEXT_5 = "\");";
  protected final String TEXT_6 = NL + "\t\t";
  protected final String TEXT_7 = NL + "\t";
  protected final String TEXT_8 = NL + "\t\t\tString driverClass_";
  protected final String TEXT_9 = " = \"";
  protected final String TEXT_10 = "\";" + NL + "\t\t\tjava.lang.Class.forName(driverClass_";
  protected final String TEXT_11 = ");";
  protected final String TEXT_12 = NL + "\t\t\tSharedDBConnectionLog4j.initLogger(log,\"";
  protected final String TEXT_13 = "\");";
  protected final String TEXT_14 = NL + "\t\t\tString sharedConnectionName_";
  protected final String TEXT_15 = " = ";
  protected final String TEXT_16 = ";" + NL + "\t\t\tconn_";
  protected final String TEXT_17 = " = ";
  protected final String TEXT_18 = ".getDBConnection(\"";
  protected final String TEXT_19 = "\",url_";
  protected final String TEXT_20 = ",dbUser_";
  protected final String TEXT_21 = " , dbPwd_";
  protected final String TEXT_22 = " , sharedConnectionName_";
  protected final String TEXT_23 = ");";
  protected final String TEXT_24 = NL + "\t\tconn_";
  protected final String TEXT_25 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_26 = ",dbUser_";
  protected final String TEXT_27 = ",dbPwd_";
  protected final String TEXT_28 = ");";
  protected final String TEXT_29 = NL + "\t\t\tlog.debug(\"";
  protected final String TEXT_30 = " - Connection is set auto commit to '";
  protected final String TEXT_31 = "'.\");";
  protected final String TEXT_32 = NL + "\t\t\tconn_";
  protected final String TEXT_33 = ".setAutoCommit(";
  protected final String TEXT_34 = ");";
  protected final String TEXT_35 = NL + "\t\t\tString url_";
  protected final String TEXT_36 = " = \"jdbc:hive2://\"+";
  protected final String TEXT_37 = "+\":\"+";
  protected final String TEXT_38 = "+\"/\"+";
  protected final String TEXT_39 = " + ";
  protected final String TEXT_40 = "; ";
  protected final String TEXT_41 = NL + NL + "\t";
  protected final String TEXT_42 = NL + NL + "\tString dbUser_";
  protected final String TEXT_43 = " = ";
  protected final String TEXT_44 = ";";
  protected final String TEXT_45 = NL + "\t" + NL + "\t";
  protected final String TEXT_46 = NL + "\t\tString dbPwd_";
  protected final String TEXT_47 = " = null;" + NL + "\t";
  protected final String TEXT_48 = NL + "\t\t";
  protected final String TEXT_49 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_50 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_51 = ");";
  protected final String TEXT_52 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_53 = " = ";
  protected final String TEXT_54 = "; ";
  protected final String TEXT_55 = NL + "\t\tString dbPwd_";
  protected final String TEXT_56 = " = decryptedPassword_";
  protected final String TEXT_57 = ";" + NL + "\t";
  protected final String TEXT_58 = NL + NL + "\tjava.sql.Connection conn_";
  protected final String TEXT_59 = " = null;" + NL + "\t";
  protected final String TEXT_60 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_61 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_62 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_63 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_64 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_65 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_66 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_67 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_68 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_69 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_70 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_71 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_72 = " - Written records count: \" + nb_line_";
  protected final String TEXT_73 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_74 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_75 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_76 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_77 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_78 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_79 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_80 = " - Writing the record \" + nb_line_";
  protected final String TEXT_81 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_82 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_83 = " - Processing the record \" + nb_line_";
  protected final String TEXT_84 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_85 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_86 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_87 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_88 = NL + "\t\t\t\tif(conn_";
  protected final String TEXT_89 = " != null) {" + NL + "\t\t\t\t\tif(conn_";
  protected final String TEXT_90 = ".getMetaData() != null) {" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_91 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_92 = " - Uses an existing connection ";
  protected final String TEXT_93 = ".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_94 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_95 = " - Uses an existing connection with username '\" + conn_";
  protected final String TEXT_96 = ".getMetaData().getUserName() + \"'. Connection URL: \" + conn_";
  protected final String TEXT_97 = ".getMetaData().getURL() + \".\");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_98 = NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_99 = NL + "\t\t\tconn_";
  protected final String TEXT_100 = " = java.sql.DriverManager.getConnection(url_";
  protected final String TEXT_101 = ", dbUser_";
  protected final String TEXT_102 = ", dbPwd_";
  protected final String TEXT_103 = ");" + NL + "\t\t\t";
  protected final String TEXT_104 = NL + "\t\t\tconn_";
  protected final String TEXT_105 = ".rollback();" + NL + "\t\t\t";
  protected final String TEXT_106 = NL + "\t\t\tconn_";
  protected final String TEXT_107 = ".commit();" + NL + "\t\t\t";
  protected final String TEXT_108 = NL + "\t\t\tconn_";
  protected final String TEXT_109 = ".close();" + NL + "\t\t\t";
  protected final String TEXT_110 = NL + "\t\t\t\tconn_";
  protected final String TEXT_111 = ".setAutoCommit(";
  protected final String TEXT_112 = ");" + NL + "\t\t\t";
  protected final String TEXT_113 = NL + "\t\t\t\tlog.";
  protected final String TEXT_114 = "(\"";
  protected final String TEXT_115 = " - \" + ";
  protected final String TEXT_116 = ".getMessage());" + NL + "\t\t\t";
  protected final String TEXT_117 = NL + "\t    \t\tlog.";
  protected final String TEXT_118 = "(\"";
  protected final String TEXT_119 = "\");" + NL + "\t\t\t";
  protected final String TEXT_120 = NL + "\t\t\t\tpstmt_";
  protected final String TEXT_121 = ".executeBatch();" + NL + "\t\t\t";
  protected final String TEXT_122 = NL + "\t\t\t\tint countSum_";
  protected final String TEXT_123 = " = 0;" + NL + "\t\t\t\tfor(int countEach_";
  protected final String TEXT_124 = ": pstmt_";
  protected final String TEXT_125 = ".executeBatch()) {" + NL + "\t\t\t\t\tcountSum_";
  protected final String TEXT_126 = " += (countEach_";
  protected final String TEXT_127 = " < 0 ? 0 : ";
  protected final String TEXT_128 = ");" + NL + "\t\t\t\t}" + NL + "\t\t\t";
  protected final String TEXT_129 = NL + "\t";
  protected final String TEXT_130 = NL + "\tjava.util.Map<String, routines.system.TalendDataSource> dataSources_";
  protected final String TEXT_131 = " = (java.util.Map<String, routines.system.TalendDataSource>) globalMap.get(KEY_DB_DATASOURCES);" + NL + "\tif (dataSources_";
  protected final String TEXT_132 = " == null) {";
  protected final String TEXT_133 = NL + "\t\t";
  protected final String TEXT_134 = NL + "\t\t";
  protected final String TEXT_135 = NL + NL + "\t\tglobalMap.put(\"conn_";
  protected final String TEXT_136 = "\", conn_";
  protected final String TEXT_137 = ");";
  protected final String TEXT_138 = NL + "\t} else {" + NL + "\t\tString dsAlias_";
  protected final String TEXT_139 = " = ";
  protected final String TEXT_140 = ";" + NL + "\t\tif (dataSources_";
  protected final String TEXT_141 = ".get(dsAlias_";
  protected final String TEXT_142 = ") == null) {" + NL + "   \t\t\tthrow new Exception(\"No DataSource with alias: \" + dsAlias_";
  protected final String TEXT_143 = " + \" available!\");" + NL + "      \t}" + NL + "\t\tconn_";
  protected final String TEXT_144 = " = dataSources_";
  protected final String TEXT_145 = ".get(dsAlias_";
  protected final String TEXT_146 = ").getConnection();" + NL + "\t\tglobalMap.put(\"conn_";
  protected final String TEXT_147 = "\", conn_";
  protected final String TEXT_148 = ");" + NL + "\t}";
  protected final String TEXT_149 = NL + "\tif (null != conn_";
  protected final String TEXT_150 = ") {" + NL + "\t\t";
  protected final String TEXT_151 = NL + "\t}";
  protected final String TEXT_152 = NL + "\t" + NL + "\tglobalMap.put(\"db_";
  protected final String TEXT_153 = "\",";
  protected final String TEXT_154 = ");" + NL + "\t\tString dbname_";
  protected final String TEXT_155 = " = ";
  protected final String TEXT_156 = ";";
  protected final String TEXT_157 = NL + "\tif(dbname_";
  protected final String TEXT_158 = "!=null && !\"\".equals(dbname_";
  protected final String TEXT_159 = ".trim()) && !\"default\".equals(dbname_";
  protected final String TEXT_160 = ".trim())) {" + NL + "\t\tjava.sql.Statement goToDatabase_";
  protected final String TEXT_161 = " = conn_";
  protected final String TEXT_162 = ".createStatement();" + NL + "\t\tgoToDatabase_";
  protected final String TEXT_163 = ".execute(\"use \" + dbname_";
  protected final String TEXT_164 = ");" + NL + "\t\tgoToDatabase_";
  protected final String TEXT_165 = ".close();" + NL + "    }";
  protected final String TEXT_166 = NL + "\t\t";
  protected final String TEXT_167 = NL + "\t\t\troutines.system.GetJarsToRegister getJarsToRegister_";
  protected final String TEXT_168 = " = new routines.system.GetJarsToRegister();" + NL + "\t\t\tjava.sql.Statement addJar_";
  protected final String TEXT_169 = " = null;";
  protected final String TEXT_170 = NL + "\t\t\t\t\t\taddJar_";
  protected final String TEXT_171 = " = conn_";
  protected final String TEXT_172 = ".createStatement();" + NL + "\t\t\t\t\t\ttry {" + NL + "\t\t\t\t\t\t\taddJar_";
  protected final String TEXT_173 = ".execute(\"add jar \" + getJarsToRegister_";
  protected final String TEXT_174 = ".replaceJarPaths(\"";
  protected final String TEXT_175 = "\"));" + NL + "\t\t\t\t\t\t} catch (Exception e) {" + NL + "\t\t\t\t\t\t\te.printStackTrace();" + NL + "\t\t\t\t\t\t} finally {" + NL + "\t\t\t\t\t\t\taddJar_";
  protected final String TEXT_176 = ".close();" + NL + "\t\t\t\t\t\t}";
  protected final String TEXT_177 = NL + "\tjava.sql.Statement stmt_";
  protected final String TEXT_178 = " = conn_";
  protected final String TEXT_179 = ".createStatement();" + NL + "\tString query_";
  protected final String TEXT_180 = " = \"\";" + NL + "\tString tableName_";
  protected final String TEXT_181 = " = ";
  protected final String TEXT_182 = ";";
  protected final String TEXT_183 = NL + "\t\tString likeTableName_";
  protected final String TEXT_184 = " = ";
  protected final String TEXT_185 = ";";
  protected final String TEXT_186 = NL + "\t\t\tString location_";
  protected final String TEXT_187 = " = ";
  protected final String TEXT_188 = ";";
  protected final String TEXT_189 = NL + "\t\tString querySQL_";
  protected final String TEXT_190 = " = \"";
  protected final String TEXT_191 = "\";" + NL + "\t\ttry {" + NL + "    \t\tstmt_";
  protected final String TEXT_192 = ".execute(querySQL_";
  protected final String TEXT_193 = ");" + NL + "    \t} catch(java.sql.SQLException e_";
  protected final String TEXT_194 = ") {";
  protected final String TEXT_195 = NL + "\t\t\t\tthrow e_";
  protected final String TEXT_196 = ";";
  protected final String TEXT_197 = " " + NL + "    \t\t\tSystem.err.println(\"Exception in the component ";
  protected final String TEXT_198 = ":\" + e_";
  protected final String TEXT_199 = ".getMessage());";
  protected final String TEXT_200 = NL + "    \t}" + NL + "\t\tstmt_";
  protected final String TEXT_201 = ".close();\t" + NL + "\t\tglobalMap.put(\"";
  protected final String TEXT_202 = "_QUERY\", querySQL_";
  protected final String TEXT_203 = ");" + NL + "\t\t" + NL + "\t\tString currentClientPathSeparator_";
  protected final String TEXT_204 = " = (String)globalMap.get(\"current_client_path_separator\");" + NL + "\t\tif(currentClientPathSeparator_";
  protected final String TEXT_205 = "!=null) {" + NL + "\t\t\tSystem.setProperty(\"path.separator\", currentClientPathSeparator_";
  protected final String TEXT_206 = ");" + NL + "\t    \tglobalMap.put(\"current_client_path_separator\", null);" + NL + "\t\t}";
  protected final String TEXT_207 = NL + "\t\t\tconn_";
  protected final String TEXT_208 = ".close();";
  protected final String TEXT_209 = NL + "\t";
  protected final String TEXT_210 = NL + "\t\t\t\t\tString comment_";
  protected final String TEXT_211 = "_";
  protected final String TEXT_212 = "_";
  protected final String TEXT_213 = " = ";
  protected final String TEXT_214 = ";";
  protected final String TEXT_215 = NL + "\t\t\t\t\tString comment_";
  protected final String TEXT_216 = "_";
  protected final String TEXT_217 = "_";
  protected final String TEXT_218 = " = ";
  protected final String TEXT_219 = ";";
  protected final String TEXT_220 = NL + "\t\t\t\t\tString key_";
  protected final String TEXT_221 = "_";
  protected final String TEXT_222 = "_";
  protected final String TEXT_223 = " = ";
  protected final String TEXT_224 = ";" + NL + "\t\t\t\t\tString value_";
  protected final String TEXT_225 = "_";
  protected final String TEXT_226 = "_";
  protected final String TEXT_227 = " = ";
  protected final String TEXT_228 = ";";
  protected final String TEXT_229 = NL + "\t\tString tableComment_";
  protected final String TEXT_230 = " = ";
  protected final String TEXT_231 = ";";
  protected final String TEXT_232 = NL + "\t\t\t\tString fieldChar_";
  protected final String TEXT_233 = " = ";
  protected final String TEXT_234 = ";";
  protected final String TEXT_235 = NL + "\t\t\t\t\tString escapeChar_";
  protected final String TEXT_236 = " = ";
  protected final String TEXT_237 = ";";
  protected final String TEXT_238 = NL + "\t\t\t\tString lineChar_";
  protected final String TEXT_239 = " = ";
  protected final String TEXT_240 = ";";
  protected final String TEXT_241 = " " + NL + "\t\t\t\tString decryptedS3Password_";
  protected final String TEXT_242 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_243 = ");";
  protected final String TEXT_244 = NL + "\t\t\t\tString decryptedS3Password_";
  protected final String TEXT_245 = " = ";
  protected final String TEXT_246 = "; ";
  protected final String TEXT_247 = NL + "        String location_";
  protected final String TEXT_248 = " = \"s3n://\" + ";
  protected final String TEXT_249 = " +\":\" + decryptedS3Password_";
  protected final String TEXT_250 = " + \"@\" + ";
  protected final String TEXT_251 = ";";
  protected final String TEXT_252 = NL + "\t\t\tString location_";
  protected final String TEXT_253 = " = ";
  protected final String TEXT_254 = ";";
  protected final String TEXT_255 = NL + "\t\tString select_";
  protected final String TEXT_256 = " = ";
  protected final String TEXT_257 = ";";
  protected final String TEXT_258 = NL + "\tString querySQL_";
  protected final String TEXT_259 = " = \"";
  protected final String TEXT_260 = "\";" + NL + "\ttry {" + NL + "\t\tstmt_";
  protected final String TEXT_261 = ".execute(querySQL_";
  protected final String TEXT_262 = ");" + NL + "\t} catch(java.sql.SQLException e_";
  protected final String TEXT_263 = ") {";
  protected final String TEXT_264 = NL + "\t\t\tthrow e_";
  protected final String TEXT_265 = ";";
  protected final String TEXT_266 = " " + NL + "\t\t\tSystem.err.println(\"Exception in the component ";
  protected final String TEXT_267 = ":\" + e_";
  protected final String TEXT_268 = ".getMessage());";
  protected final String TEXT_269 = NL + "\t}" + NL + "\tstmt_";
  protected final String TEXT_270 = ".close();\t";
  protected final String TEXT_271 = NL + "\t\tconn_";
  protected final String TEXT_272 = ".close();";
  protected final String TEXT_273 = NL + "    " + NL + "\tglobalMap.put(\"";
  protected final String TEXT_274 = "_QUERY\", querySQL_";
  protected final String TEXT_275 = ");";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
	CodeGeneratorArgument createTableCodeGenArgument = (CodeGeneratorArgument) argument;
	INode createTableNode = (INode)createTableCodeGenArgument.getArgument();
	String createTableCid = createTableNode.getUniqueName();
	
	boolean useExistingConnection = "true".equals(ElementParameterParser.getValue(createTableNode,"__USE_EXISTING_CONNECTION__"));
	boolean isLog4jEnabled = "true".equals(ElementParameterParser.getValue(createTableNode.getProcess(), "__LOG4J_ACTIVATE__"));
	boolean dieOnError = "true".equals(ElementParameterParser.getValue(createTableNode, "__DIE_ON_ERROR__"));
	
	if(useExistingConnection) { // We re-use a connection. Get the connection from the global map.
		String connection = ElementParameterParser.getValue(createTableNode, "__CONNECTION__");
		String db = "db_" + connection;

    stringBuffer.append(TEXT_1);
    stringBuffer.append(createTableCid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(createTableCid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(db);
    stringBuffer.append(TEXT_5);
    
	} else { //We do not use an existing connection. Create it.

    stringBuffer.append(TEXT_6);
    
	// ---------------------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------------------------
	// This javajet is included in other tImpala components. Be careful if you rename and modify it.
	// ---------------------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------------------------
	// ---------------------------------------------------------------------------------------------
	
	CodeGeneratorArgument connectionCodeGenArgument = (CodeGeneratorArgument) argument;
	INode connectionNode = (INode)connectionCodeGenArgument.getArgument();
	String connectionCid = connectionNode.getUniqueName();
	
	boolean useKrb = "true".equals(ElementParameterParser.getValue(connectionNode, "__USE_KRB__"));
	String additionalParameters = "\";auth=noSasl\"";
	if(useKrb) {
		additionalParameters = "\";principal=\" + " + ElementParameterParser.getValue(connectionNode, "__IMPALA_PRINCIPAL__");
	}
	
	final String parameters = additionalParameters;
	
	

    stringBuffer.append(TEXT_7);
    
	class DefaultConnectionUtil {
	
		protected String cid ;
		protected String dbproperties ;
		protected String dbhost;
	    protected String dbport;
	    protected String dbname;
	    protected boolean isLog4jEnabled;
	    
	    public void beforeComponentProcess(INode node){
	    }
	    
		public void createURL(INode node) {
			cid = node.getUniqueName();
			dbproperties = ElementParameterParser.getValue(node, "__PROPERTIES__");
			dbhost = ElementParameterParser.getValue(node, "__HOST__");
	    	dbport = ElementParameterParser.getValue(node, "__PORT__");
	    	dbname = ElementParameterParser.getValue(node, "__DBNAME__");
	    	isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
		}
		
		public String getDirverClassName(INode node){
			return "";
		}
		
		public void classForName(INode node){

    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(this.getDirverClassName(node));
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    
		}
		
		public void useShareConnection(INode node) {
			String sharedConnectionName = ElementParameterParser.getValue(node, "__SHARED_CONNECTION_NAME__");
			String shareDBClass = "SharedDBConnection";
			if(isLog4jEnabled){
				shareDBClass = "SharedDBConnectionLog4j";

    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
			}

    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(sharedConnectionName);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(shareDBClass);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(this.getDirverClassName(node));
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    
		}
		
		public void createConnection(INode node) {

    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    
		}
		
		public void setAutoCommit(INode node) {
			boolean setAutoCommit = "true".equals(ElementParameterParser.getValue(node, "__AUTO_COMMIT__"));
			if(isLog4jEnabled){

    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(setAutoCommit);
    stringBuffer.append(TEXT_31);
    
			}

    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(setAutoCommit);
    stringBuffer.append(TEXT_34);
    
		}
		
		public void afterComponentProcess(INode node){
	    }
	}//end DefaultUtil class
	
	DefaultConnectionUtil connUtil = new DefaultConnectionUtil();

    
	class ConnectionUtil extends DefaultConnectionUtil{
		public void createURL(INode node) {
			super.createURL(node);

    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_36);
    stringBuffer.append(dbhost);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(dbport);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(parameters);
    stringBuffer.append(TEXT_40);
    	
		}
	
		public String getDirverClassName(INode node){
			return "org.apache.hive.jdbc.HiveDriver";
		}
		
	}

	connUtil = new ConnectionUtil();

    stringBuffer.append(TEXT_41);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    String dbhost = ElementParameterParser.getValue(node, "__HOST__");
    String dbport = ElementParameterParser.getValue(node, "__PORT__");
    String dbschema = ElementParameterParser.getValue(node, "__DB_SCHEMA__");
    if(dbschema == null||dbschema.trim().length()==0) {
    	 dbschema = ElementParameterParser.getValue(node, "__SCHEMA_DB__");
    }
    String dbname = ElementParameterParser.getValue(node, "__DBNAME__");
    String dbuser = ElementParameterParser.getValue(node, "__USER__");
    String dbpass = ElementParameterParser.getValue(node, "__PASS__");
    String encoding = ElementParameterParser.getValue(node, "__ENCODING__");
    
	boolean isUseSharedConnection = ("true").equals(ElementParameterParser.getValue(node, "__USE_SHARED_CONNECTION__"));

    
	connUtil.beforeComponentProcess(node);
	connUtil.createURL(node);

    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append((dbuser != null && dbuser.trim().length() == 0)? "null":dbuser);
    stringBuffer.append(TEXT_44);
    //the tSQLiteConnection component not contain user and pass return null
    stringBuffer.append(TEXT_45);
    if(dbpass != null && dbpass.trim().length() == 0) {
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    } else {
		String passwordFieldName = "__PASS__";
	
    stringBuffer.append(TEXT_48);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_51);
    } else {
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_54);
    }
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    }
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_59);
    
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
			
    stringBuffer.append(TEXT_60);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_61);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_62);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_65);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_67);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_68);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_70);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_71);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_73);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_75);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_76);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_78);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_79);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_81);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_82);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_84);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_85);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_87);
    
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
			
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    if (cid.startsWith("tImpala") || cid.startsWith("tHive")) {
    stringBuffer.append(TEXT_91);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(connection );
    stringBuffer.append(TEXT_93);
    } else {
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    }
    stringBuffer.append(TEXT_98);
    
			}
		}

		public void connect(INode node){
			beforeComponentProcess(node);
			connect();
		}

		public void connect(){
			connect_begin();
			
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_103);
    
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
			
    stringBuffer.append(TEXT_104);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_105);
    
			logInfo(node,"debug",cid+" - Connection "+connection+"rollback has succeeded.");
		}

		public void commit(INode node){
			beforeComponentProcess(node);
			commit();
		}

		private void commit(){
			commit_begin();
			
    stringBuffer.append(TEXT_106);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_107);
    
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
			
    stringBuffer.append(TEXT_108);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_109);
    
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
			
    stringBuffer.append(TEXT_110);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_111);
    stringBuffer.append(autoCommit);
    stringBuffer.append(TEXT_112);
    
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
	    	
    stringBuffer.append(TEXT_113);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(exception);
    stringBuffer.append(TEXT_116);
    
			}
	    }

	    public void logError(INode node,String logLevel){
	    	logError(node,logLevel,"e");
	    }
	    
	    public void logInfo(INode node,String logLevel,String message){
	    	beforeComponentProcess(node);
	    	if(isLog4jEnabled){
	    	
    stringBuffer.append(TEXT_117);
    stringBuffer.append(logLevel);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(message);
    stringBuffer.append(TEXT_119);
    
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
			
    stringBuffer.append(TEXT_120);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_121);
    
			}else if(batchType==2){
				boolean isMysqlBatchInsert = false;
				if ((node.getUniqueName().contains("tMysqlOutput")||node.getUniqueName().contains("tAmazonMysqlOutput")) && ("INSERT").equals(dataAction)) {
					isMysqlBatchInsert = true;
				}
			
    stringBuffer.append(TEXT_122);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_125);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(isMysqlBatchInsert? "1" : "countEach_"+cid );
    stringBuffer.append(TEXT_128);
    
			}
			if(logBatch){
				logInfo(node,"debug",cid+" - The "+dataAction+" batch execution has succeeded.");
			}
		}
	}

	DefaultLog4jCodeGenerateUtil log4jCodeGenerateUtil = new DefaultLog4jCodeGenerateUtil();

    
	if(isUseSharedConnection){

    stringBuffer.append(TEXT_129);
    connUtil.useShareConnection(node);
    
	} else {
		boolean specify_alias = "true".equals(ElementParameterParser.getValue(node, "__SPECIFY_DATASOURCE_ALIAS__"));
		if(specify_alias){

    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_132);
    
		}

    stringBuffer.append(TEXT_133);
    connUtil.classForName(node);
    stringBuffer.append(TEXT_134);
    
		log4jCodeGenerateUtil.debugConnectionParams(node);
		log4jCodeGenerateUtil.connect_begin();
		connUtil.createConnection(node);
		log4jCodeGenerateUtil.connect_end();
		
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_137);
    
		if(specify_alias){
			String alias = ElementParameterParser.getValue(node, "__DATASOURCE_ALIAS__");

    stringBuffer.append(TEXT_138);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_139);
    stringBuffer.append((null != alias && !("".equals(alias)))?alias:"\"\"");
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_148);
    
		}
	}

    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    connUtil.setAutoCommit(node);
    stringBuffer.append(TEXT_151);
    
	connUtil.afterComponentProcess(node);

    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(createTableCid);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_156);
    
		// The connection object is created in the tImpalaConnection_begin.javajet.
	}
	
	final String cid = createTableCid; // The variable named cid is required in the javajet GetJarsToRegister.

    stringBuffer.append(TEXT_157);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    

	String impalaVersion = ElementParameterParser.getValue(createTableNode, "__IMPALA_VERSION__");
	String distribution = ElementParameterParser.getValue(createTableNode, "__DISTRIBUTION__");
	
	if(useExistingConnection) {
		distribution = "";
		impalaVersion = "";
		String connection = ElementParameterParser.getValue(createTableNode, "__CONNECTION__");
		for (INode pNode : createTableNode.getProcess().getNodesOfType("tHiveConnection")) {
			if(connection!=null && connection.equals(pNode.getUniqueName())) {
				distribution = ElementParameterParser.getValue(pNode, "__DISTRIBUTION__");
				impalaVersion = ElementParameterParser.getValue(pNode, "__HIVE_VERSION__");
				break;
			}
		}
	}
	
	boolean isCustom = "CUSTOM".equals(distribution);
	
	String storageFormat = ElementParameterParser.getValue(createTableNode, "__STORAGE_FORMAT__");
	List<Map<String, String>> serdeProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(createTableNode, "__SERDE_PROPERTIES__");
	boolean setDelimitedRowFormat = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_ROW_FORMAT__"));
	
	boolean setLocation = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_FILE_LOCATION__"));
	String location = ElementParameterParser.getValue(createTableNode, "__FILE_LOCATION__");
	
	boolean isS3Location = "true".equals(ElementParameterParser.getValue(createTableNode, "__S3_LOCATION__"));
	
	String tableName = ElementParameterParser.getValue(createTableNode, "__TABLE__");
	boolean external = "true".equals(ElementParameterParser.getValue(createTableNode, "__CREATE_EXTERNAL__"));
	String tableAction = ElementParameterParser.getValue(createTableNode,"__TABLEACTION__");
	boolean createIfNotExist = "CREATE_IF_NOT_EXIST".equals(tableAction);
	boolean setPartitioned = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_PARTITIONS__"));
	boolean setClustered = false;
	boolean setSkewed = false;
	
	List<Map<String, String>> tableProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(createTableNode, "__TABLE_PROPERTIES__");
	boolean setRowFormat = setDelimitedRowFormat;
	boolean storeAsParquet = "PARQUET".equals(storageFormat);
	boolean storeAsAvro = "AVRO".equals(storageFormat);
	boolean needToGenerateJarsToRegister = "PARQUET".equals(storageFormat);
	
	
	if(needToGenerateJarsToRegister) {

    stringBuffer.append(TEXT_166);
    
	
	//This clazz is used to generate the list of local file path for a given set of jars, which are supposed to be in the job classpath.
	//The constructor takes a String of "," separated jar name.

	class GenerateJarsToRegister {
		private java.util.List<String> jarsToRegister;
		
		public GenerateJarsToRegister(String jarsToRegister) {
			if(jarsToRegister==null) {
				this.jarsToRegister = null;
			} else {
				String[] jarsToRegisterArray = jarsToRegister.split(",");
				this.jarsToRegister = java.util.Arrays.asList(jarsToRegisterArray);
			}
		}
		
		public java.util.List<String> jarsToRegister() {
			return this.jarsToRegister;
		}
		
		public java.util.List<String> getJarsToRegisterPath(String processId) {
			String[] commandLine = new String[] {"<command>"};
			java.util.List<String> jarsPath = new java.util.ArrayList<String>();;
			
			try {
				commandLine = org.talend.designer.runprocess.ProcessorUtilities.getCommandLine("win32",true, processId, "",org.talend.designer.runprocess.IProcessor.NO_STATISTICS,org.talend.designer.runprocess.IProcessor.NO_TRACES, new String[]{});
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			for (int j = 0; j < commandLine.length; j++) {
				if(commandLine[j].contains("jar")) {
					jarsPath = java.util.Arrays.asList(commandLine[j].split(";"));
					break;
				}
			}
			return jarsPath;
		}
	}

    	
		//GenerateJarsToRegister jarsGenInstance = new GenerateJarsToRegister("parquet-hive-bundle,snappy-java");
		GenerateJarsToRegister jarsGenInstance = new GenerateJarsToRegister(null); // This operation is not available for now in Impala JDBC.
		java.util.List<String> jarsPath = jarsGenInstance.getJarsToRegisterPath(createTableNode.getProcess().getId());
		if(jarsPath!=null && jarsGenInstance.jarsToRegister()!=null) {

    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    
			for(String jarToRegister : jarsGenInstance.jarsToRegister()) {
				for(String pathToRegister : jarsPath) {
					if(pathToRegister.contains(jarToRegister)) {

    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_174);
    stringBuffer.append(pathToRegister);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_176);
    
					}
				}
			}
		}
	}

    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_178);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_179);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_180);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_182);
    
	List<IMetadataColumn> listColumn = null;
	List<IMetadataTable> metadatas = createTableNode.getMetadataList();
	if(metadatas != null && metadatas.size() > 0) {
	    IMetadataTable metadata = metadatas.get(0);
	    listColumn = metadata.getListColumns();
	}

	StringBuilder createTableSQL = new StringBuilder();

	createTableSQL.append("CREATE ");
	createTableSQL.append(external || isS3Location ?"EXTERNAL":"");
	createTableSQL.append(" TABLE ");
	createTableSQL.append(createIfNotExist?"IF NOT EXISTS":"");
	createTableSQL.append(" \" + ");
	createTableSQL.append("tableName_");
	createTableSQL.append(cid);
	createTableSQL.append(" + \"");

	boolean likeTable = "true".equals(ElementParameterParser.getValue(createTableNode, "__LIKE_TABLE__"));
	if(likeTable) {
		String likeTableName = ElementParameterParser.getValue(createTableNode, "__LIKE_TABLE_NAME__");

    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(likeTableName);
    stringBuffer.append(TEXT_185);
    
		createTableSQL.append(" LIKE ");
		createTableSQL.append("\" + ");
		createTableSQL.append("likeTableName_");
		createTableSQL.append(cid);
		createTableSQL.append(" + \"");
	
		if(setLocation) {

    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(location);
    stringBuffer.append(TEXT_188);
    
	    	createTableSQL.append(" LOCATION '");
	    	createTableSQL.append("\" + ");
	    	createTableSQL.append("location_");
	    	createTableSQL.append(cid);
	    	createTableSQL.append(" + \"'");
		}

    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_190);
    stringBuffer.append(createTableSQL.toString());
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    
			if(dieOnError) {

    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    
    		}else {

    stringBuffer.append(TEXT_197);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    
    		}

    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_206);
    	
		if(!useExistingConnection) {

    stringBuffer.append(TEXT_207);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_208);
    
		}
		return stringBuffer.toString();
	}

	boolean asSelect = "true".equals(ElementParameterParser.getValue(createTableNode, "__AS_SELECT__"));

	if(!asSelect) {
		createTableSQL.append("(");
	}

	final org.talend.core.model.metadata.MappingTypeRetriever mappingType = org.talend.core.model.metadata.MetadataTalendType.getMappingTypeRetriever("hive_id");

    stringBuffer.append(TEXT_209);
    
	class Util {
		int index = 0;
		java.util.Map<String,String> hiveTypeToAvroType = null;
		String appendKeyValue = null;
	
		void generateColumnsSQL(List<IMetadataColumn> listColumn,StringBuilder createTableSQL) {
			index++;
			int count = 0;
			String ending = ",";
			for(IMetadataColumn metadataColumn : listColumn) {
				createTableSQL.append(metadataColumn.getOriginalDbColumnName());
				createTableSQL.append(" ");
            
				String hiveType = null;
				if(metadataColumn.getType() == null || metadataColumn.getType().trim().length() == 0) {
					hiveType = mappingType.getDefaultSelectedDbType(metadataColumn.getTalendType());
				} else {
					// Replace VARCHAR2 with VARCHAR since Hive can only handle the latter
					hiveType = "VARCHAR2".equals(metadataColumn.getType()) ? "VARCHAR" : metadataColumn.getType();
				}
        		createTableSQL.append(hiveType);
        		if(metadataColumn.getLength() != null && metadataColumn.getPrecision() != null && "DECIMAL".equals(hiveType)) {
        			createTableSQL.append("(" + metadataColumn.getLength() + ", " + metadataColumn.getPrecision() + ")");
        		} else if(metadataColumn.getLength() != null && "VARCHAR".equals(hiveType)) {
        			createTableSQL.append("(" + metadataColumn.getLength() + ")");
        		}
        	
	        	String comment = metadataColumn.getComment();
	        	if(comment!=null && !"".equals(comment) && !"\"\"".equals(comment)) {
	        	
	        		if(!comment.trim().startsWith("\"") && !comment.trim().endsWith("\"")) {
	        			comment = "\"" + comment + "\"";
	        		}

    stringBuffer.append(TEXT_210);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(count);
    stringBuffer.append(TEXT_212);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_213);
    stringBuffer.append(comment);
    stringBuffer.append(TEXT_214);
    
	        		createTableSQL.append(" COMMENT '");
		    		createTableSQL.append("\" + ");
	    			createTableSQL.append("comment_");
	    			createTableSQL.append(index);
	    			createTableSQL.append("_");
	    			createTableSQL.append(count);
	    			createTableSQL.append("_");
	    			createTableSQL.append(cid);
	        		createTableSQL.append(" + \"'");
        		}
            
            if(count == listColumn.size() - 1) {
                ending = "";
            }
            createTableSQL.append(ending);
            count++;
			}
		}
	
		void generateAvroSchema(List<IMetadataColumn> listColumn,StringBuilder schemaBuilder, String quote) {
			if(hiveTypeToAvroType == null) {
				hiveTypeToAvroType = new java.util.HashMap<String,String>();
				hiveTypeToAvroType.put("SMALLINT","int");
				hiveTypeToAvroType.put("FLOAT","float");
				hiveTypeToAvroType.put("DOUBLE","double");
				hiveTypeToAvroType.put("BIGINT","long");
				hiveTypeToAvroType.put("INT","int");
				hiveTypeToAvroType.put("TINYINT","int");
				hiveTypeToAvroType.put("STRING","string");
				hiveTypeToAvroType.put("BOOLEAN","boolean");
				hiveTypeToAvroType.put("STRUCT","record");
				hiveTypeToAvroType.put("MAP","map");
				hiveTypeToAvroType.put("ARRAY","list");
				hiveTypeToAvroType.put("TIMESTAMP","long");
			}
		
			index++;
		
			int count = 0;
			String ending = ",";
			for(IMetadataColumn metadataColumn : listColumn) {
        		schemaBuilder.append("{");
        	
        		schemaBuilder.append(quote).append("name").append(quote);
            schemaBuilder.append(" : ");
            schemaBuilder.append(quote).append(metadataColumn.getOriginalDbColumnName()).append(quote);
            
            schemaBuilder.append(", ");
            
            String hiveType = null;
            if(metadataColumn.getType() == null || metadataColumn.getType().trim().length() == 0) {
                hiveType = mappingType.getDefaultSelectedDbType(metadataColumn.getTalendType());
            } else {
                hiveType = metadataColumn.getType();
            }
            
            String avroType = hiveTypeToAvroType.get(hiveType);
            
            schemaBuilder.append(quote).append("type").append(quote);
            schemaBuilder.append(" : ");
        		schemaBuilder.append(quote).append(avroType).append(quote);
        	
        		String comment = metadataColumn.getComment();
        		if(comment!=null && !"".equals(comment) && !"\"\"".equals(comment)) {
        		
        			if(!comment.trim().startsWith("\"") && !comment.trim().endsWith("\"")) {
	        			comment = "\"" + comment + "\"";
	        		}
        			schemaBuilder.append(", ");

    stringBuffer.append(TEXT_215);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(count);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(comment);
    stringBuffer.append(TEXT_219);
    
	        		schemaBuilder.append(quote).append("doc").append(quote);
	        		schemaBuilder.append(" : ");
	        		schemaBuilder.append(quote);
	        		
		    		schemaBuilder.append("\" + ");
	    			schemaBuilder.append("comment_");
	    			schemaBuilder.append(index);
	    			schemaBuilder.append("_");
	    			schemaBuilder.append(count);
	    			schemaBuilder.append("_");
	    			schemaBuilder.append(cid);
	        		schemaBuilder.append(" + \"");
	        		
	        		schemaBuilder.append(quote);
        		}
            
            schemaBuilder.append("}");
            
            if(count == listColumn.size() - 1) {
                ending = "";
            }
            schemaBuilder.append(ending);
            count++;
			}
		}
	
		void generatePros(String prefix, List<Map<String, String>> pros, StringBuilder createTableSQL) {
			index++;
			
			int count = 0;
			String ending = ",";
			if(pros.size() > 0){
				createTableSQL.append(prefix);
				createTableSQL.append("(");
				for(Map<String, String> item : pros){

    stringBuffer.append(TEXT_220);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(count);
    stringBuffer.append(TEXT_222);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(item.get("PROPERTY") );
    stringBuffer.append(TEXT_224);
    stringBuffer.append(index);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(count);
    stringBuffer.append(TEXT_226);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_227);
    stringBuffer.append(item.get("VALUE") );
    stringBuffer.append(TEXT_228);
    
		    		createTableSQL.append("'\" + ");
	    			createTableSQL.append("key_");
	    			createTableSQL.append(index);
	    			createTableSQL.append("_");
	    			createTableSQL.append(count);
	    			createTableSQL.append("_");
	    			createTableSQL.append(cid);
	        		createTableSQL.append(" + \"'");
	        		
	        		createTableSQL.append(" = ");
	        		
	        		createTableSQL.append("'\" + ");
	    			createTableSQL.append("value_");
	    			createTableSQL.append(index);
	    			createTableSQL.append("_");
	    			createTableSQL.append(count);
	    			createTableSQL.append("_");
	    			createTableSQL.append(cid);
	        		createTableSQL.append(" + \"'");
        		
        			if(count == pros.size() - 1) {
            		ending = "";
        			}
            	
	            createTableSQL.append(ending);
            	count++;
				}
			
				if(appendKeyValue!=null) {
					createTableSQL.append(",");
					createTableSQL.append(appendKeyValue);
					appendKeyValue = null;
				}
			
				createTableSQL.append(")");
			} else if(appendKeyValue!=null) {
				createTableSQL.append(prefix);
				createTableSQL.append("(");
				createTableSQL.append(appendKeyValue);
				createTableSQL.append(")");
				appendKeyValue = null;
			}
		}
	
		void appendKeyValue(String appendKeyValue) {
			this.appendKeyValue = appendKeyValue;
		}
	}

    
	Util util = new Util();

	if(!asSelect) {
		util.generateColumnsSQL(listColumn,createTableSQL);
		createTableSQL.append(")");
	}

	String tableComment = ElementParameterParser.getValue(createTableNode, "__TABLE_COMMENT__");
	if(tableComment!=null && !"".equals(tableComment) && !"\"\"".equals(tableComment)) {

    stringBuffer.append(TEXT_229);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(tableComment);
    stringBuffer.append(TEXT_231);
    
		createTableSQL.append(" COMMENT '");
		createTableSQL.append("\" + ");
		createTableSQL.append("tableComment_");
		createTableSQL.append(cid);
		createTableSQL.append(" + \"'");
	}
	
	if(setPartitioned) {
		if ((metadatas!=null)&&(metadatas.size()>0)) {
			IMetadataTable metadata = metadatas.get(1);
			if (metadata!=null) {
				List<IMetadataColumn> columnList = metadata.getListColumns();
	    		if(columnList != null && columnList.size() > 0) {
	    			createTableSQL.append(" PARTITIONED BY (");
					util.generateColumnsSQL(columnList,createTableSQL);
	    			createTableSQL.append(")");
	    		}
	    	}
		}
	}
	
	if(storeAsParquet || storeAsAvro) {
		util.generatePros(" WITH SERDEPROPERTIES ", serdeProps, createTableSQL);
	}
	
	if(setRowFormat && !storeAsParquet && !storeAsAvro) {
		createTableSQL.append(" ROW FORMAT ");
		if(setDelimitedRowFormat) {
			createTableSQL.append("DELIMITED ");
			boolean setField = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_FIELD_TERMINATED_BY__"));
			if(setField) {
	    		String fieldChar = ElementParameterParser.getValue(createTableNode, "__FIELD_TERMINATED_BY__");

    stringBuffer.append(TEXT_232);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_233);
    stringBuffer.append(fieldChar);
    stringBuffer.append(TEXT_234);
    
	    		createTableSQL.append(" FIELDS TERMINATED BY '");
	    		createTableSQL.append("\" + ");
				createTableSQL.append("fieldChar_");
				createTableSQL.append(cid);
	    		createTableSQL.append(" + \"'");
    		
	    		boolean setEscape = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_FIELD_ESCAPE_BY__"));
	    		if(setEscape) {
	    			String escapeChar = ElementParameterParser.getValue(createTableNode, "__FIELD_ESCAPE_BY__");

    stringBuffer.append(TEXT_235);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_236);
    stringBuffer.append(escapeChar);
    stringBuffer.append(TEXT_237);
    
		    		createTableSQL.append(" ESCAPED BY '");
	        		createTableSQL.append("\" + ");
	    			createTableSQL.append("escapeChar_");
	    			createTableSQL.append(cid);
	        		createTableSQL.append(" + \"'");
	    		}
			}
		
			boolean setLine = "true".equals(ElementParameterParser.getValue(createTableNode, "__SET_LINES_TERMINATED_BY__"));
			if(setLine) {
	    		String lineChar = ElementParameterParser.getValue(createTableNode, "__LINES_TERMINATED_BY__");

    stringBuffer.append(TEXT_238);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(lineChar);
    stringBuffer.append(TEXT_240);
    
	    		createTableSQL.append(" LINES TERMINATED BY '");
	    		createTableSQL.append("\" + ");
				createTableSQL.append("lineChar_");
				createTableSQL.append(cid);
	    		createTableSQL.append(" + \"'");
			}
		}
	}

	createTableSQL.append(" STORED AS ");
	createTableSQL.append(storageFormat);
	if(setLocation) {
		if (isS3Location) {
			String s3bucket = ElementParameterParser.getValue(createTableNode, "__S3_BUCKET__");
			String s3username = ElementParameterParser.getValue(createTableNode, "__S3_USERNAME__");
	    
			String passwordFieldName = "__S3_PASSWORD__";
			// Get the decrypted password under the variable decryptedS3Password
	        
			if (ElementParameterParser.canEncrypt(createTableNode, passwordFieldName)) {

    stringBuffer.append(TEXT_241);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(createTableNode, passwordFieldName));
    stringBuffer.append(TEXT_243);
    
        	} else {

    stringBuffer.append(TEXT_244);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_245);
    stringBuffer.append( ElementParameterParser.getValue(createTableNode, passwordFieldName));
    stringBuffer.append(TEXT_246);
    
			}

    stringBuffer.append(TEXT_247);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(s3username);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(s3bucket);
    stringBuffer.append(TEXT_251);
    
		} else {

    stringBuffer.append(TEXT_252);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(location);
    stringBuffer.append(TEXT_254);
    
		}
		createTableSQL.append(" LOCATION '");
		createTableSQL.append("\" + ");
		createTableSQL.append("location_");
		createTableSQL.append(cid);
		createTableSQL.append(" + \"'");
	}

	util.generatePros(" TBLPROPERTIES ", tableProps, createTableSQL);

	if(asSelect) {
		String sql = ElementParameterParser.getValue(createTableNode, "__SELECT__");
		sql = sql.replaceAll("\n"," ");
		sql = sql.replaceAll("\r"," ");

    stringBuffer.append(TEXT_255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(sql);
    stringBuffer.append(TEXT_257);
    
		createTableSQL.append(" AS ");
		createTableSQL.append("\" + ");
		createTableSQL.append("select_");
		createTableSQL.append(cid);
		createTableSQL.append(" + \"");
	}

    stringBuffer.append(TEXT_258);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_259);
    stringBuffer.append(createTableSQL.toString());
    stringBuffer.append(TEXT_260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_261);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_262);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_263);
    
		if(dieOnError) {

    stringBuffer.append(TEXT_264);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_265);
    
		}else {

    stringBuffer.append(TEXT_266);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_267);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_268);
    
		}

    stringBuffer.append(TEXT_269);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_270);
    
	if(!useExistingConnection) {

    stringBuffer.append(TEXT_271);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_272);
    
	}

    stringBuffer.append(TEXT_273);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_274);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_275);
    return stringBuffer.toString();
  }
}
