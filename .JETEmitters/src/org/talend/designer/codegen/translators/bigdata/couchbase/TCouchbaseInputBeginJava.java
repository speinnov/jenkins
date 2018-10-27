package org.talend.designer.codegen.translators.bigdata.couchbase;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;
import java.util.Map;

public class TCouchbaseInputBeginJava
{
  protected static String nl;
  public static synchronized TCouchbaseInputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCouchbaseInputBeginJava result = new TCouchbaseInputBeginJava();
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
  protected final String TEXT_30 = NL + "\t";
  protected final String TEXT_31 = NL + "\tcom.couchbase.client.CouchbaseClient client_";
  protected final String TEXT_32 = " = null;" + NL + "\t";
  protected final String TEXT_33 = NL + "\t    client_";
  protected final String TEXT_34 = " = (com.couchbase.client.CouchbaseClient)globalMap.get(\"conn_";
  protected final String TEXT_35 = "\");" + NL + "\t\t";
  protected final String TEXT_36 = "\t" + NL + "\t\t\tif(client_";
  protected final String TEXT_37 = " != null) {" + NL + "\t\t\t\tlog.info(\"";
  protected final String TEXT_38 = " - Get an existing client from \" + \"";
  protected final String TEXT_39 = "\" + \".\");" + NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_40 = NL + "\t\t\tSystem.setProperty(\"net.spy.log.LoggerImpl\", \"net.spy.memcached.compat.log.Log4JLogger\");" + NL + "\t\t";
  protected final String TEXT_41 = NL + "\t    java.util.ArrayList<java.net.URI> uris_";
  protected final String TEXT_42 = " = new java.util.ArrayList<java.net.URI>();" + NL + "\t    ";
  protected final String TEXT_43 = NL + "\t    \turis_";
  protected final String TEXT_44 = ".add(java.net.URI.create(";
  protected final String TEXT_45 = "));" + NL + "\t    ";
  protected final String TEXT_46 = NL + "\t    ";
  protected final String TEXT_47 = NL + "        ";
  protected final String TEXT_48 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_49 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_50 = ");";
  protected final String TEXT_51 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_52 = " = ";
  protected final String TEXT_53 = "; ";
  protected final String TEXT_54 = NL + "\t\t    " + NL + "\t    " + NL + "\t\tclient_";
  protected final String TEXT_55 = " = new com.couchbase.client.CouchbaseClient(uris_";
  protected final String TEXT_56 = ", ";
  protected final String TEXT_57 = ",";
  protected final String TEXT_58 = ",";
  protected final String TEXT_59 = " decryptedPassword_";
  protected final String TEXT_60 = ");" + NL + "\t";
  protected final String TEXT_61 = NL + "\t" + NL + "\tint nb_line_";
  protected final String TEXT_62 = " = 0;" + NL + "\t" + NL + "\t";
  protected final String TEXT_63 = NL + "\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_64 = ".";
  protected final String TEXT_65 = " = jsonRow_";
  protected final String TEXT_66 = ".get";
  protected final String TEXT_67 = "();" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_68 = NL + "\t\t\t\t\t\t\t\t                ";
  protected final String TEXT_69 = ".";
  protected final String TEXT_70 = " = ParserUtils.parseTo_Date(new Date(jsonRow_";
  protected final String TEXT_71 = ".get";
  protected final String TEXT_72 = "()),";
  protected final String TEXT_73 = ");" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_74 = NL + "\t\t\t\t\t\t\t\t                ";
  protected final String TEXT_75 = ".";
  protected final String TEXT_76 = " = jsonRow_";
  protected final String TEXT_77 = ".get";
  protected final String TEXT_78 = "().getBytes();" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_79 = NL + "\t\t\t\t\t\t\t\t                ";
  protected final String TEXT_80 = ".";
  protected final String TEXT_81 = " = ParserUtils.parseTo_";
  protected final String TEXT_82 = "(jsonRow_";
  protected final String TEXT_83 = ".get";
  protected final String TEXT_84 = "());" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_85 = NL + "\t\t\t\t\t\t\t\t    nb_line_";
  protected final String TEXT_86 = "++;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_87 = NL + "\t\t\t\t\t\t\t\t\t    key_";
  protected final String TEXT_88 = " = ";
  protected final String TEXT_89 = ";" + NL + "\t\t\t\t\t\t\t\t\t    if(key_";
  protected final String TEXT_90 = " instanceof String){" + NL + "\t\t\t\t\t\t\t\t\t        query_";
  protected final String TEXT_91 = ".";
  protected final String TEXT_92 = "(key_";
  protected final String TEXT_93 = ".toString());" + NL + "\t\t\t\t\t\t\t\t\t    }else if(key_";
  protected final String TEXT_94 = " instanceof Object[]){" + NL + "\t\t\t\t\t\t\t\t\t        query_";
  protected final String TEXT_95 = ".";
  protected final String TEXT_96 = "(com.couchbase.client.protocol.views.ComplexKey.of((Object[])key_";
  protected final String TEXT_97 = "));" + NL + "\t\t\t\t\t\t\t\t\t    }else{" + NL + "\t\t\t\t\t\t\t\t\t    \t";
  protected final String TEXT_98 = NL + "\t\t\t\t\t\t\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_99 = " - The type of \"+key_";
  protected final String TEXT_100 = "+\" should be String or Object[]!\");" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_101 = NL + "\t\t\t\t\t\t\t\t\t        throw new RuntimeException(\"The type of \"+key_";
  protected final String TEXT_102 = "+\" should be String or Object[]!\");" + NL + "\t\t\t\t\t\t\t\t\t    }" + NL + "\t\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_103 = NL + "\t\t\t\t\t\t\t\t        ";
  protected final String TEXT_104 = ".";
  protected final String TEXT_105 = " = str_";
  protected final String TEXT_106 = ";" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_107 = NL + "\t\t\t\t\t\t\t\t        ";
  protected final String TEXT_108 = ".";
  protected final String TEXT_109 = " = ParserUtils.parseTo_Date(new Date(str_";
  protected final String TEXT_110 = "),";
  protected final String TEXT_111 = ");" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_112 = NL + "\t\t\t\t\t\t\t\t        ";
  protected final String TEXT_113 = ".";
  protected final String TEXT_114 = " = str_";
  protected final String TEXT_115 = ".getBytes();" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_116 = NL + "\t\t\t\t\t\t\t\t        ";
  protected final String TEXT_117 = ".";
  protected final String TEXT_118 = " = ParserUtils.parseTo_";
  protected final String TEXT_119 = "(str_";
  protected final String TEXT_120 = ");" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_121 = NL + "\t\t\t\t\t\t\t\t    nb_line_";
  protected final String TEXT_122 = "++;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_123 = NL + "\t\t\t\t\t\t\t\t    com.couchbase.client.protocol.views.DesignDocument dd_";
  protected final String TEXT_124 = " = client_";
  protected final String TEXT_125 = ".getDesignDocument(";
  protected final String TEXT_126 = ");" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_127 = NL + "\t\t\t\t\t\t            com.couchbase.client.protocol.views.DesignDocument dd_";
  protected final String TEXT_128 = " = new com.couchbase.client.protocol.views.DesignDocument(";
  protected final String TEXT_129 = ");" + NL + "\t\t\t\t\t\t            ";
  protected final String TEXT_130 = NL + "\t\t\t\t\t\t\t\t    client_";
  protected final String TEXT_131 = ".deleteDesignDoc(";
  protected final String TEXT_132 = ");" + NL + "\t\t\t\t\t\t\t\t    com.couchbase.client.protocol.views.DesignDocument dd_";
  protected final String TEXT_133 = " = new com.couchbase.client.protocol.views.DesignDocument(";
  protected final String TEXT_134 = ");" + NL + "\t\t\t\t\t\t\t\t    ";
  protected final String TEXT_135 = NL + "\t\t\t\t\t\t\t\t    com.couchbase.client.protocol.views.ViewDesign vd_";
  protected final String TEXT_136 = " = new com.couchbase.client.protocol.views.ViewDesign(";
  protected final String TEXT_137 = ",";
  protected final String TEXT_138 = ",";
  protected final String TEXT_139 = ");" + NL + "\t\t\t\t\t\t\t\t    dd_";
  protected final String TEXT_140 = ".setView(vd_";
  protected final String TEXT_141 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_142 = NL + "\t\t\t\t\t\t\t\t    client_";
  protected final String TEXT_143 = ".createDesignDoc(dd_";
  protected final String TEXT_144 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_145 = NL + "\t\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t\tcom.couchbase.client.protocol.views.View view_";
  protected final String TEXT_146 = " = client_";
  protected final String TEXT_147 = ".getView(";
  protected final String TEXT_148 = ", ";
  protected final String TEXT_149 = ");" + NL + "\t\t\t\t\t\t\t\tcom.couchbase.client.protocol.views.Query query_";
  protected final String TEXT_150 = " = new com.couchbase.client.protocol.views.Query();" + NL + "\t\t\t\t\t\t\t\tObject key_";
  protected final String TEXT_151 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_152 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_153 = ".setStartkeyDocID(";
  protected final String TEXT_154 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_155 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_156 = ".setEndkeyDocID(";
  protected final String TEXT_157 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_158 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_159 = ".setKey(";
  protected final String TEXT_160 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_161 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_162 = ".setLimit(";
  protected final String TEXT_163 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_164 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_165 = ".setSkip(";
  protected final String TEXT_166 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_167 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_168 = ".setIncludeDocs(";
  protected final String TEXT_169 = "); " + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_170 = NL + "\t\t\t\t\t\t\t\t    query_";
  protected final String TEXT_171 = ".setReduce(";
  protected final String TEXT_172 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_173 = NL + "\t\t\t\t\t\t\t\t        query_";
  protected final String TEXT_174 = ".setGroup(";
  protected final String TEXT_175 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_176 = NL + "\t\t\t\t\t\t\t\t        query_";
  protected final String TEXT_177 = ".setGroupLevel(";
  protected final String TEXT_178 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_179 = NL + "\t\t\t\t\t\t\t\tquery_";
  protected final String TEXT_180 = ".setStale(com.couchbase.client.protocol.views.Stale.";
  protected final String TEXT_181 = ");" + NL + "\t\t\t\t\t\t\t\tquery_";
  protected final String TEXT_182 = ".setDescending(";
  protected final String TEXT_183 = ");" + NL + "\t\t\t\t\t\t\t\tquery_";
  protected final String TEXT_184 = ".setDebug(";
  protected final String TEXT_185 = ");" + NL + "\t\t\t\t\t\t\t\tquery_";
  protected final String TEXT_186 = ".setInclusiveEnd(";
  protected final String TEXT_187 = "); " + NL + "\t\t\t\t\t\t\t\tcom.couchbase.client.protocol.views.ViewResponse result_";
  protected final String TEXT_188 = " = client_";
  protected final String TEXT_189 = ".query(view_";
  protected final String TEXT_190 = ", query_";
  protected final String TEXT_191 = ");" + NL + "\t\t\t\t\t\t\t\tjava.util.Iterator<com.couchbase.client.protocol.views.ViewRow> itr_";
  protected final String TEXT_192 = " = result_";
  protected final String TEXT_193 = ".iterator();" + NL + "\t\t\t\t\t\t\t\twhile (itr_";
  protected final String TEXT_194 = ".hasNext()) {" + NL + "\t\t\t\t\t\t\t\t    com.couchbase.client.protocol.views.ViewRow  jsonRow_";
  protected final String TEXT_195 = " = itr_";
  protected final String TEXT_196 = ".next();" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_197 = "                            " + NL + "\t\t\t\t\t\t\t\tjava.util.Map<String, Object> Map_";
  protected final String TEXT_198 = " = client_";
  protected final String TEXT_199 = ".getBulk(";
  protected final String TEXT_200 = ");" + NL + "\t\t\t\t\t\t\t\tfor (java.util.Map.Entry<String, Object> m_";
  protected final String TEXT_201 = " : Map_";
  protected final String TEXT_202 = ".entrySet()) {" + NL + "\t\t\t\t\t\t\t\t    Object obj_";
  protected final String TEXT_203 = " = m_";
  protected final String TEXT_204 = ".getValue();" + NL + "\t\t\t\t\t\t\t\t    if(obj_";
  protected final String TEXT_205 = "!=null){" + NL + "\t\t\t\t\t\t\t\t        String str_";
  protected final String TEXT_206 = " = obj_";
  protected final String TEXT_207 = ".toString();" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_208 = NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_209 = ".Key = m_";
  protected final String TEXT_210 = ".getKey();" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_211 = NL + "\t\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_212 = NL;

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
	
    stringBuffer.append(TEXT_30);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	
	INode node = (INode)codeGenArgument.getArgument();
	
	String cid = node.getUniqueName();
	boolean useExistingConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
	if(useExistingConn){
	    String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
		
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_35);
    
		if(isLog4jEnabled){
		
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(connection);
    stringBuffer.append(TEXT_39);
    
		}
	}else{
	    String dbname = ElementParameterParser.getValue(node, "__DBNAME__");
	    String username = ElementParameterParser.getValue(node, "__USER__");

	    List<Map<String, String>> URIList = (List<Map<String,String>>)ElementParameterParser.getObjectValue(node, "__URIS__");
	    if(isLog4jEnabled){
		
    stringBuffer.append(TEXT_40);
    
		}
		
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    
		for(java.util.Map<String,String> map:URIList){
	    
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(map.get("URI"));
    stringBuffer.append(TEXT_45);
    
		}
	    
    stringBuffer.append(TEXT_46);
    
        String passwordFieldName = "__PASS__";
        
    stringBuffer.append(TEXT_47);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_50);
    } else {
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_53);
    }
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(dbname);
    stringBuffer.append(TEXT_57);
    if(username!=null && !"".equals(username)){
    stringBuffer.append(username);
    stringBuffer.append(TEXT_58);
    }
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
	}
	
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    
	List<IMetadataTable> metadatas = node.getMetadataList();
	
	if ((metadatas!=null) && (metadatas.size() > 0)) {
	    IMetadataTable metadata = metadatas.get(0);
	    if (metadata != null) {
	    	
			List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
			if (conns != null){
			
				if (conns.size()>0){
			
					IConnection conn =conns.get(0);
					String connName = conn.getName();
					metadata = conn.getMetadataTable();
					if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			
						List<IMetadataColumn> columns = metadata.getListColumns();
						int nbColumns = columns.size();
						if(nbColumns>0){
						    class Generate{
						    
						    	private boolean isLog4jEnabled;
						    	
						    	public void setIsLog4jEnabled(boolean isLog4jEnabled){
						    		this.isLog4jEnabled = isLog4jEnabled;
						    	}

						        public void generate(String connName,String cid,IMetadataTable metadata){
								    String[] columns = new String[]{"Id","Key","Value","jsonDoc"};
								    for(String columnName:columns){
								        IMetadataColumn column = metadata.getColumn(columnName);
								        if(column!=null){
								            String talendType = column.getTalendType();
								            String typeToGenerate = JavaTypesManager.getTypeToGenerate(talendType, column.isNullable());
								            JavaType javaType = JavaTypesManager.getJavaTypeFromId(talendType);
								            String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
								            String method = columnName;
								            if("jsonDoc".equals(columnName)){
								                method = "Document";
								            }
								            if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
											
    stringBuffer.append(TEXT_63);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_67);
    
								            }else if(javaType == JavaTypesManager.DATE) {
											
    stringBuffer.append(TEXT_68);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(patternValue);
    stringBuffer.append(TEXT_73);
    
								            }else if(javaType == JavaTypesManager.BYTE_ARRAY){
											
    stringBuffer.append(TEXT_74);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_78);
    
								            }else{
											
    stringBuffer.append(TEXT_79);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_81);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_82);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_84);
    
								            }
								        }
								    }
									
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    
						        }
						        public void generateParameter(String cid,Object key,String method){
						            if(key!=null && !"".equals(key.toString().trim()) && !"\"\"".equals(key.toString().trim())){
									    
    stringBuffer.append(TEXT_87);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(key);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(method);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_97);
    
									    	if(isLog4jEnabled){
											
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_100);
    
											}
									    	
    stringBuffer.append(TEXT_101);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_102);
    
									}
						        }
						        public void generateMap(String connName,String cid,IMetadataColumn column){
						            String columnName = column.getLabel();
								    String talendType = column.getTalendType();
								    String typeToGenerate = JavaTypesManager.getTypeToGenerate(talendType, column.isNullable());
								    JavaType javaType = JavaTypesManager.getJavaTypeFromId(talendType);
								    String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
								    if(javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT) {
								    
    stringBuffer.append(TEXT_103);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_105);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_106);
    
								    }else if(javaType == JavaTypesManager.DATE) {
								    
    stringBuffer.append(TEXT_107);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_110);
    stringBuffer.append(patternValue);
    stringBuffer.append(TEXT_111);
    
								    }else if(javaType == JavaTypesManager.BYTE_ARRAY){
								    
    stringBuffer.append(TEXT_112);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_114);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_115);
    
								    }else{
								    
    stringBuffer.append(TEXT_116);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(columnName );
    stringBuffer.append(TEXT_118);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    
								    }
								    
    stringBuffer.append(TEXT_121);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_122);
    
						        }
						    }
						    Generate generateCode = new Generate();
						    generateCode.setIsLog4jEnabled(isLog4jEnabled);
						    String fromview = ElementParameterParser.getValue(node,"__FROMVIEW__");
						    
						    log4jFileUtil.startRetriveDataInfo();
						    if("true".equals(fromview)){
						        String docName = ElementParameterParser.getValue(node, "__DOC__");
						        String viewName = ElementParameterParser.getValue(node, "__VIEW__");
						        String include_docs = ElementParameterParser.getValue(node,"__INCLUDE_DOCS__");
						        String inclusiveend = ElementParameterParser.getValue(node,"__INCLUSIVEEND__");
						        String isreduce = ElementParameterParser.getValue(node,"__ISREDUCE__");
						        String stale = ElementParameterParser.getValue(node,"__STALE__");
						        String debug = ElementParameterParser.getValue(node,"__DEBUG__");
						        String descending = ElementParameterParser.getValue(node,"__DESCENDING__");
						        String useComplexkey = ElementParameterParser.getValue(node,"__USECOMPLEXKEY__");
						        Object complexkey = ElementParameterParser.getValue(node,"__COMPLEXKEY__");
						        String viewkey = ElementParameterParser.getValue(node, "__VIEWKEY__");
						        Object startkey = ElementParameterParser.getValue(node,"__STARTKEY__");
						        Object endkey = ElementParameterParser.getValue(node,"__ENDKEY__");
						        String startkey_docid = ElementParameterParser.getValue(node,"__STARTKEY_DOCID__");
						        String endkey_docid = ElementParameterParser.getValue(node,"__ENDKEY_DOCID__");
						        String group = ElementParameterParser.getValue(node,"__GROUP__");
						        String group_level = ElementParameterParser.getValue(node,"__GROUP_LEVEL__");
						        String doc_action = ElementParameterParser.getValue(node,"__DOC_ACTION__");
						        String view_action = ElementParameterParser.getValue(node,"__VIEW_ACTION__");
						        String map = ElementParameterParser.getValue(node,"__MAP__");
						        String reduce = ElementParameterParser.getValue(node,"__REDUCE__");
						        String limit = ElementParameterParser.getValue(node, "__LIMIT__");
						        String skip = ElementParameterParser.getValue(node, "__SKIP__");
						        if("NONE".equals(doc_action) && "CREATE".equals(view_action)){
								    
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(docName);
    stringBuffer.append(TEXT_126);
    
								}else if("CREATE".equals(doc_action)){
						            
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(docName);
    stringBuffer.append(TEXT_129);
    
						        }else if("DROP_CREATE".equals(doc_action)){
								    
    stringBuffer.append(TEXT_130);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(docName);
    stringBuffer.append(TEXT_132);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(docName);
    stringBuffer.append(TEXT_134);
    
								}
						        if("CREATE".equals(view_action)){
								
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(viewName);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(map);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(reduce);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    
						        }
						        if("CREATE".equals(view_action) || !"NONE".equals(doc_action)){
								
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    
						        }
								
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(docName);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(viewName);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_151);
    
								generateCode.generateParameter(cid,startkey,"setRangeStart");
								generateCode.generateParameter(cid,endkey,"setRangeEnd");
								if(startkey_docid!=null && !"".equals(startkey_docid.replaceAll("\\\"", ""))){
								
    stringBuffer.append(TEXT_152);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(startkey_docid);
    stringBuffer.append(TEXT_154);
    
								}
								if(endkey_docid!=null && !"".equals(endkey_docid.replaceAll("\\\"", ""))){
								
    stringBuffer.append(TEXT_155);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(endkey_docid);
    stringBuffer.append(TEXT_157);
    
								}
								if("true".equals(useComplexkey)){
									generateCode.generateParameter(cid,complexkey,"setKeys");
								} else if(viewkey!=null &&!"".equals(viewkey.replaceAll("\\\"", ""))){
								
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(viewkey);
    stringBuffer.append(TEXT_160);
    
								}
								if(limit!=null &&!"".equals(limit)){
								
    stringBuffer.append(TEXT_161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(limit);
    stringBuffer.append(TEXT_163);
    
								}
								if(skip!=null &&!"".equals(skip)){
								
    stringBuffer.append(TEXT_164);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(skip);
    stringBuffer.append(TEXT_166);
    
								}
								if("false".equals(isreduce)&&"true".equals(include_docs)){
								
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(include_docs);
    stringBuffer.append(TEXT_169);
    
								}else if("true".equals(isreduce)&&"false".equals(include_docs)){
								
    stringBuffer.append(TEXT_170);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(isreduce);
    stringBuffer.append(TEXT_172);
    
								    if("true".equals(group)){
									
    stringBuffer.append(TEXT_173);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(group);
    stringBuffer.append(TEXT_175);
    
								    }
								    if(group_level!=null && !"".equals(group_level.trim())){
									
    stringBuffer.append(TEXT_176);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_177);
    stringBuffer.append(group_level);
    stringBuffer.append(TEXT_178);
    
								    }
								}
								// fromview true reduce false doc false --->id,key,value ----->real component
								// fromview true reduce true doc false --->key value ----->real component
								// fromview true reduce false doc true --->id key value doc
								// fromview false isjson true --->id doc
								// fromview false isjson false --->id value ----->real component
								
    stringBuffer.append(TEXT_179);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(stale);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(descending);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(debug);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(inclusiveend);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    
								    generateCode.generate(connName,cid,metadata);
								    log4jFileUtil.debugRetriveData(node);
							}else{
								String searchKey = ElementParameterParser.getValue(node, "__KEY__");
								
    stringBuffer.append(TEXT_197);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(searchKey);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_207);
    
										if(metadata.getColumn("Key")!=null){
										
    stringBuffer.append(TEXT_208);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_210);
    
										}
								        IMetadataColumn column = metadata.getColumn("Value");
								        if(column!=null){
								            generateCode.generateMap(connName,cid,column);
								        }
										log4jFileUtil.debugRetriveData(node);
										
    stringBuffer.append(TEXT_211);
    
							}
						}
					}
				}
			}
		}
	}
	
    stringBuffer.append(TEXT_212);
    return stringBuffer.toString();
  }
}
