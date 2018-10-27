package org.talend.designer.codegen.translators.common;

import org.talend.designer.codegen.config.NodesSubTree;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;
import java.util.Vector;
import java.util.List;

public class Spark_subprocess_footerJava
{
  protected static String nl;
  public static synchronized Spark_subprocess_footerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Spark_subprocess_footerJava result = new Spark_subprocess_footerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t            ";
  protected final String TEXT_2 = "Process(ctx, globalMap);" + NL + "\t        ";
  protected final String TEXT_3 = NL + "\t    } catch(java.lang.Exception e) {";
  protected final String TEXT_4 = NL;
  protected final String TEXT_5 = NL + "                if(log.is";
  protected final String TEXT_6 = "Enabled())";
  protected final String TEXT_7 = NL + "            log.";
  protected final String TEXT_8 = "(\"";
  protected final String TEXT_9 = " - \" ";
  protected final String TEXT_10 = " + (";
  protected final String TEXT_11 = ") ";
  protected final String TEXT_12 = ");";
  protected final String TEXT_13 = NL + "    \tclass BytesLimit65535_";
  protected final String TEXT_14 = "{" + NL + "    \t\tpublic void limitLog4jByte() throws Exception{" + NL + "    \t\t\t";
  protected final String TEXT_15 = NL + "            StringBuilder ";
  protected final String TEXT_16 = " = new StringBuilder();";
  protected final String TEXT_17 = NL + "            ";
  protected final String TEXT_18 = ".append(\"Parameters:\");";
  protected final String TEXT_19 = NL + "                    ";
  protected final String TEXT_20 = ".append(\"";
  protected final String TEXT_21 = "\" + \" = \" + String.valueOf(";
  protected final String TEXT_22 = ").substring(0, 4) + \"...\");     ";
  protected final String TEXT_23 = NL + "                    ";
  protected final String TEXT_24 = ".append(\"";
  protected final String TEXT_25 = "\" + \" = \" + ";
  protected final String TEXT_26 = ");";
  protected final String TEXT_27 = NL + "                ";
  protected final String TEXT_28 = ".append(\" | \");";
  protected final String TEXT_29 = NL + "    \t\t}" + NL + "    \t}" + NL + "    \t" + NL + "        new BytesLimit65535_";
  protected final String TEXT_30 = "().limitLog4jByte();";
  protected final String TEXT_31 = NL + "            StringBuilder ";
  protected final String TEXT_32 = " = new StringBuilder();    ";
  protected final String TEXT_33 = NL + "                    ";
  protected final String TEXT_34 = ".append(";
  protected final String TEXT_35 = ".";
  protected final String TEXT_36 = ");";
  protected final String TEXT_37 = NL + "                    if(";
  protected final String TEXT_38 = ".";
  protected final String TEXT_39 = " == null){";
  protected final String TEXT_40 = NL + "                        ";
  protected final String TEXT_41 = ".append(\"<null>\");" + NL + "                    }else{";
  protected final String TEXT_42 = NL + "                        ";
  protected final String TEXT_43 = ".append(";
  protected final String TEXT_44 = ".";
  protected final String TEXT_45 = ");" + NL + "                    }   ";
  protected final String TEXT_46 = NL + "                ";
  protected final String TEXT_47 = ".append(\"|\");";
  protected final String TEXT_48 = NL + "                if (junitGlobalMap.get(\"tests.log\") == null) {" + NL + "                    junitGlobalMap.put(\"tests.log\", new String());" + NL + "                }" + NL + "                if (junitGlobalMap.get(\"tests.nbFailure\") == null) {" + NL + "                    junitGlobalMap.put(\"tests.nbFailure\", new Integer(0));" + NL + "                }" + NL + "                junitGlobalMap.put(\"tests.nbFailure\"," + NL + "                        ((Integer) junitGlobalMap.get(\"tests.nbFailure\")) + 1);" + NL + "                junitGlobalMap.put(\"tests.log\", \"Error on job: \" + e);";
  protected final String TEXT_49 = NL + "\t    \tthrow e;" + NL + "\t    }" + NL + "\t}\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    INode mrconn = (INode)v.get(0);
    NodesSubTree subTree = (NodesSubTree)v.get(1);
    boolean isLog4jEnabled = false;
    if(mrconn != null) {
        isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(mrconn.getProcess(), "__LOG4J_ACTIVATE__"));
    }

    List<String> beforeSubProcesses = subTree.getBeforeSubProcesses();
    INode firstNode = subTree.getNode(subTree.getName());
    if(firstNode != null) {
	    for(IConnection conn : firstNode.getOutgoingConnections()) {

	        String uniqueNameTargetNode = conn.getTarget().getUniqueName();

	        if (beforeSubProcesses.indexOf(uniqueNameTargetNode) != -1) {
	        
    stringBuffer.append(TEXT_1);
    stringBuffer.append( uniqueNameTargetNode );
    stringBuffer.append(TEXT_2);
    
	        }
	    }
	}

    stringBuffer.append(TEXT_3);
    
            if(isLog4jEnabled) {

    stringBuffer.append(TEXT_4);
    
class BasicLogUtil{
    protected String cid  = "";
    protected org.talend.core.model.process.INode node = null;
    protected boolean log4jEnabled = false;
    private String logID = "";
    
    private BasicLogUtil(){}
    
    public BasicLogUtil(org.talend.core.model.process.INode node){
        this.node = node;
        String cidx = this.node.getUniqueName();
        if(cidx.matches("^.*?tAmazonAuroraOutput_\\d+_out$")){
             cidx = cidx.substring(0,cidx.length()-4);// 4 ==> "_out".length();
        }
        this.cid = cidx;
        this.log4jEnabled = ("true").equals(org.talend.core.model.process.ElementParameterParser.getValue(this.node.getProcess(), "__LOG4J_ACTIVATE__"));
        this.log4jEnabled = this.log4jEnabled &&
                            this.node.getComponent().isLog4JEnabled() && !"JOBLET".equals(node.getComponent().getComponentType().toString());
        this.logID = this.cid;
    }
    
    public String var(String varName){
        return varName + "_" + this.cid;
    }
    public String str(String content){
        return "\"" + content + "\"";
    }
    
    public void info(String... message){
        log4j("info", message);
    }
    
    public void debug(String... message){
        log4j("debug", message);
    }
    
    public void warn(String... message){
        log4j("warn", message);
    }
    
    public void error(String... message){
        log4j("error", message);
    }
    
    public void fatal(String... message){
        log4j("fatal", message);
    }
    
    public void trace(String... message){
        log4j("trace", message);
    }
    java.util.List<String> checkableList = java.util.Arrays.asList(new String[]{"info", "debug", "trace"});     
    public void log4j(String level, String... messages){
        if(this.log4jEnabled){
            if(checkableList.contains(level)){
            
    stringBuffer.append(TEXT_5);
    stringBuffer.append(level.substring(0, 1).toUpperCase() + level.substring(1));
    stringBuffer.append(TEXT_6);
    
            }
            
    stringBuffer.append(TEXT_7);
    stringBuffer.append(level);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(logID);
    stringBuffer.append(TEXT_9);
    for(String message : messages){
    stringBuffer.append(TEXT_10);
    stringBuffer.append(message);
    stringBuffer.append(TEXT_11);
    }
    stringBuffer.append(TEXT_12);
    
        }
    }
    
    public boolean isActive(){
        return this.log4jEnabled;
    }
}

class LogUtil extends BasicLogUtil{
    
    private LogUtil(){
    }
    
    public LogUtil(org.talend.core.model.process.INode node){
        super(node);
    }
    
    public void startWork(){
        debug(str("Start to work."));
    }
    
    public void endWork(){
        debug(str("Done."));
    }
    
    public void logIgnoredException(String exception){
        warn(exception);
    }
    
    public void logPrintedException(String exception){
        error(exception);
    }
    
    public void logException(String exception){
        fatal(exception);
    }
    
    public void logCompSetting(){
    
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
       			 if(log4jEnabled){
       			 
    stringBuffer.append(TEXT_15);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_16);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_18);
    
            java.util.Set<org.talend.core.model.process.EParameterFieldType> ignoredParamsTypes = new java.util.HashSet<org.talend.core.model.process.EParameterFieldType>(); 
            ignoredParamsTypes.addAll(
                java.util.Arrays.asList(
                    org.talend.core.model.process.EParameterFieldType.SCHEMA_TYPE,
                    org.talend.core.model.process.EParameterFieldType.LABEL,
                    org.talend.core.model.process.EParameterFieldType.EXTERNAL,
                    org.talend.core.model.process.EParameterFieldType.MAPPING_TYPE,
                    org.talend.core.model.process.EParameterFieldType.IMAGE,
                    org.talend.core.model.process.EParameterFieldType.TNS_EDITOR,
                    org.talend.core.model.process.EParameterFieldType.WSDL2JAVA,
                    org.talend.core.model.process.EParameterFieldType.GENERATEGRAMMARCONTROLLER,
                    org.talend.core.model.process.EParameterFieldType.GENERATE_SURVIVORSHIP_RULES_CONTROLLER,
                    org.talend.core.model.process.EParameterFieldType.REFRESH_REPORTS,
                    org.talend.core.model.process.EParameterFieldType.BROWSE_REPORTS,
                    org.talend.core.model.process.EParameterFieldType.PALO_DIM_SELECTION,
                    org.talend.core.model.process.EParameterFieldType.GUESS_SCHEMA,
                    org.talend.core.model.process.EParameterFieldType.MATCH_RULE_IMEX_CONTROLLER,
                    org.talend.core.model.process.EParameterFieldType.MEMO_PERL,
                    org.talend.core.model.process.EParameterFieldType.DBTYPE_LIST,
                    org.talend.core.model.process.EParameterFieldType.VERSION,
                    org.talend.core.model.process.EParameterFieldType.TECHNICAL,
                    org.talend.core.model.process.EParameterFieldType.ICON_SELECTION,
                    org.talend.core.model.process.EParameterFieldType.JAVA_COMMAND,
                    org.talend.core.model.process.EParameterFieldType.TREE_TABLE,
                    org.talend.core.model.process.EParameterFieldType.VALIDATION_RULE_TYPE,
                    org.talend.core.model.process.EParameterFieldType.DCSCHEMA,
                    org.talend.core.model.process.EParameterFieldType.SURVIVOR_RELATION,
                    org.talend.core.model.process.EParameterFieldType.REST_RESPONSE_SCHEMA_TYPE
                    )
            );
            for(org.talend.core.model.process.IElementParameter ep : org.talend.core.model.utils.NodeUtil.getDisplayedParameters(node)){
                if(!ep.isLog4JEnabled() || ignoredParamsTypes.contains(ep.getFieldType())){
                    continue;
                }
                String name = ep.getName();
                if(org.talend.core.model.process.EParameterFieldType.PASSWORD.equals(ep.getFieldType())){
                    String epName = "__" + name + "__";
                    String password = "";
                    if(org.talend.core.model.process.ElementParameterParser.canEncrypt(node, epName)){
                        password = org.talend.core.model.process.ElementParameterParser.getEncryptedValue(node, epName);
                    }else{
                        String passwordValue = org.talend.core.model.process.ElementParameterParser.getValue(node, epName);
                        if (passwordValue == null || "".equals(passwordValue.trim())) {// for the value which empty
                            passwordValue = "\"\"";
                        } 
                        password = "routines.system.PasswordEncryptUtil.encryptPassword(" + passwordValue + ")";
                    } 
                    
    stringBuffer.append(TEXT_19);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_20);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(password);
    stringBuffer.append(TEXT_22);
    
                }else{
                    String value = org.talend.core.model.utils.NodeUtil.getNormalizeParameterValue(node, ep);
                    
    stringBuffer.append(TEXT_23);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_24);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(value);
    stringBuffer.append(TEXT_26);
    
                }   
                
    stringBuffer.append(TEXT_27);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_28);
    
            }
        }
		debug(var("log4jParamters"));
		
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    
    }
    
    //no use for now, because we log the data by rowStruct
    public void traceData(String rowStruct, java.util.List<org.talend.core.model.metadata.IMetadataColumn> columnList, String nbline){
        if(log4jEnabled){
        
    stringBuffer.append(TEXT_31);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_32);
    
            for(org.talend.core.model.metadata.IMetadataColumn column : columnList){
                org.talend.core.model.metadata.types.JavaType javaType = org.talend.core.model.metadata.types.JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                String columnName = column.getLabel();
                boolean isPrimit = org.talend.core.model.metadata.types.JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
                if(isPrimit){
                
    stringBuffer.append(TEXT_33);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_34);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_36);
    
                }else{
                
    stringBuffer.append(TEXT_37);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_41);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_43);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_45);
    
                }
                
    stringBuffer.append(TEXT_46);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_47);
    
            }
        }
        trace(str("Content of the record "), nbline, str(": "), var("log4jSb"));
        
    
    }
}

class LogHelper{
    
    java.util.Map<String, String> pastDict = null;
    
    public LogHelper(){
        pastDict = new java.util.HashMap<String, String>();
        pastDict.put("insert", "inserted");
        pastDict.put("update", "updated");
        pastDict.put("delete", "deleted");
        pastDict.put("upsert", "upserted");
    }   
    
    public String upperFirstChar(String data){ 
        return data.substring(0, 1).toUpperCase() + data.substring(1);
    }
    
    public String toPastTense(String data){
        return pastDict.get(data);
    }
}
LogHelper logHelper = new LogHelper();

LogUtil log = null;

    
                log = new LogUtil();
                log.logPrintedException("The subjob starting with the " + (firstNode == null ? "<Unknow>" :firstNode.getUniqueName()) + " has failed.");
            }

    
            // Validate result of test cases
            org.talend.core.model.process.IProcess process = mrconn.getProcess();
            boolean isTestContainer = org.talend.core.model.process.ProcessUtils.isTestContainer(process);
            if (isTestContainer) {
                
    stringBuffer.append(TEXT_48);
    
            }
            
    stringBuffer.append(TEXT_49);
    return stringBuffer.toString();
  }
}
