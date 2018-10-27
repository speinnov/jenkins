package org.talend.designer.codegen.translators.common;

import org.talend.core.model.process.IProcess;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.designer.runprocess.CodeGeneratorRoutine;
import org.talend.designer.codegen.i18n.Messages;
import org.talend.core.ui.branding.IBrandingService;
import org.talend.core.ui.branding.AbstractBrandingService;
import org.talend.core.GlobalServiceRegister;
import org.talend.designer.codegen.ITalendSynchronizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;
import org.talend.core.model.process.IContextParameter;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.utils.NodeUtil;
import org.talend.core.model.utils.JavaResourcesHelper;

public class Mr_headerJava
{
  protected static String nl;
  public static synchronized Mr_headerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Mr_headerJava result = new Mr_headerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "package ";
  protected final String TEXT_3 = ";" + NL;
  protected final String TEXT_4 = NL + "import routines.";
  protected final String TEXT_5 = ";";
  protected final String TEXT_6 = NL + "import routines.system.*;" + NL + "import routines.system.api.*;" + NL + "import java.text.ParseException;" + NL + "import java.text.SimpleDateFormat;" + NL + "import java.util.Date;" + NL + "import java.util.List;" + NL + "import java.math.BigDecimal;" + NL + "import java.io.ByteArrayOutputStream;" + NL + "import java.io.ByteArrayInputStream;" + NL + "import java.io.DataInputStream;" + NL + "import java.io.DataOutputStream;" + NL + "import java.io.ObjectOutputStream;" + NL + "import java.io.ObjectInputStream;" + NL + "import java.io.IOException;" + NL + "import java.io.InputStream; " + NL + "import java.io.DataInput;" + NL + "import java.io.DataOutput;" + NL + "import java.io.IOException;" + NL + "import java.io.Serializable;" + NL + "import java.util.Comparator;" + NL + "import java.util.Iterator;" + NL + "import java.security.PrivilegedExceptionAction;" + NL + "import java.sql.PreparedStatement;" + NL + "import java.sql.ResultSet;" + NL + "import java.sql.SQLException;" + NL + "" + NL + "import org.apache.hadoop.conf.Configuration;" + NL + "import org.apache.hadoop.conf.Configured;" + NL + "import org.apache.hadoop.fs.FileSystem;" + NL + "import org.apache.hadoop.fs.FSDataInputStream;" + NL + "import org.apache.hadoop.fs.FSDataOutputStream;" + NL + "import org.apache.hadoop.fs.Path;" + NL + "import org.apache.hadoop.io.compress.CompressionCodec;" + NL + "import org.apache.hadoop.io.compress.CompressionCodecFactory;" + NL + "import org.apache.hadoop.io.compress.GzipCodec;" + NL + "import org.apache.hadoop.io.LongWritable;" + NL + "import org.apache.hadoop.io.NullWritable;" + NL + "import org.apache.hadoop.io.Text;" + NL + "import org.apache.hadoop.io.Writable;" + NL + "import org.apache.hadoop.io.WritableComparable;" + NL + "import org.apache.hadoop.io.WritableComparator;" + NL + "import org.apache.hadoop.io.WritableUtils;" + NL + "import org.apache.hadoop.io.SequenceFile;" + NL + "import org.apache.hadoop.mapred.FileAlreadyExistsException;" + NL + "import org.apache.hadoop.mapred.FileInputFormat;" + NL + "import org.apache.hadoop.mapred.SequenceFileInputFormat;" + NL + "import org.apache.hadoop.mapred.FileOutputFormat;" + NL + "import org.apache.hadoop.mapred.FileSplit;" + NL + "import org.apache.hadoop.mapred.InputSplit;" + NL + "import org.apache.hadoop.mapred.InvalidJobConfException;" + NL + "import org.apache.hadoop.mapred.JobClient;" + NL + "import org.apache.hadoop.mapred.JobConf;" + NL + "import org.apache.hadoop.mapred.JobConfigurable;" + NL + "import org.apache.hadoop.mapred.Mapper;" + NL + "import org.apache.hadoop.mapred.MapReduceBase;" + NL + "import org.apache.hadoop.mapred.OutputCollector;" + NL + "import org.apache.hadoop.mapred.RecordReader;" + NL + "import org.apache.hadoop.mapred.RecordWriter;" + NL + "import org.apache.hadoop.mapred.Reducer;" + NL + "import org.apache.hadoop.mapred.Reporter;" + NL + "import org.apache.hadoop.mapred.InputFormat;" + NL + "import org.apache.hadoop.mapred.OutputFormat;" + NL + "import org.apache.hadoop.security.UserGroupInformation;" + NL + "import org.apache.hadoop.util.Tool;" + NL + "import org.apache.hadoop.util.ToolRunner;" + NL + "import org.apache.hadoop.util.Progressable;" + NL + "import org.apache.hadoop.util.ReflectionUtils;" + NL + "import org.apache.commons.lang.SerializationUtils;" + NL + "import org.apache.commons.codec.binary.Base64;" + NL + "import org.apache.commons.codec.binary.StringUtils;" + NL + "import org.talend.hadoop.mapred.lib.ChainMapper;" + NL + "import org.talend.hadoop.mapred.lib.ChainReducer;" + NL + "import org.talend.hadoop.mapred.lib.MRJobClient;" + NL + "import org.talend.hadoop.mapred.lib.MultipleInputs;" + NL + "import org.talend.hadoop.mapred.lib.MultipleOutputs; " + NL;
  protected final String TEXT_7 = NL;
  protected final String TEXT_8 = NL;
  protected final String TEXT_9 = NL + NL;
  protected final String TEXT_10 = NL + "    //the import part of ";
  protected final String TEXT_11 = NL + "    ";
  protected final String TEXT_12 = NL;
  protected final String TEXT_13 = NL + NL + "@SuppressWarnings(\"unused\")" + NL + "" + NL + "/**" + NL + " * Job: ";
  protected final String TEXT_14 = " Purpose: ";
  protected final String TEXT_15 = "<br>" + NL + " * Description: ";
  protected final String TEXT_16 = " <br>" + NL + " * @author ";
  protected final String TEXT_17 = NL + " * @version ";
  protected final String TEXT_18 = NL + " * @status ";
  protected final String TEXT_19 = NL + " */" + NL + "public class ";
  protected final String TEXT_20 = " extends Configured implements Tool, TalendJob {" + NL + "    static {" + NL + "        System.setProperty(\"TalendJob.log\", \"";
  protected final String TEXT_21 = ".log\");" + NL + "    }" + NL + "    private final static String utf8Charset = \"UTF-8\";" + NL + "    private GlobalVar globalMap = null;" + NL + "\t";
  protected final String TEXT_22 = NL + "    private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger" + NL + "            .getLogger(";
  protected final String TEXT_23 = ".class);" + NL + "" + NL + "    // DI compatibility" + NL + "    private static org.apache.log4j.Logger log = LOG;" + NL + "\t";
  protected final String TEXT_24 = NL + NL + "    private MRJobClient mrJobClient;" + NL + "    " + NL + "    private static class GlobalVar{" + NL + "    \tpublic static final String GLOBALVAR_PARAMS_PREFIX = \"talend.globalvar.params.\";" + NL + "\t\tprivate Configuration job;" + NL + "\t\tprivate java.util.Map<String, Object> map;" + NL + "\t\t" + NL + "\t\tpublic GlobalVar(Configuration job){" + NL + "\t\t\tthis.job = job;" + NL + "\t\t\tthis.map = new java.util.HashMap<String, Object>();" + NL + "\t\t}" + NL + "\t\t" + NL + "\t\tpublic Object get(String key){" + NL + "\t\t\tString tempValue = job.get(GLOBALVAR_PARAMS_PREFIX + key);" + NL + "\t\t\tif(tempValue != null){" + NL + "\t\t\t\treturn SerializationUtils.deserialize(Base64.decodeBase64(StringUtils.getBytesUtf8(tempValue)));" + NL + "\t\t\t}else{" + NL + "\t\t\t\treturn null;" + NL + "\t\t\t}\t" + NL + "\t\t}" + NL + "\t\t" + NL + "\t\tpublic void put(String key, Object value){" + NL + "\t\t\tif(value == null)" + NL + "\t\t\t\treturn;" + NL + "\t\t\tjob.set(GLOBALVAR_PARAMS_PREFIX + key, StringUtils.newStringUtf8(Base64.encodeBase64(SerializationUtils.serialize((Serializable)value))));" + NL + "\t\t}" + NL + "\t\t" + NL + "\t\tpublic void putLocal(String key, Object value){" + NL + "\t\t\tmap.put(key, value);" + NL + "\t\t}" + NL + "\t\t" + NL + "\t\tpublic Object getLocal(String key){" + NL + "\t\t\treturn map.get(key);" + NL + "\t\t}" + NL + "    }" + NL + "    " + NL + "\t// create and load default properties" + NL + "    private java.util.Properties defaultProps = new java.util.Properties();" + NL + "    // create application properties with default" + NL + "    public static class ContextProperties extends java.util.Properties {" + NL + "" + NL + "        private static final long serialVersionUID = 1L;" + NL + "        " + NL + "        public static final String CONTEXT_FILE_NAME = \"talend.context.fileName\";" + NL + "        public static final String CONTEXT_KEYS = \"talend.context.keys\";" + NL + "        public static final String CONTEXT_PARAMS_PREFIX = \"talend.context.params.\";" + NL + "        public static final String CONTEXT_PARENT_KEYS = \"talend.context.parent.keys\";" + NL + "        public static final String CONTEXT_PARENT_PARAMS_PREFIX = \"talend.context.parent.params.\";" + NL + "" + NL + "        public ContextProperties(java.util.Properties properties){" + NL + "            super(properties);" + NL + "        }" + NL + "        public ContextProperties(){" + NL + "            super();" + NL + "        }" + NL + "        public ContextProperties(Configuration job){" + NL + "        \tsuper();" + NL + "\t\t\tString contextFileName = job.get(CONTEXT_FILE_NAME);" + NL + "        \ttry {" + NL + "\t\t\t\tif(contextFileName != null && !\"\".equals(contextFileName)){" + NL + "\t\t\t\t\tjava.io.File contextFile = new java.io.File(contextFileName);" + NL + "\t\t\t\t\tif(contextFile.exists()){" + NL + "\t\t\t\t\t\tjava.io.InputStream contextIn = contextFile.toURI().toURL().openStream();" + NL + "\t\t\t\t\t\tthis.load(contextIn);" + NL + "\t\t\t\t\t\tcontextIn.close();" + NL + "\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_25 = NL + "\t\t\t\t\t\tjava.io.InputStream contextIn = ";
  protected final String TEXT_26 = ".class.getClassLoader().getResourceAsStream(\"";
  protected final String TEXT_27 = "/";
  protected final String TEXT_28 = "/contexts/\"+contextFileName);" + NL + "\t\t\t\t\t\tif(contextIn != null){" + NL + "\t\t\t\t\t\t\tthis.load(contextIn);" + NL + "\t\t\t\t\t\t\tcontextIn.close();" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t\t\tjava.util.StringTokenizer st = new java.util.StringTokenizer(job.get(CONTEXT_KEYS, \"\"), \" \");" + NL + "\t\t\t\twhile (st.hasMoreTokens()) {" + NL + "\t\t\t\t\tString contextKey = st.nextToken();" + NL + "\t\t\t\t\tif(job.get(CONTEXT_PARAMS_PREFIX + contextKey) != null){" + NL + "\t            \t\tthis.put(contextKey, job.get(CONTEXT_PARAMS_PREFIX + contextKey));" + NL + "\t            \t}" + NL + "\t\t        }" + NL + "\t\t        st = new java.util.StringTokenizer(job.get(CONTEXT_PARENT_KEYS, \"\"), \" \");" + NL + "\t\t\t\twhile (st.hasMoreTokens()) {" + NL + "\t\t\t\t\tString contextKey = st.nextToken();" + NL + "\t\t\t\t\tif(job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey) != null){" + NL + "\t            \t\tthis.put(contextKey, job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey));" + NL + "\t            \t}" + NL + "\t\t        }" + NL + "\t\t        " + NL + "\t\t\t\tthis.loadValue(null,job);" + NL + "\t\t\t} catch (java.io.IOException ie) {" + NL + "\t\t\t\tSystem.err.println(\"Could not load context \" + contextFileName);" + NL + "\t\t\t\tie.printStackTrace();" + NL + "\t\t\t}" + NL + "        }" + NL + "" + NL + "        public void synchronizeContext(){";
  protected final String TEXT_29 = NL + "                if(";
  protected final String TEXT_30 = " != null){";
  protected final String TEXT_31 = NL + "                        String pattern_";
  protected final String TEXT_32 = " = \"yyyy-MM-dd HH:mm:ss\";" + NL + "                        String value_";
  protected final String TEXT_33 = " = \"";
  protected final String TEXT_34 = "\";" + NL + "                        String[] parts_";
  protected final String TEXT_35 = " = value_";
  protected final String TEXT_36 = ".split(\";\");" + NL + "                        if(parts_";
  protected final String TEXT_37 = ".length > 1){" + NL + "                            pattern_";
  protected final String TEXT_38 = " = parts_";
  protected final String TEXT_39 = "[0];" + NL + "                            this.setProperty(\"";
  protected final String TEXT_40 = "\", pattern_";
  protected final String TEXT_41 = " + \";\" + FormatterUtils.format_DateInUTC(";
  protected final String TEXT_42 = ", pattern_";
  protected final String TEXT_43 = "));" + NL + "                        }else{" + NL + "                            this.setProperty(\"";
  protected final String TEXT_44 = "\", FormatterUtils.format_DateInUTC(";
  protected final String TEXT_45 = ", pattern_";
  protected final String TEXT_46 = "));" + NL + "                        }";
  protected final String TEXT_47 = NL + "                        this.setProperty(\"";
  protected final String TEXT_48 = "\", ";
  protected final String TEXT_49 = ".toString());";
  protected final String TEXT_50 = NL + "                }";
  protected final String TEXT_51 = NL + "        }" + NL;
  protected final String TEXT_52 = NL + "                public String ";
  protected final String TEXT_53 = ";" + NL + "                public String get";
  protected final String TEXT_54 = "(){" + NL + "                    return this.";
  protected final String TEXT_55 = ";" + NL + "                }";
  protected final String TEXT_56 = NL + "                public ";
  protected final String TEXT_57 = " ";
  protected final String TEXT_58 = ";" + NL + "                public ";
  protected final String TEXT_59 = " get";
  protected final String TEXT_60 = "(){" + NL + "                    return this.";
  protected final String TEXT_61 = ";" + NL + "                }";
  protected final String TEXT_62 = NL + "        public void loadValue(java.util.Properties context_param, Configuration job){" + NL + "\t        ";
  protected final String TEXT_63 = NL + "            \t\tString pwd_";
  protected final String TEXT_64 = "_value = this.getProperty(\"";
  protected final String TEXT_65 = "\");" + NL + "            \t\tthis.";
  protected final String TEXT_66 = " = null;" + NL + "            \t\tif(pwd_";
  protected final String TEXT_67 = "_value!=null) {" + NL + "            \t\t\t//no need to decrypt if it come from program argument or parent job runtime" + NL + "            \t\t\tif((context_param!=null && context_param.containsKey(\"";
  protected final String TEXT_68 = "\"))" + NL + "            \t\t\t || (job!=null && job.get(CONTEXT_PARAMS_PREFIX + \"";
  protected final String TEXT_69 = "\") != null) " + NL + "            \t\t\t || (job!=null && job.get(CONTEXT_PARENT_PARAMS_PREFIX + \"";
  protected final String TEXT_70 = "\") != null)){" + NL + "            \t\t\t\tthis.";
  protected final String TEXT_71 = " = pwd_";
  protected final String TEXT_72 = "_value;" + NL + "            \t\t\t} else if (!pwd_";
  protected final String TEXT_73 = "_value.isEmpty()) {" + NL + "            \t\t\t\ttry {" + NL + "            \t\t\t\t\tthis.";
  protected final String TEXT_74 = " = routines.system.PasswordEncryptUtil.decryptPassword(pwd_";
  protected final String TEXT_75 = "_value);" + NL + "            \t\t\t\t} catch (java.lang.RuntimeException e) {" + NL + "            \t\t\t\t\t//do nothing" + NL + "            \t\t\t\t}" + NL + "            \t\t\t}" + NL + "            \t\t}" + NL + "            \t";
  protected final String TEXT_76 = NL + "\t\t            try{" + NL + "\t\t                String context_";
  protected final String TEXT_77 = "_value = this.getProperty(\"";
  protected final String TEXT_78 = "\");" + NL + "\t\t                if (context_";
  protected final String TEXT_79 = "_value == null){" + NL + "\t\t                    context_";
  protected final String TEXT_80 = "_value = \"\";" + NL + "\t\t                }" + NL + "\t\t                int context_";
  protected final String TEXT_81 = "_pos = context_";
  protected final String TEXT_82 = "_value.indexOf(\";\");" + NL + "\t\t                String context_";
  protected final String TEXT_83 = "_pattern =  \"yyyy-MM-dd HH:mm:ss\";" + NL + "\t\t                if(context_";
  protected final String TEXT_84 = "_pos > -1){" + NL + "\t\t                    context_";
  protected final String TEXT_85 = "_pattern = context_";
  protected final String TEXT_86 = "_value.substring(0, context_";
  protected final String TEXT_87 = "_pos);" + NL + "\t\t                    context_";
  protected final String TEXT_88 = "_value = context_";
  protected final String TEXT_89 = "_value.substring(context_";
  protected final String TEXT_90 = "_pos + 1);" + NL + "\t\t                }" + NL + "\t" + NL + "\t\t                this.";
  protected final String TEXT_91 = "=(java.util.Date)(new java.text.SimpleDateFormat(context_";
  protected final String TEXT_92 = "_pattern).parse(context_";
  protected final String TEXT_93 = "_value));" + NL + "\t" + NL + "\t\t            }catch(ParseException e){" + NL + "\t\t                this.";
  protected final String TEXT_94 = "=null;" + NL + "\t\t            }" + NL + "\t          \t";
  protected final String TEXT_95 = NL + "\t            \tthis.";
  protected final String TEXT_96 = "=(";
  protected final String TEXT_97 = ") this.getProperty(\"";
  protected final String TEXT_98 = "\");" + NL + "\t            ";
  protected final String TEXT_99 = NL + "\t         \t\tthis.";
  protected final String TEXT_100 = "= new java.text.StringCharacterIterator(this.getProperty(\"";
  protected final String TEXT_101 = "\")).first();" + NL + "\t     \t\t";
  protected final String TEXT_102 = " " + NL + "\t         \t\ttry{" + NL + "\t             \t\tthis.";
  protected final String TEXT_103 = "=routines.system.BigDataParserUtils.parseTo_";
  protected final String TEXT_104 = "(this.getProperty(\"";
  protected final String TEXT_105 = "\"));" + NL + "\t         \t\t}catch(NumberFormatException e){" + NL + "\t             \t\tthis.";
  protected final String TEXT_106 = "=null;" + NL + "\t          \t\t}" + NL + "\t         \t";
  protected final String TEXT_107 = NL + "\t    }" + NL + "    }" + NL + "    private ContextProperties context = new ContextProperties();" + NL + "    public ContextProperties getContext() {" + NL + "        return this.context;" + NL + "    }" + NL + "" + NL + "\tprivate final static String jobVersion = \"";
  protected final String TEXT_108 = "\";" + NL + "\tprivate final static String jobName = \"";
  protected final String TEXT_109 = "\";" + NL + "\tprivate final static String projectName = \"";
  protected final String TEXT_110 = "\";" + NL + "\tpublic Integer errorCode = null;" + NL + "\t" + NL + "\tprivate final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();" + NL + "\tprivate final java.io.PrintStream errorMessagePS = new java.io.PrintStream(" + NL + "\t\t\tnew java.io.BufferedOutputStream(baos));" + NL + "" + NL + "\tpublic String getExceptionStackTrace() {" + NL + "\t\tif (\"failure\".equals(this.getStatus())) {" + NL + "\t\t\terrorMessagePS.flush();" + NL + "\t\t\treturn baos.toString();" + NL + "\t\t}" + NL + "\t\treturn null;" + NL + "\t}" + NL + "\t" + NL + "\t//should be remove later" + NL + "\tpublic void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {" + NL + "\t\t" + NL + "\t}" + NL;
  protected final String TEXT_111 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    IProcess process = (IProcess)v.get(0);
    String version = (String)v.get(1);

    List< ? extends INode> processNodes = (List< ? extends INode>)process.getGeneratingNodes();
    List<IContextParameter> params = new ArrayList<IContextParameter>();
    params=process.getContextManager().getDefaultContext().getContextParameterList();

    
//?
IBrandingService service=(IBrandingService)GlobalServiceRegister.getDefault().getService(IBrandingService.class);
if(service instanceof AbstractBrandingService){

    stringBuffer.append(TEXT_1);
    stringBuffer.append(((AbstractBrandingService) service).getJobLicenseHeader(version));
    
}
    String jobFolderName = JavaResourcesHelper.getJobFolderName(process.getName(), process.getVersion());
    String packageName = codeGenArgument.getCurrentProjectName().toLowerCase() + "." + jobFolderName;
    boolean isLog4jEnabled = ("true").equals(org.talend.core.model.process.ElementParameterParser.getValue(process, "__LOG4J_ACTIVATE__"));

    stringBuffer.append(TEXT_2);
    stringBuffer.append( packageName );
    stringBuffer.append(TEXT_3);
    for (String routine : CodeGeneratorRoutine.getRequiredRoutineName(process)) {
    if(!routine.equals(ITalendSynchronizer.TEMPLATE)){
    stringBuffer.append(TEXT_4);
    stringBuffer.append(routine);
    stringBuffer.append(TEXT_5);
      }
}
    stringBuffer.append(TEXT_6);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(ElementParameterParser.getValue(process, "__HEADER_IMPORT__") );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(ElementParameterParser.getValue(process, "__FOOTER_IMPORT__") );
    stringBuffer.append(TEXT_9);
    
    List<INode> nodesWithImport = process.getNodesWithImport();
    if(nodesWithImport != null) {
        for(INode node:nodesWithImport){

    stringBuffer.append(TEXT_10);
    stringBuffer.append(node.getUniqueName() );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(ElementParameterParser.getValue(node, "__IMPORT__") );
    stringBuffer.append(TEXT_12);
          }
    }

    stringBuffer.append(TEXT_13);
    stringBuffer.append(process.getName() );
    stringBuffer.append(TEXT_14);
    stringBuffer.append(ElementParameterParser.getValue(process, "__PURPOSE__") );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(ElementParameterParser.getValue(process, "__DESCRIPTION__") );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(ElementParameterParser.getValue(process, "__AUTHOR__") );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(version );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(ElementParameterParser.getValue(process, "__STATUS__") );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_20);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_21);
    if (isLog4jEnabled) {
    stringBuffer.append(TEXT_22);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_23);
    }
    stringBuffer.append(TEXT_24);
    //input/output format will be init on client and getSplit() will be called on client
    stringBuffer.append(TEXT_25);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_26);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_27);
    stringBuffer.append(jobFolderName);
    stringBuffer.append(TEXT_28);
    
            for(IContextParameter ctxParam : params){
                String cParaName = ctxParam.getName();
                
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_30);
    if(ctxParam.getType().equals("id_Date")){
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_33);
    stringBuffer.append(ctxParam.getValue() );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_46);
    }else{
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cParaName );
    stringBuffer.append(TEXT_49);
    }
    stringBuffer.append(TEXT_50);
     
            }
            
    stringBuffer.append(TEXT_51);
    
        for(IContextParameter ctxParam : params){
            if(ctxParam.getType().equals("id_List Of Value") || ctxParam.getType().equals("id_File") || ctxParam.getType().equals("id_Directory")){
            
    stringBuffer.append(TEXT_52);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_53);
    stringBuffer.append(Character.toUpperCase(ctxParam.getName().charAt(0)) + ctxParam.getName().substring(1));
    stringBuffer.append(TEXT_54);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_55);
    
            }else{
            
    stringBuffer.append(TEXT_56);
    stringBuffer.append(JavaTypesManager.getTypeToGenerate(ctxParam.getType(),true));
    stringBuffer.append(TEXT_57);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_58);
    stringBuffer.append(JavaTypesManager.getTypeToGenerate(ctxParam.getType(),true));
    stringBuffer.append(TEXT_59);
    stringBuffer.append(Character.toUpperCase(ctxParam.getName().charAt(0)) + ctxParam.getName().substring(1));
    stringBuffer.append(TEXT_60);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_61);
    
            }
        }
        
    stringBuffer.append(TEXT_62);
    
	        for(IContextParameter ctxParam : params){
	        	if (ctxParam.getType().equals("id_Password")) {
            	
    stringBuffer.append(TEXT_63);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_64);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_65);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_66);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_67);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_68);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_69);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_70);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_71);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_72);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_73);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_74);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_75);
    
            		continue;
            	}
	            String typeToGenerate ="String";
	            if( !(ctxParam.getType().equals("id_File") || ctxParam.getType().equals("id_Directory") ||ctxParam.getType().equals("id_List Of Value"))){
	               typeToGenerate=JavaTypesManager.getTypeToGenerate(ctxParam.getType(),true);
	            }
	            if(typeToGenerate.equals("java.util.Date")){
				
    stringBuffer.append(TEXT_76);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_77);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_78);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_79);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_80);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_81);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_82);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_83);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_84);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_85);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_86);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_87);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_88);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_89);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_90);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_91);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_92);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_93);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_94);
    
	            }else if(typeToGenerate.equals("Object")||typeToGenerate.equals("String")||typeToGenerate.equals("java.lang.String")){
	            
    stringBuffer.append(TEXT_95);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_96);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_98);
    
	            }else if(typeToGenerate.equals("Character")&&ctxParam.getName()!=null){
	         	
    stringBuffer.append(TEXT_99);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_100);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_101);
    
	            }else{
	         	
    stringBuffer.append(TEXT_102);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_103);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_104);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_105);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_106);
    
	            }
	        }
	        
    stringBuffer.append(TEXT_107);
    stringBuffer.append(process.getVersion() );
    stringBuffer.append(TEXT_108);
    stringBuffer.append(codeGenArgument.getJobName() );
    stringBuffer.append(TEXT_109);
    stringBuffer.append(codeGenArgument.getCurrentProjectName() );
    stringBuffer.append(TEXT_110);
    stringBuffer.append(TEXT_111);
    return stringBuffer.toString();
  }
}
