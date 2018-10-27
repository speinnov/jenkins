package org.talend.designer.codegen.translators.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.talend.core.CorePlugin;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IContextParameter;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IProcess;
import org.talend.core.model.process.ProcessUtils;
import org.talend.core.model.utils.JavaResourcesHelper;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.designer.common.tsetkeystore.TSetKeystoreUtil;
import org.talend.designer.runprocess.ProcessorException;
import org.talend.designer.runprocess.ProcessorUtilities;

public class Spark_footerJava
{
  protected static String nl;
  public static synchronized Spark_footerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Spark_footerJava result = new Spark_footerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 generated tHadoopConfManager in the process.\");";
  protected final String TEXT_2 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tGSConfiguration defined in the designer.\");";
  protected final String TEXT_3 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tS3Configuration defined in the designer.\");";
  protected final String TEXT_4 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tTachyonConfiguration defined in the designer.\");";
  protected final String TEXT_5 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tHDFSConfiguration defined in the designer.\");";
  protected final String TEXT_6 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tCassandraConfiguration defined in the designer.\");";
  protected final String TEXT_7 = NL + "        throw new java.lang.RuntimeException(\"A Spark job can't have more than 1 tBigQueryConfiguration defined in the designer.\");";
  protected final String TEXT_8 = NL;
  protected final String TEXT_9 = NL + "public static class TalendPipelineModel implements java.io.Serializable {" + NL + "" + NL + "\tprivate static final long serialVersionUID = 1L;" + NL + "\t" + NL + "\tprivate org.apache.spark.ml.PipelineModel pipelineModel;" + NL + "\tprivate java.util.Map<String, String> params;" + NL + "\t" + NL + "\tpublic TalendPipelineModel(org.apache.spark.ml.PipelineModel pipelineModel) {" + NL + "\t\tthis.pipelineModel = pipelineModel;" + NL + "\t\tthis.params = new java.util.HashMap<String, String>();" + NL + "\t}" + NL + "\t" + NL + "\tpublic TalendPipelineModel(org.apache.spark.ml.PipelineModel pipelineModel, java.util.Map<String, String> params) {" + NL + "\t\tthis.pipelineModel = pipelineModel;" + NL + "\t\tthis.params = params;" + NL + "\t}" + NL + "\t" + NL + "\tpublic org.apache.spark.ml.PipelineModel getPipelineModel() {" + NL + "\t\treturn this.pipelineModel;" + NL + "\t}" + NL + "\t" + NL + "\tpublic java.util.Map<String, String> getParams() {" + NL + "\t\treturn this.params;" + NL + "\t}" + NL + "}  ";
  protected final String TEXT_10 = NL + "public static class TalendPipeline implements java.io.Serializable {" + NL + "" + NL + "\tprivate static final long serialVersionUID = 1L;" + NL + "\t" + NL + "\tprivate org.apache.spark.ml.Pipeline pipeline;" + NL + "\tprivate java.util.Map<String, String> params;" + NL + "\t" + NL + "\tpublic TalendPipeline(org.apache.spark.ml.Pipeline pipeline) {" + NL + "\t\tthis.pipeline = pipeline;" + NL + "\t\tthis.params = new java.util.HashMap<String, String>();" + NL + "\t}" + NL + "\t" + NL + "\tpublic TalendPipeline(org.apache.spark.ml.Pipeline pipeline, java.util.Map<String, String> params) {" + NL + "\t\tthis.pipeline = pipeline;" + NL + "\t\tthis.params = params;" + NL + "\t}" + NL + "\t" + NL + "\tpublic org.apache.spark.ml.Pipeline getPipeline() {" + NL + "\t\treturn this.pipeline;" + NL + "\t}" + NL + "\t" + NL + "\tpublic java.util.Map<String, String> getParams() {" + NL + "\t\treturn this.params;" + NL + "\t}" + NL + "}  ";
  protected final String TEXT_11 = NL;
  protected final String TEXT_12 = NL + "public static class TalendKryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {" + NL + "\t\t" + NL + "\t@Override" + NL + "\tpublic void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {" + NL + "\t\ttry {" + NL + "\t\t\tkryo.register(Class.forName(\"org.talend.bigdata.dataflow.keys.JoinKeyRecord\"));" + NL + "\t\t} catch (java.lang.ClassNotFoundException e) {" + NL + "\t\t\t// Ignore" + NL + "\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_13 = "\t\t" + NL + "\t\ttry {" + NL + "\t\t\tkryo.register(" + NL + "\t\t\t\tClass.forName(\"org.apache.avro.generic.GenericData$Record\"), " + NL + "\t\t\t\tnew org.talend.bigdata.dataflow.serializer.KryoAvroRecordSerializer());" + NL + "\t\t} catch (java.lang.ClassNotFoundException e) {" + NL + "\t\t\t// Ignore" + NL + "\t\t}";
  protected final String TEXT_14 = NL + NL + "\t\tkryo.register(java.net.InetAddress.class, new InetAddressSerializer());" + NL + "\t\tkryo.addDefaultSerializer(java.net.InetAddress.class, new InetAddressSerializer());" + NL + "\t";
  protected final String TEXT_15 = NL + "\t\t\tkryo.register(TalendPipelineModel.class, new TalendPipelineModelSerializer());" + NL + "\t\t\tkryo.addDefaultSerializer(TalendPipelineModel.class, new TalendPipelineModelSerializer());" + NL + "\t\t\tkryo.register(TalendPipeline.class, new TalendPipelineSerializer());" + NL + "\t\t\tkryo.addDefaultSerializer(TalendPipeline.class, new TalendPipelineSerializer());" + NL + "\t";
  protected final String TEXT_16 = "\t\t\t" + NL + "\t\tkryo.register(";
  protected final String TEXT_17 = ".class);" + NL + "\t";
  protected final String TEXT_18 = NL + "\t}" + NL + "}" + NL + "\t" + NL + "public static class InetAddressSerializer extends com.esotericsoftware.kryo.Serializer<java.net.InetAddress> {" + NL + "" + NL + "\t@Override" + NL + "\tpublic void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output, java.net.InetAddress value) {" + NL + "\t\toutput.writeInt(value.getAddress().length);" + NL + "\t\toutput.writeBytes(value.getAddress());" + NL + "\t}" + NL + "" + NL + "\t@Override" + NL + "\tpublic java.net.InetAddress read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input, Class<java.net.InetAddress> paramClass) {" + NL + "\t\tjava.net.InetAddress inetAddress = null;" + NL + "\t\ttry {" + NL + "\t\t\tint length = input.readInt();" + NL + "\t\t\tbyte[] address = input.readBytes(length);" + NL + "\t\t\tinetAddress = java.net.InetAddress.getByAddress(address);" + NL + "\t\t} catch (java.net.UnknownHostException e) {" + NL + "\t\t\t// Cannot recreate InetAddress instance : return null" + NL + "\t\t} catch (com.esotericsoftware.kryo.KryoException e) {" + NL + "\t\t\t// Should not happen since write() and read() methods are consistent, but if it does happen, it is an unrecoverable error." + NL + "            throw new RuntimeException(e);" + NL + "\t\t}" + NL + "\t\treturn inetAddress;" + NL + "\t}" + NL + "}" + NL;
  protected final String TEXT_19 = NL + "public static class TalendPipelineModelSerializer extends com.esotericsoftware.kryo.Serializer<TalendPipelineModel> {" + NL + "" + NL + "\t@Override" + NL + "\tpublic void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output, TalendPipelineModel value) {" + NL + "\t\tkryo.writeObject(output, value.getParams(), new com.esotericsoftware.kryo.serializers.MapSerializer());" + NL + "\t\tkryo.writeObject(output, value.getPipelineModel(), new TalendJavaSerializer());" + NL + "\t}" + NL + "" + NL + "\t@Override" + NL + "\tpublic TalendPipelineModel read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input, Class<TalendPipelineModel> paramClass) {" + NL + "\t\tTalendPipelineModel talendPipelineModel = null;" + NL + "\t\ttry {" + NL + "\t\t\t@SuppressWarnings(\"unchecked\")" + NL + "\t\t\tjava.util.Map<String, String> params = kryo.readObject(input, java.util.HashMap.class, new com.esotericsoftware.kryo.serializers.MapSerializer());" + NL + "\t\t\torg.apache.spark.ml.PipelineModel pipelineModel = kryo.readObject(input, org.apache.spark.ml.PipelineModel.class, new TalendJavaSerializer());" + NL + "\t\t\ttalendPipelineModel = new TalendPipelineModel(pipelineModel, params);" + NL + "\t\t} catch (com.esotericsoftware.kryo.KryoException e) {" + NL + "\t\t\t// Should not happen since write() and read() methods are consistent, but if it does happen, it is an unrecoverable error." + NL + "            throw new RuntimeException(e);" + NL + "\t\t}" + NL + "\t\treturn talendPipelineModel;" + NL + "\t}" + NL + "}" + NL + "" + NL + "" + NL + "public static class TalendPipelineSerializer extends com.esotericsoftware.kryo.Serializer<TalendPipeline> {" + NL + "" + NL + "    @Override" + NL + "    public void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output, TalendPipeline value) {" + NL + "        kryo.writeObject(output, value.getParams(), new com.esotericsoftware.kryo.serializers.MapSerializer());" + NL + "        kryo.writeObject(output, value.getPipeline(), new TalendJavaSerializer());" + NL + "    }" + NL + "" + NL + "    @Override" + NL + "    public TalendPipeline read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input, Class<TalendPipeline> paramClass) {" + NL + "        TalendPipeline talendPipeline = null;" + NL + "        try {" + NL + "            @SuppressWarnings(\"unchecked\")" + NL + "            java.util.Map<String, String> params = kryo.readObject(input, java.util.HashMap.class, new com.esotericsoftware.kryo.serializers.MapSerializer());" + NL + "            org.apache.spark.ml.Pipeline pipeline = kryo.readObject(input, org.apache.spark.ml.Pipeline.class, new TalendJavaSerializer());" + NL + "            talendPipeline = new TalendPipeline(pipeline, params);" + NL + "        } catch (com.esotericsoftware.kryo.KryoException e) {" + NL + "\t\t\t// Should not happen since write() and read() methods are consistent, but if it does happen, it is an unrecoverable error." + NL + "            throw new RuntimeException(e);" + NL + "        }" + NL + "        return talendPipeline;" + NL + "    }" + NL + "}" + NL + "" + NL + "public static class TalendJavaSerializer extends com.esotericsoftware.kryo.Serializer{" + NL + "    " + NL + "    public void write (com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output, Object object) {" + NL + "        try {" + NL + "            com.esotericsoftware.kryo.util.ObjectMap graphContext = kryo.getGraphContext();" + NL + "            java.io.ObjectOutputStream objectStream = (java.io.ObjectOutputStream)graphContext.get(this);" + NL + "            if (objectStream == null) {" + NL + "                objectStream = new java.io.ObjectOutputStream(output);" + NL + "                graphContext.put(this, objectStream);" + NL + "            }" + NL + "            objectStream.writeObject(object);" + NL + "            objectStream.flush();" + NL + "        } catch (Exception ex) {" + NL + "            throw new com.esotericsoftware.kryo.KryoException(\"Error during Java serialization.\", ex);" + NL + "        }" + NL + "    }" + NL + "    " + NL + "    public Object read (com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input, Class type) {" + NL + "        try {" + NL + "            com.esotericsoftware.kryo.util.ObjectMap graphContext = kryo.getGraphContext();" + NL + "            ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);" + NL + "            if (objectStream == null) {" + NL + "                objectStream = new ObjectInputStreamWithCurrentThreadClassLoader(input);" + NL + "                graphContext.put(this, objectStream);" + NL + "            }" + NL + "            return objectStream.readObject();" + NL + "        } catch (Exception ex) {" + NL + "            throw new com.esotericsoftware.kryo.KryoException(\"Error during Java deserialization.\", ex);" + NL + "        }" + NL + "    }" + NL + "    " + NL + "    private static class ObjectInputStreamWithCurrentThreadClassLoader extends ObjectInputStream {" + NL + "        private final ClassLoader loader;" + NL + "" + NL + "        ObjectInputStreamWithCurrentThreadClassLoader(java.io.InputStream in) throws IOException {" + NL + "            super(in);" + NL + "            this.loader = Thread.currentThread().getContextClassLoader();" + NL + "        }" + NL + "" + NL + "        @Override" + NL + "        protected Class<?> resolveClass(java.io.ObjectStreamClass desc) {" + NL + "            try {" + NL + "                return Class.forName(desc.getName(), true, loader);" + NL + "            } catch (ClassNotFoundException e) {" + NL + "                throw new RuntimeException(\"Class not found: \" + desc.getName(), e);" + NL + "            }" + NL + "        }" + NL + "    }" + NL + "}// end class TalendJavaSerializer" + NL;
  protected final String TEXT_20 = NL + NL + "    public String resuming_logs_dir_path = null;" + NL + "    public String resuming_checkpoint_path = null;" + NL + "    public String parent_part_launcher = null;" + NL + "    public String pid = \"0\";" + NL + "    public String rootPid = null;" + NL + "    public String fatherPid = null;" + NL + "    public Integer portStats = null;" + NL + "    public String clientHost;" + NL + "    public String defaultClientHost = \"localhost\";" + NL + "    public String libjars = null;" + NL + "    private boolean execStat = true;" + NL + "    public boolean isChildJob = false;" + NL + "    public String fatherNode = null;" + NL + "    public String log4jLevel = \"\";" + NL + "    private boolean doInspect = false;";
  protected final String TEXT_21 = NL + "        private java.util.List<String> cloudApiArgs = new java.util.ArrayList<>();";
  protected final String TEXT_22 = NL + NL + "    public String contextStr = \"";
  protected final String TEXT_23 = "\";" + NL + "    public boolean isDefaultContext = true;" + NL + "" + NL + "    private java.util.Properties context_param = new java.util.Properties();" + NL + "    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();" + NL + "" + NL + "    public String status= \"\";" + NL + "" + NL + "    public static void main(String[] args) throws java.lang.RuntimeException {" + NL + "        int exitCode = new ";
  protected final String TEXT_24 = "().runJobInTOS(args);";
  protected final String TEXT_25 = NL + "            if(exitCode == 0) {" + NL + "                log.info(\"TalendJob: '";
  protected final String TEXT_26 = "' - Done.\");" + NL + "            } else {" + NL + "                log.error(\"TalendJob: '";
  protected final String TEXT_27 = "' - Failed with exit code: \" + exitCode + \".\");" + NL + "            }";
  protected final String TEXT_28 = NL + "        if(exitCode == 0) {";
  protected final String TEXT_29 = " if(!isCloudDriverCall(args)) { ";
  protected final String TEXT_30 = NL + "            System.exit(exitCode);";
  protected final String TEXT_31 = " } ";
  protected final String TEXT_32 = NL + "        } else {" + NL + "            throw new java.lang.RuntimeException(\"TalendJob: '";
  protected final String TEXT_33 = "' - Failed with exit code: \" + exitCode + \".\");" + NL + "        }" + NL + "    }" + NL;
  protected final String TEXT_34 = NL + "            @Test" + NL + "            public void test";
  protected final String TEXT_35 = "() throws java.lang.Exception{";
  protected final String TEXT_36 = NL + "                if(";
  protected final String TEXT_37 = "<=0){" + NL + "                    throw new java.lang.Exception(\"There is no tCollectAndCheck in your test case!\");" + NL + "                }" + NL + "                junitGlobalMap.put(\"tests.log\",new String());" + NL + "                junitGlobalMap.put(\"tests.nbFailure\",new Integer(0));" + NL + "                final ";
  protected final String TEXT_38 = " ";
  protected final String TEXT_39 = "Class = new ";
  protected final String TEXT_40 = "();" + NL + "                java.util.List<String> paraList_";
  protected final String TEXT_41 = " = new java.util.ArrayList<String>();" + NL + "                paraList_";
  protected final String TEXT_42 = ".add(\"--context=";
  protected final String TEXT_43 = "\");";
  protected final String TEXT_44 = NL + "                        paraList_";
  protected final String TEXT_45 = ".add(\"--context_param\");" + NL + "                        paraList_";
  protected final String TEXT_46 = ".add(\"";
  protected final String TEXT_47 = "=\" + ";
  protected final String TEXT_48 = ");";
  protected final String TEXT_49 = NL + "                String[] arrays = new String[paraList_";
  protected final String TEXT_50 = ".size()];" + NL + "                for(int i=0;i<paraList_";
  protected final String TEXT_51 = ".size();i++){" + NL + "                    arrays[i] = (String)paraList_";
  protected final String TEXT_52 = ".get(i);" + NL + "                }";
  protected final String TEXT_53 = NL + "            ";
  protected final String TEXT_54 = "Class.runJobInTOS(arrays);" + NL + "" + NL + "            String errors = (String)junitGlobalMap.get(\"tests.log\");" + NL + "            Integer nbFailure = (Integer)junitGlobalMap.get(\"tests.nbFailure\");" + NL + "            assertTrue(\"Failure=\"+nbFailure+java.lang.System.getProperty(\"line.separator\")+errors, errors.isEmpty());" + NL + "" + NL + "            }";
  protected final String TEXT_55 = NL + NL + "    public String[][] runJob(String[] args){" + NL + "        int exitCode = runJobInTOS(args);" + NL + "        String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };" + NL + "        return bufferValue;" + NL + "    }" + NL + "" + NL + "    public int runJobInTOS (String[] args) {" + NL + "        normalizeArgs(args);" + NL + "" + NL + "        String lastStr = \"\";" + NL + "        for (String arg : args) {" + NL + "            if (arg.equalsIgnoreCase(\"--context_param\")) {" + NL + "                lastStr = arg;" + NL + "            } else if (lastStr.equals(\"\")) {" + NL + "                evalParam(arg);" + NL + "            } else {" + NL + "                evalParam(lastStr + \" \" + arg);" + NL + "                lastStr = \"\";" + NL + "            }" + NL + "        }" + NL;
  protected final String TEXT_56 = NL + "            if(!\"\".equals(log4jLevel)){" + NL + "                if(\"trace\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.TRACE);" + NL + "                }else if(\"debug\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.DEBUG);" + NL + "                }else if(\"info\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.INFO);" + NL + "                }else if(\"warn\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.WARN);" + NL + "                }else if(\"error\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.ERROR);" + NL + "                }else if(\"fatal\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.FATAL);" + NL + "                }else if (\"off\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.OFF);" + NL + "                }" + NL + "                org.apache.log4j.Logger.getRootLogger().setLevel(log.getLevel());" + NL + "            }" + NL + "            log.info(\"TalendJob: '";
  protected final String TEXT_57 = "' - Start.\");";
  protected final String TEXT_58 = NL + NL + "        initContext();" + NL + "" + NL + "        if (clientHost == null) {" + NL + "            clientHost = defaultClientHost;" + NL + "        }" + NL + "" + NL + "        if (pid == null || \"0\".equals(pid)) {" + NL + "            pid = TalendString.getAsciiRandomString(6);" + NL + "        }" + NL + "" + NL + "        if (rootPid == null) {" + NL + "            rootPid = pid;" + NL + "        }" + NL + "        if (fatherPid == null) {" + NL + "            fatherPid = pid;" + NL + "        } else {" + NL + "            isChildJob = true;" + NL + "        }" + NL;
  protected final String TEXT_59 = NL + NL + "            if (portStats != null) {" + NL + "                // portStats = -1; //for testing" + NL + "                if (portStats < 0 || portStats > 65535) {" + NL + "                    // issue:10869, the portStats is invalid, so this client socket" + NL + "                    // can't open" + NL + "                    System.err.println(\"The statistics socket port \" + portStats" + NL + "                            + \" is invalid.\");" + NL + "                    execStat = false;" + NL + "                }" + NL + "            } else {" + NL + "                execStat = false;" + NL + "            }" + NL + "" + NL + "            if (execStat) {" + NL + "                try {" + NL + "                    runStat.startThreadStat(clientHost, portStats);" + NL + "                    runStat.setAllPID(rootPid, fatherPid, pid);" + NL + "                } catch (java.io.IOException ioException) {" + NL + "                    ioException.printStackTrace();" + NL + "                }" + NL + "            }";
  protected final String TEXT_60 = NL + "            if(!\"\".equals(";
  protected final String TEXT_61 = ")) {" + NL + "                System.setProperty(\"HADOOP_USER_NAME\", ";
  protected final String TEXT_62 = ");" + NL + "            }";
  protected final String TEXT_63 = NL + "        String osName = System.getProperty(\"os.name\");" + NL + "        String snappyLibName = \"libsnappyjava.so\";" + NL + "        if(osName.startsWith(\"Windows\")) {" + NL + "            snappyLibName = \"snappyjava.dll\";" + NL + "        } else if(osName.startsWith(\"Mac\")) {" + NL + "            snappyLibName = \"libsnappyjava.jnilib\";" + NL + "        }" + NL + "        System.setProperty(\"org.xerial.snappy.lib.name\", snappyLibName);";
  protected final String TEXT_64 = NL + "            try {";
  protected final String TEXT_65 = NL + "                    ";
  protected final String TEXT_66 = "Process();";
  protected final String TEXT_67 = NL + "                // Set job wide SSL settings before creating the Spark context" + NL + "                setupJobWideSSLConfigurations();" + NL + "                java.util.Map<String, String> tuningConf = new java.util.HashMap<>();" + NL + "                org.apache.spark.SparkConf sparkConfiguration = getConf(tuningConf);";
  protected final String TEXT_68 = NL + "                    return runSparkJobServerJob(sparkConfiguration, tuningConf);";
  protected final String TEXT_69 = NL + "                        if(!isCloudDriverCall(args)) {" + NL + "                            return runCloudJob(sparkConfiguration, cloudApiArgs);" + NL + "                        } else {";
  protected final String TEXT_70 = NL + "                                try {" + NL + "                                    org.apache.hadoop.conf.Configuration hadoopConfiguration = org.apache.spark.deploy.SparkHadoopUtil.get().newConfiguration(sparkConfiguration);" + NL + "                                    org.apache.hadoop.security.UserGroupInformation.setConfiguration(hadoopConfiguration);" + NL + "                                    org.apache.hadoop.security.UserGroupInformation.getLoginUser();" + NL + "                                } catch (IOException ioe) {";
  protected final String TEXT_71 = NL + "                                        log.warn(ioe.getMessage());";
  protected final String TEXT_72 = NL + "                                }";
  protected final String TEXT_73 = NL + "                            org.apache.spark.api.java.JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext(sparkConfiguration);" + NL + "                            return run(ctx);" + NL + "                        } // end of else part of if(!isCloudDriver(args))";
  protected final String TEXT_74 = NL + "                            try {" + NL + "                                org.apache.hadoop.conf.Configuration hadoopConfiguration = org.apache.spark.deploy.SparkHadoopUtil.get().newConfiguration(sparkConfiguration);" + NL + "                                org.apache.hadoop.security.UserGroupInformation.setConfiguration(hadoopConfiguration);" + NL + "                                org.apache.hadoop.security.UserGroupInformation.getLoginUser();" + NL + "                            } catch (IOException ioe) {";
  protected final String TEXT_75 = NL + "                                    log.warn(ioe.getMessage());";
  protected final String TEXT_76 = NL + "                            }";
  protected final String TEXT_77 = NL + "                        org.apache.spark.api.java.JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext(sparkConfiguration);" + NL + "                        return run(ctx);";
  protected final String TEXT_78 = NL + "            } catch(Exception ex) {" + NL + "                ex.printStackTrace();" + NL + "                return 1;" + NL + "            }";
  protected final String TEXT_79 = NL + "    }" + NL + NL;
  protected final String TEXT_80 = NL + "            final String clouderaManagerPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_81 = ");";
  protected final String TEXT_82 = NL + "            final String clouderaManagerPassword = ";
  protected final String TEXT_83 = ";";
  protected final String TEXT_84 = NL + "        this.lineageCreator = new org.talend.lineage.cloudera.LineageCreator(";
  protected final String TEXT_85 = NL + "                ";
  protected final String TEXT_86 = ",";
  protected final String TEXT_87 = NL + "                ";
  protected final String TEXT_88 = ",";
  protected final String TEXT_89 = NL + "                ";
  protected final String TEXT_90 = ",";
  protected final String TEXT_91 = NL + "                ";
  protected final String TEXT_92 = "," + NL + "                clouderaManagerPassword," + NL + "                jobName + \"_\" + jobVersion.replace(\".\", \"_\")," + NL + "                projectName,";
  protected final String TEXT_93 = NL + "                ";
  protected final String TEXT_94 = ",";
  protected final String TEXT_95 = NL + "                ";
  protected final String TEXT_96 = ",";
  protected final String TEXT_97 = NL + "                ";
  protected final String TEXT_98 = ");";
  protected final String TEXT_99 = NL + "            System.setProperty(\"atlas.conf\", ";
  protected final String TEXT_100 = ");";
  protected final String TEXT_101 = NL + "                String decryptedAtlasUserPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_102 = ");";
  protected final String TEXT_103 = NL + "                String decryptedAtlasUserPassword = ";
  protected final String TEXT_104 = ";";
  protected final String TEXT_105 = NL + "            this.lineageCreator = new org.talend.lineage.atlas.AtlasLineageCreator(";
  protected final String TEXT_106 = ", ";
  protected final String TEXT_107 = ", decryptedAtlasUserPassword);";
  protected final String TEXT_108 = NL + "            this.lineageCreator = new org.talend.lineage.atlas.AtlasLineageCreator(";
  protected final String TEXT_109 = ");";
  protected final String TEXT_110 = NL + "        java.util.Map<String, Object> lineageCreatorJobMetadata = new java.util.HashMap<String, Object>();" + NL + "        lineageCreatorJobMetadata.put(\"name\", jobName);" + NL + "        lineageCreatorJobMetadata.put(\"description\", jobName);" + NL + "        lineageCreatorJobMetadata.put(\"purpose\", \"Talend BigData Job\");" + NL + "        lineageCreatorJobMetadata.put(\"author\", System.getProperty(\"user.name\"));" + NL + "        lineageCreatorJobMetadata.put(\"version\", jobVersion);" + NL + "        lineageCreatorJobMetadata.put(\"jobType\", \"Talend BigData Job\");" + NL + "        lineageCreatorJobMetadata.put(\"framework\", \"Talend BigData\");" + NL + "        lineageCreatorJobMetadata.put(\"status\", \"FINISHED\");" + NL + "        lineageCreatorJobMetadata.put(\"creationDate\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"lastModificationDate\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"startTime\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"endTime\", System.currentTimeMillis());" + NL + "" + NL + "        this.lineageCreator.addJobInfo(lineageCreatorJobMetadata);";
  protected final String TEXT_111 = NL + "        java.util.Map<String, String> columns_";
  protected final String TEXT_112 = " = new java.util.HashMap<>();";
  protected final String TEXT_113 = NL + "            columns_";
  protected final String TEXT_114 = ".put(\"";
  protected final String TEXT_115 = "\", \"";
  protected final String TEXT_116 = "\");";
  protected final String TEXT_117 = NL + "        java.util.Map<String, String> columns_";
  protected final String TEXT_118 = " = new java.util.HashMap<>();";
  protected final String TEXT_119 = NL + "                        columns_";
  protected final String TEXT_120 = ".put(\"";
  protected final String TEXT_121 = "\", \"";
  protected final String TEXT_122 = "\");";
  protected final String TEXT_123 = NL + "                    columns_";
  protected final String TEXT_124 = ".put(\"";
  protected final String TEXT_125 = "\", \"";
  protected final String TEXT_126 = "\");";
  protected final String TEXT_127 = NL + "        lineageCreator.addDataset(columns_";
  protected final String TEXT_128 = ", \"";
  protected final String TEXT_129 = "\", ";
  protected final String TEXT_130 = ", \"";
  protected final String TEXT_131 = "\");";
  protected final String TEXT_132 = NL + "        java.util.List<String> inputNodes_";
  protected final String TEXT_133 = " = new java.util.ArrayList<String>();";
  protected final String TEXT_134 = NL + "                inputNodes_";
  protected final String TEXT_135 = ".add(\"";
  protected final String TEXT_136 = "\");";
  protected final String TEXT_137 = NL + "        java.util.List<String> outputNodes_";
  protected final String TEXT_138 = " = new java.util.ArrayList<String>();";
  protected final String TEXT_139 = NL + "                outputNodes_";
  protected final String TEXT_140 = ".add(\"";
  protected final String TEXT_141 = "\");";
  protected final String TEXT_142 = NL + "        this.lineageCreator.addNodeToLineage(\"";
  protected final String TEXT_143 = "\", columns_";
  protected final String TEXT_144 = ", inputNodes_";
  protected final String TEXT_145 = ", outputNodes_";
  protected final String TEXT_146 = ", new java.util.HashMap<String, Object>());";
  protected final String TEXT_147 = NL + "        org.talend.lineage.common.ILineageCreator lineageCreator;";
  protected final String TEXT_148 = NL + NL + "    /**" + NL + "     *" + NL + "     * This method runs the Spark job using the SparkContext in argument." + NL + "     * @param ctx, the SparkContext." + NL + "     * @return the Spark job exit code." + NL + "     *" + NL + "    */" + NL + "    private int run(org.apache.spark.api.java.JavaSparkContext ctx) throws java.lang.Exception {";
  protected final String TEXT_149 = NL + "            ctx.sc().setCheckpointDir(";
  protected final String TEXT_150 = ");";
  protected final String TEXT_151 = NL + "                System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_152 = ");" + NL + "                System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_153 = ");";
  protected final String TEXT_154 = NL + "                org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(";
  protected final String TEXT_155 = ", ";
  protected final String TEXT_156 = ");";
  protected final String TEXT_157 = NL + "                com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();" + NL + "                maprLogin.getMapRCredentialsViaKerberos(";
  protected final String TEXT_158 = ", ";
  protected final String TEXT_159 = ");";
  protected final String TEXT_160 = NL + "                System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_161 = ");" + NL + "                com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();";
  protected final String TEXT_162 = NL + "                    System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_163 = ");";
  protected final String TEXT_164 = NL + "                    maprLogin.setCheckUGI(false);";
  protected final String TEXT_165 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_166 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_167 = ");";
  protected final String TEXT_168 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_169 = " = ";
  protected final String TEXT_170 = "; ";
  protected final String TEXT_171 = NL;
  protected final String TEXT_172 = NL + "                    maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_173 = ", ";
  protected final String TEXT_174 = ", decryptedPassword_";
  protected final String TEXT_175 = ", ";
  protected final String TEXT_176 = ", \"\");";
  protected final String TEXT_177 = NL + "                    maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_178 = ", ";
  protected final String TEXT_179 = ", decryptedPassword_";
  protected final String TEXT_180 = ", ";
  protected final String TEXT_181 = ");";
  protected final String TEXT_182 = NL + "            ctx.sc().addSparkListener(new TalendRuntimeSparkListener(ctx.getConf()));";
  protected final String TEXT_183 = NL + "            ctx.sc().addSparkListener(new TalendEndOfRunSparkListener(jobName));";
  protected final String TEXT_184 = NL + "        ctx.setJobGroup(projectName + \"_\" + jobName + \"_\" + Thread.currentThread().getId(), \"\");" + NL + "" + NL + "        initContext();" + NL + "" + NL + "        setContext(ctx.hadoopConfiguration(), ctx);" + NL + "" + NL + "        if (doInspect) {" + NL + "            System.out.println(\"== inspect start ==\");" + NL + "            System.out.println(\"{\");" + NL + "            System.out.println(\"  \\\"SPARK_MASTER\\\": \\\"\" + ctx.getConf().get(\"spark.master\") + \"\\\",\");" + NL + "            System.out.println(\"  \\\"SPARK_UI_PORT\\\": \\\"\" + ctx.getConf().get(\"spark.ui.port\", \"4040\") + \"\\\",\");" + NL + "            System.out.println(\"  \\\"JOB_NAME\\\": \\\"\" + ctx.getConf().get(\"spark.app.name\", jobName) + \"\\\"\");" + NL + "            System.out.println(\"}\"); //$NON-NLS-1$" + NL + "            System.out.println(\"== inspect end ==\");" + NL + "        }";
  protected final String TEXT_185 = NL;
  protected final String TEXT_186 = NL + "                if(log.is";
  protected final String TEXT_187 = "Enabled())";
  protected final String TEXT_188 = NL + "            log.";
  protected final String TEXT_189 = "(\"";
  protected final String TEXT_190 = " - \" ";
  protected final String TEXT_191 = " + (";
  protected final String TEXT_192 = ") ";
  protected final String TEXT_193 = ");";
  protected final String TEXT_194 = NL + "    \tclass BytesLimit65535_";
  protected final String TEXT_195 = "{" + NL + "    \t\tpublic void limitLog4jByte() throws Exception{" + NL + "    \t\t\t";
  protected final String TEXT_196 = NL + "            StringBuilder ";
  protected final String TEXT_197 = " = new StringBuilder();";
  protected final String TEXT_198 = NL + "            ";
  protected final String TEXT_199 = ".append(\"Parameters:\");";
  protected final String TEXT_200 = NL + "                    ";
  protected final String TEXT_201 = ".append(\"";
  protected final String TEXT_202 = "\" + \" = \" + String.valueOf(";
  protected final String TEXT_203 = ").substring(0, 4) + \"...\");     ";
  protected final String TEXT_204 = NL + "                    ";
  protected final String TEXT_205 = ".append(\"";
  protected final String TEXT_206 = "\" + \" = \" + ";
  protected final String TEXT_207 = ");";
  protected final String TEXT_208 = NL + "                ";
  protected final String TEXT_209 = ".append(\" | \");";
  protected final String TEXT_210 = NL + "    \t\t}" + NL + "    \t}" + NL + "    \t" + NL + "        new BytesLimit65535_";
  protected final String TEXT_211 = "().limitLog4jByte();";
  protected final String TEXT_212 = NL + "            StringBuilder ";
  protected final String TEXT_213 = " = new StringBuilder();    ";
  protected final String TEXT_214 = NL + "                    ";
  protected final String TEXT_215 = ".append(";
  protected final String TEXT_216 = ".";
  protected final String TEXT_217 = ");";
  protected final String TEXT_218 = NL + "                    if(";
  protected final String TEXT_219 = ".";
  protected final String TEXT_220 = " == null){";
  protected final String TEXT_221 = NL + "                        ";
  protected final String TEXT_222 = ".append(\"<null>\");" + NL + "                    }else{";
  protected final String TEXT_223 = NL + "                        ";
  protected final String TEXT_224 = ".append(";
  protected final String TEXT_225 = ".";
  protected final String TEXT_226 = ");" + NL + "                    }   ";
  protected final String TEXT_227 = NL + "                ";
  protected final String TEXT_228 = ".append(\"|\");";
  protected final String TEXT_229 = NL + "                    log.info(\"A <";
  protected final String TEXT_230 = "> configuration component has been found.\");";
  protected final String TEXT_231 = NL + "                try {" + NL + "                    globalMap = new GlobalVar(ctx.hadoopConfiguration());";
  protected final String TEXT_232 = NL + "                    ";
  protected final String TEXT_233 = "Process(ctx, globalMap);" + NL + "                } catch(Exception e) {" + NL + "                    e.printStackTrace();" + NL + "                    e.printStackTrace(errorMessagePS);" + NL + "                    throw e;" + NL + "                } finally {" + NL + "                    ctx.cancelJobGroup(projectName + \"_\" + jobName + \"_\" + Thread.currentThread().getId());" + NL + "                    ctx.stop();";
  protected final String TEXT_234 = NL + "                        if (execStat) {" + NL + "                            runStat.stopThreadStat();" + NL + "                        }";
  protected final String TEXT_235 = NL + "                }" + NL;
  protected final String TEXT_236 = NL + "                    if ((junitGlobalMap.get(\"tests.nbFailure\") != null)" + NL + "                            && (((Integer)junitGlobalMap.get(\"tests.nbFailure\")) > 0)) {" + NL + "                        Integer failedTest = (Integer) junitGlobalMap.get(\"tests.nbFailure\");" + NL + "                        String logs = (String) junitGlobalMap.get(\"tests.log\");" + NL + "                        if (failedTest == 1) {" + NL + "                            throw new RuntimeException(\"1 test failed:\" + logs);" + NL + "                        } else {" + NL + "                            throw new RuntimeException(failedTest + \" tests failed:\" + logs);" + NL + "                        }" + NL + "                    }";
  protected final String TEXT_237 = NL + "            lineageCreator.sendToLineageProvider(";
  protected final String TEXT_238 = ");";
  protected final String TEXT_239 = NL + "        return 0;" + NL + "    }" + NL + "" + NL + "    /**" + NL + "     *" + NL + "     * This method has the responsibility to return a Spark configuration for the Spark job to run." + NL + "     * @return a Spark configuration." + NL + "     *" + NL + "     */" + NL + "    private org.apache.spark.SparkConf getConf(java.util.Map<String, String> tuningConf) throws java.lang.Exception {" + NL + "        org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();" + NL + "" + NL + "        sparkConfiguration.setAppName(projectName + \"_\" + jobName + \"_\" + jobVersion);" + NL + "        sparkConfiguration.set(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\");" + NL + "        sparkConfiguration.set(\"spark.kryo.registrator\", TalendKryoRegistrator.class.getName());";
  protected final String TEXT_240 = NL + "            sparkConfiguration.setMaster(";
  protected final String TEXT_241 = ");" + NL + "" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "            java.util.List<String> listJar = new java.util.ArrayList<String>();" + NL + "            String libJarUriPrefix = System.getProperty(\"os.name\").startsWith(\"Windows\") ? \"file:///\" : \"file://\";" + NL + "            if(libjars != null) {" + NL + "                for(String jar:libjars.split(\",\")) {";
  protected final String TEXT_242 = NL + "                        if(!jar.contains(\"";
  protected final String TEXT_243 = "\")) {";
  protected final String TEXT_244 = NL + "                            listJar.add(getJarsToRegister.replaceJarPaths(jar, libJarUriPrefix));";
  protected final String TEXT_245 = NL + "                        }";
  protected final String TEXT_246 = NL + "                }" + NL + "            }" + NL;
  protected final String TEXT_247 = NL + "                sparkConfiguration.set(\"spark.submit.deployMode\", ";
  protected final String TEXT_248 = ");" + NL + "                if (listJar != null) {" + NL + "                    String jarsLine = \"\";" + NL + "" + NL + "                    for (String jarPath: listJar)" + NL + "                      jarsLine=jarsLine+','+jarPath;" + NL + "" + NL + "                    jarsLine = (jarsLine.length()>0) ? jarsLine.substring(1): jarsLine;";
  protected final String TEXT_249 = NL + "                                log.info(\"Got \" + jarsLine.split(\",\").length + \" Spark libjars\");" + NL + "                                log.info(\"Spark libjars : \" + jarsLine);";
  protected final String TEXT_250 = NL + "                    sparkConfiguration.set(\"spark.yarn.jars\", jarsLine);" + NL + "                }";
  protected final String TEXT_251 = NL;
  protected final String TEXT_252 = NL + "                        routines.system.BigDataUtil.installWinutils(";
  protected final String TEXT_253 = ", getJarsToRegister.replaceJarPaths(\"";
  protected final String TEXT_254 = "\" + \"winutils-hadoop-2.6.0.exe\"));";
  protected final String TEXT_255 = NL + "                    String spark2JarsPaths = \"";
  protected final String TEXT_256 = "\";";
  protected final String TEXT_257 = NL + "                            log.info(\"Got \" + spark2JarsPaths.split(\",\").length + \" Spark jars\");" + NL + "                            log.info(\"Spark jars local paths = \" + spark2JarsPaths);";
  protected final String TEXT_258 = NL + "                    java.util.List<String> spark2JarsPathsList = java.util.Arrays.asList(spark2JarsPaths.split(\",\"));" + NL + "                    java.util.List<String> runtimeSpark2JarsPathsList = new java.util.ArrayList<String>();" + NL + "                    for(String sparkJarpath : spark2JarsPathsList){" + NL + "                        runtimeSpark2JarsPathsList.add(getJarsToRegister.replaceJarPaths(sparkJarpath, libJarUriPrefix));" + NL + "                    }";
  protected final String TEXT_259 = NL + "                            log.info(\"Runtime Spark jars = \" + runtimeSpark2JarsPathsList.toString());" + NL + "                            log.info(\"Got \" + runtimeSpark2JarsPathsList.size() + \" Runtime Spark jars\");";
  protected final String TEXT_260 = NL + "                    String allRuntimeSparkJarsPaths = org.apache.commons.lang3.StringUtils.join(runtimeSpark2JarsPathsList, \",\");" + NL + "                    if (sparkConfiguration.get(\"spark.yarn.jars\") == null){" + NL + "                        sparkConfiguration.set(\"spark.yarn.jars\", allRuntimeSparkJarsPaths);" + NL + "                    }else{" + NL + "                        sparkConfiguration.set(\"spark.yarn.jars\", allRuntimeSparkJarsPaths + \",\" + sparkConfiguration.get(\"spark.yarn.jars\"));" + NL + "                    }";
  protected final String TEXT_261 = NL + "            sparkConfiguration.setJars(listJar.toArray(new String[listJar.size()]));";
  protected final String TEXT_262 = NL + "                   sparkConfiguration.set(\"spark.driver.host\", ";
  protected final String TEXT_263 = ");" + NL + "                   org.apache.spark.util.Utils.setCustomHostname(";
  protected final String TEXT_264 = ");";
  protected final String TEXT_265 = NL + "                sparkConfiguration.setSparkHome(";
  protected final String TEXT_266 = ");";
  protected final String TEXT_267 = NL + NL + "\t\t\t    sparkConfiguration.set(\"spark.hadoop.yarn.application.classpath\", \"";
  protected final String TEXT_268 = "\");" + NL + "                sparkConfiguration.set(\"spark.hadoop.yarn.resourcemanager.address\", ";
  protected final String TEXT_269 = ");";
  protected final String TEXT_270 = "sparkConfiguration.set(\"spark.hadoop.yarn.resourcemanager.scheduler.address\", ";
  protected final String TEXT_271 = ");";
  protected final String TEXT_272 = "sparkConfiguration.set(\"spark.hadoop.mapreduce.jobhistory.address\", ";
  protected final String TEXT_273 = ");";
  protected final String TEXT_274 = "sparkConfiguration.set(\"spark.hadoop.yarn.app.mapreduce.am.staging-dir\", ";
  protected final String TEXT_275 = ");";
  protected final String TEXT_276 = NL + "                    sparkConfiguration.set(\"spark.hadoop.mapreduce.map.memory.mb\", ";
  protected final String TEXT_277 = ");" + NL + "                    sparkConfiguration.set(\"spark.hadoop.mapreduce.reduce.memory.mb\", ";
  protected final String TEXT_278 = ");" + NL + "                    sparkConfiguration.set(\"spark.hadoop.yarn.app.mapreduce.am.resource.mb\", ";
  protected final String TEXT_279 = ");";
  protected final String TEXT_280 = NL + "                    sparkConfiguration.set(\"spark.hadoop.yarn.resourcemanager.principal\", ";
  protected final String TEXT_281 = ");" + NL + "                    sparkConfiguration.set(\"spark.hadoop.mapreduce.jobhistory.principal\", ";
  protected final String TEXT_282 = ");";
  protected final String TEXT_283 = NL + "                        System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                        System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                        System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_284 = ");" + NL + "                        System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_285 = ");";
  protected final String TEXT_286 = NL + "                        org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(";
  protected final String TEXT_287 = ", ";
  protected final String TEXT_288 = ");";
  protected final String TEXT_289 = NL + "                        com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();" + NL + "                        maprLogin.getMapRCredentialsViaKerberos(";
  protected final String TEXT_290 = ", ";
  protected final String TEXT_291 = ");";
  protected final String TEXT_292 = NL + "                        System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                        System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                        System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_293 = ");" + NL + "                        com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();";
  protected final String TEXT_294 = NL + "                            System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_295 = ");";
  protected final String TEXT_296 = NL + "                            maprLogin.setCheckUGI(false);";
  protected final String TEXT_297 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_298 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_299 = ");";
  protected final String TEXT_300 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_301 = " = ";
  protected final String TEXT_302 = "; ";
  protected final String TEXT_303 = NL + "                            maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_304 = ", ";
  protected final String TEXT_305 = ", decryptedPassword_";
  protected final String TEXT_306 = ", ";
  protected final String TEXT_307 = ", \"\");";
  protected final String TEXT_308 = NL + "                            maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_309 = ", ";
  protected final String TEXT_310 = ", decryptedPassword_";
  protected final String TEXT_311 = ", ";
  protected final String TEXT_312 = ");";
  protected final String TEXT_313 = NL + "                            if(!";
  protected final String TEXT_314 = ".equals(";
  protected final String TEXT_315 = ")) {" + NL + "                                throw new RuntimeException(\"The HDFS and the Spark configurations must have the same user name.\");" + NL + "                            }";
  protected final String TEXT_316 = NL + "                            if(!\"\".equals(";
  protected final String TEXT_317 = ")) {" + NL + "                                System.setProperty(\"HADOOP_USER_NAME\", ";
  protected final String TEXT_318 = ");" + NL + "                            }";
  protected final String TEXT_319 = NL + "            System.setProperty(\"hadoop.home.dir\", ";
  protected final String TEXT_320 = ");";
  protected final String TEXT_321 = NL + "                tuningConf.put(\"spark.ui.port\", ";
  protected final String TEXT_322 = ");";
  protected final String TEXT_323 = NL + "            tuningConf.put(\"spark.executor.memory\", ";
  protected final String TEXT_324 = ");";
  protected final String TEXT_325 = NL + "                tuningConf.put(\"spark.yarn.executor.memoryOverhead\", ";
  protected final String TEXT_326 = ");";
  protected final String TEXT_327 = NL + "                tuningConf.put(\"spark.driver.cores\", ";
  protected final String TEXT_328 = ");" + NL + "                tuningConf.put(\"spark.driver.memory\", ";
  protected final String TEXT_329 = ");";
  protected final String TEXT_330 = NL + "\t\t\t\t\ttuningConf.put(\"spark.yarn.am.cores\", ";
  protected final String TEXT_331 = ");" + NL + "                \ttuningConf.put(\"spark.yarn.am.memory\", ";
  protected final String TEXT_332 = ");";
  protected final String TEXT_333 = NL + "                tuningConf.put(\"spark.executor.cores\", ";
  protected final String TEXT_334 = ");";
  protected final String TEXT_335 = NL + "                    tuningConf.put(\"spark.executor.instances\", ";
  protected final String TEXT_336 = ");";
  protected final String TEXT_337 = NL + "                    tuningConf.put(\"spark.dynamicAllocation.enabled\", \"true\");" + NL + "                    tuningConf.put(\"spark.shuffle.service.enabled\", \"true\");" + NL + "                    String dynInitialValue = ";
  protected final String TEXT_338 = ";" + NL + "                    tuningConf.put(\"spark.dynamicAllocation.initialExecutors\", dynInitialValue);" + NL + "                    String dynMinValue = ";
  protected final String TEXT_339 = ";" + NL + "                    tuningConf.put(\"spark.dynamicAllocation.minExecutors\", dynMinValue);";
  protected final String TEXT_340 = NL + "                        Integer iDynMaxValue = Integer.MAX_VALUE;" + NL + "                        tuningConf.put(\"spark.dynamicAllocation.maxExecutors\", new Integer(Integer.MAX_VALUE).toString());";
  protected final String TEXT_341 = NL + "                        Integer iDynMaxValue = Integer.parseInt(";
  protected final String TEXT_342 = ");" + NL + "                        tuningConf.put(\"spark.dynamicAllocation.maxExecutors\", ";
  protected final String TEXT_343 = ");";
  protected final String TEXT_344 = NL + "                    Integer iDynInitialValue = Integer.parseInt(dynInitialValue);" + NL + "                    Integer iDynMinValue = Integer.parseInt(dynMinValue);" + NL + "                    if(iDynInitialValue < iDynMinValue|| iDynInitialValue > iDynMaxValue || iDynMinValue > iDynMaxValue) {" + NL + "                        throw new RuntimeException(\"Please check your dynamicAllocation bounds, you should have min <= initial <= max\");" + NL + "                    }";
  protected final String TEXT_345 = NL + "                    tuningConf.put(\"spark.broadcast.factory\", \"org.apache.spark.broadcast.TorrentBroadcastFactory\");";
  protected final String TEXT_346 = NL + "                    tuningConf.put(\"spark.broadcast.factory\", \"org.apache.spark.broadcast.HttpBroadcastFactory\");";
  protected final String TEXT_347 = NL + "                tuningConf.put(\"spark.serializer\", ";
  protected final String TEXT_348 = ");";
  protected final String TEXT_349 = NL + "            tuningConf.put(\"spark.eventLog.enabled\",\"true\");";
  protected final String TEXT_350 = NL + "                tuningConf.put(\"spark.eventLog.compress\",\"true\");";
  protected final String TEXT_351 = NL + "            tuningConf.put(\"spark.eventLog.dir\",";
  protected final String TEXT_352 = ");" + NL + "            tuningConf.put(\"spark.yarn.historyServer.address\",";
  protected final String TEXT_353 = ");";
  protected final String TEXT_354 = NL + "            sparkConfiguration.set(\"spark.sql.warehouse.dir\", \"file:///\" +  ";
  protected final String TEXT_355 = " + \"/spark-warehouse\");";
  protected final String TEXT_356 = NL + "            tuningConf.put(";
  protected final String TEXT_357 = ", ";
  protected final String TEXT_358 = ");";
  protected final String TEXT_359 = NL + "            String driverExtraJavaOpts = tuningConf.get(\"spark.driver.extraJavaOptions\") == null ? \"\" : tuningConf.get(\"spark.driver.extraJavaOptions\");" + NL + "            String appMasterExtraJavaOpts = tuningConf.get(\"spark.yarn.am.extraJavaOptions\") == null ? \"\" : tuningConf.get(\"spark.yarn.am.extraJavaOptions\");" + NL + "            tuningConf.put(\"spark.driver.extraJavaOptions\", driverExtraJavaOpts + \" -Dhdp.version=\" + ";
  protected final String TEXT_360 = ");" + NL + "            tuningConf.put(\"spark.yarn.am.extraJavaOptions\", appMasterExtraJavaOpts + \" -Dhdp.version=\" + ";
  protected final String TEXT_361 = ");";
  protected final String TEXT_362 = NL + "        sparkConfiguration.setAll(scala.collection.JavaConversions.mapAsScalaMap(tuningConf));";
  protected final String TEXT_363 = NL + "            sparkConfiguration.set(\"spark.local.dir\", ";
  protected final String TEXT_364 = ");";
  protected final String TEXT_365 = NL + NL + "        return sparkConfiguration;" + NL + "    }";
  protected final String TEXT_366 = NL + "        /**" + NL + "         *" + NL + "         * This method uses the Spark JobServer REST API to submit a Spark job." + NL + "         * @param conf the Spark configuration of the job to execute." + NL + "         * @return the result of the job." + NL + "         *" + NL + "         */" + NL + "        public int runSparkJobServerJob(org.apache.spark.SparkConf conf, java.util.Map<String, String> tuningConf) throws Exception {";
  protected final String TEXT_367 = NL + "                final String hdInsightPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_368 = ");";
  protected final String TEXT_369 = NL + "                final String hdInsightPassword = ";
  protected final String TEXT_370 = ";";
  protected final String TEXT_371 = NL + "            String jobJar = \"\";" + NL + "            String[] jars = libjars.toString().split(\",\");" + NL + "            for(int i=0; i<jars.length; i++) {" + NL + "                String jar = jars[i];" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            java.util.Map<String, String> confMap = new java.util.HashMap<>();" + NL + "            for(scala.Tuple2 element: java.util.Arrays.asList(conf.getAll())) {" + NL + "                confMap.put((String) element._1, (String) element._2);" + NL + "            }" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "            org.talend.bigdata.launcher.jobserver.JobServerJob instance = new org.talend.bigdata.launcher.jobserver.SparkBatchJob.Builder()" + NL + "                .withEndpoint(";
  protected final String TEXT_372 = ")" + NL + "                .withCredentials(new org.talend.bigdata.launcher.security.HDInsightCredentials(";
  protected final String TEXT_373 = ", hdInsightPassword))" + NL + "                .withJarToExecute(jobJar)" + NL + "                .withClassToExecute(\"";
  protected final String TEXT_374 = ".";
  protected final String TEXT_375 = ".";
  protected final String TEXT_376 = "\")" + NL + "                .withAppName(projectName + \"_\" + jobName + \"_\" + jobVersion + \"_\" + pid)" + NL + "                .withConf(confMap)" + NL + "                .withTuningConf(tuningConf)" + NL + "                .build();" + NL + "" + NL + "            String jobResult = instance.executeJob();" + NL + "            if(\"ERROR\".equals(jobResult)) {" + NL + "                throw instance.getException();" + NL + "            } else if(\"OK\".equals(jobResult)) {" + NL + "                return instance.getExitCode();" + NL + "            }" + NL + "            return 1;" + NL + "        }" + NL + "" + NL + "        @Override" + NL + "        public Object runJob(Object sparkContext, com.typesafe.config.Config conf) {" + NL + "            org.apache.spark.api.java.JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext((org.apache.spark.SparkContext) sparkContext);" + NL + "            int returnCode = 1;" + NL + "            try {" + NL + "                returnCode = run(ctx);" + NL + "            } catch (java.lang.Exception e) {" + NL + "                Thread.currentThread().stop(e);" + NL + "            }" + NL + "            return returnCode;" + NL + "        }";
  protected final String TEXT_377 = NL + "        public int runCloudJob(org.apache.spark.SparkConf sparkConfiguration, java.util.List livyJobArgs) throws Exception {" + NL + "            initContext();" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();";
  protected final String TEXT_378 = NL + "                final String hdInsightPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_379 = ");";
  protected final String TEXT_380 = NL + "                final String hdInsightPassword = ";
  protected final String TEXT_381 = ";";
  protected final String TEXT_382 = NL + "                final String wasbPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_383 = ");";
  protected final String TEXT_384 = NL + "                final String wasbPassword = ";
  protected final String TEXT_385 = ";";
  protected final String TEXT_386 = NL + NL + "            String jobJar = \"\";" + NL + "            String[] jars = libjars.toString().split(\",\");" + NL + "            for(int i=0; i<jars.length; i++) {" + NL + "                String jar = jars[i];" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            java.util.Map<String, String> conf = new java.util.HashMap<>();" + NL + "            for(scala.Tuple2 element: java.util.Arrays.asList(sparkConfiguration.getAll())) {" + NL + "                conf.put((String) element._1, (String) element._2);" + NL + "            }" + NL + "" + NL + "            org.talend.bigdata.launcher.fs.FileSystem azureFs = new org.talend.bigdata.launcher.fs.AzureFileSystem(\"DefaultEndpointsProtocol=https;\"" + NL + "                + \"AccountName=\"" + NL + "                + ";
  protected final String TEXT_387 = NL + "                + \";\"" + NL + "                + \"AccountKey=\" + wasbPassword, ";
  protected final String TEXT_388 = ");" + NL + "" + NL + "            org.talend.bigdata.launcher.livy.LivyJob instance = new org.talend.bigdata.launcher.livy.SparkJob.Builder()" + NL + "                .withFileSystem(azureFs)" + NL + "                .withCredentials(new org.talend.bigdata.launcher.security.HDInsightCredentials(";
  protected final String TEXT_389 = ", hdInsightPassword))" + NL + "                .withJarToExecute(jobJar)" + NL + "                .withClassToExecute(\"";
  protected final String TEXT_390 = ".";
  protected final String TEXT_391 = ".";
  protected final String TEXT_392 = "\")" + NL + "                .withLivyEndpoint(\"https://\" + ";
  protected final String TEXT_393 = " + \":\" + ";
  protected final String TEXT_394 = ")" + NL + "                .withRemoteFolder(org.talend.bigdata.launcher.utils.Utils.removeFirstSlash(";
  protected final String TEXT_395 = "))" + NL + "                .withUsername(";
  protected final String TEXT_396 = ")" + NL + "                .withLibJars(libjars.toString())" + NL + "                .withConf(conf)" + NL + "                .withArgs(livyJobArgs)" + NL + "                .withAppName(projectName + \"_\" + jobName + \"_\" + jobVersion)";
  protected final String TEXT_397 = NL + "                    .withExecutorMemory(conf.get(\"spark.executor.memory\"))" + NL + "                    .withDriverMemory(conf.get(\"spark.driver.memory\"))";
  protected final String TEXT_398 = NL + "                        .withExecutorCore(conf.get(\"spark.executor.cores\"))";
  protected final String TEXT_399 = NL + "                    .withDriverCore(conf.get(\"spark.driver.cores\"))";
  protected final String TEXT_400 = NL + "                .build();" + NL + "" + NL + "            int returnCode = instance.executeJob();" + NL + "            System.out.println(instance.getJobLog());" + NL + "            return returnCode;" + NL + "        }";
  protected final String TEXT_401 = NL + "        public int runCloudJob(org.apache.spark.SparkConf sparkConfiguration, java.util.List dataprocJobArgs) throws Exception {" + NL + "            initContext();" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "" + NL + "            String jobJar = \"\";" + NL + "            for (String jar: libjars.toString().split(\",\")) {" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                    break;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            java.util.Map<String, String> conf = new java.util.HashMap<>();" + NL + "            for(scala.Tuple2 element: java.util.Arrays.asList(sparkConfiguration.getAll())) {" + NL + "                conf.put((String) element._1, (String) element._2);" + NL + "            }" + NL + "" + NL + "            org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob instance =" + NL + "                    new org.talend.bigdata.launcher.google.dataproc.DataprocSparkJob.Builder()" + NL + "                .withTalendJobName(projectName + \"_\" + jobName + \"_\" + jobVersion.replace(\".\",\"_\") + \"_\" + pid)" + NL + "                .withClusterName(";
  protected final String TEXT_402 = ")" + NL + "                .withRegion(";
  protected final String TEXT_403 = ")" + NL + "                .withProjectId(";
  protected final String TEXT_404 = ")";
  protected final String TEXT_405 = NL + "\t\t\t\t\t\t\t.withServiceAccountCredentialsPath(";
  protected final String TEXT_406 = ")";
  protected final String TEXT_407 = NL + NL + "                .withJarsBucket(";
  protected final String TEXT_408 = ")" + NL + "                .withJarToExecute(jobJar)" + NL + "                .withMainClass(\"";
  protected final String TEXT_409 = ".";
  protected final String TEXT_410 = ".";
  protected final String TEXT_411 = "\")" + NL + "                .withLibJars(libjars)" + NL + "" + NL + "                .withConf(conf)" + NL + "                .withArgs(dataprocJobArgs)" + NL + "" + NL + "" + NL + "        \t";
  protected final String TEXT_412 = NL + "\t        \t.withLogLevel(\"";
  protected final String TEXT_413 = "\")" + NL + "        \t";
  protected final String TEXT_414 = NL + "                .build();" + NL + "" + NL + "\t\t\t\t// Add JVM shutdown hook to send a cancel job request to the cluster" + NL + "\t\t\t\tRuntime.getRuntime().addShutdownHook(new DataprocShutdownHook(instance));" + NL + "\t\t\t\t// Submit the actual job" + NL + "\t\t\t\tint returnCode = instance.executeJob();" + NL + "\t\t\t\tSystem.out.println(instance.getJobLog());" + NL + "\t\t\t\treturn returnCode;" + NL + "        }" + NL + "" + NL + "        class DataprocShutdownHook extends Thread {" + NL + "        \t\tprivate org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob job;" + NL + "" + NL + "        \t\tpublic DataprocShutdownHook(org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob job) {" + NL + "        \t\t\tthis.job = job;" + NL + "        \t\t}" + NL + "" + NL + "        \t\t@Override" + NL + "        \t\tpublic void run() {" + NL + "\t\t\t\t\t";
  protected final String TEXT_415 = " log.info(\"Calling Dataproc Shutdown Hook\"); ";
  protected final String TEXT_416 = NL + "        \t\t   try {" + NL + "        \t\t   \t// A cancel request will be actually sent to the cluster only if the job is still ongoing" + NL + "        \t\t\t\tjob.cancelJob();" + NL + "        \t\t\t} catch (Exception e) {" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_417 = " log.error(\"Could not send a job cancel request to Dataproc : \"+e.getMessage()); ";
  protected final String TEXT_418 = NL + "        \t\t\t}" + NL + "        \t\t}" + NL + "        }";
  protected final String TEXT_419 = NL + "        public int runCloudJob(org.apache.spark.SparkConf sparkConfiguration, java.util.List altusJobArgs) throws Exception {" + NL + "            initContext();" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "" + NL + "            String jobJar = \"\";" + NL + "            for (String jar: libjars.toString().split(\",\")) {" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                    break;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            java.util.Map<String, String> conf = new java.util.HashMap<>();" + NL + "            for(scala.Tuple2 element: java.util.Arrays.asList(sparkConfiguration.getAll())) {" + NL + "                conf.put((String) element._1, (String) element._2);" + NL + "            }" + NL + "" + NL + "\t\t\t";
  protected final String TEXT_420 = NL + "                final String decryptedPassword_";
  protected final String TEXT_421 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_422 = ");";
  protected final String TEXT_423 = NL + "                final String decryptedPassword_";
  protected final String TEXT_424 = " = ";
  protected final String TEXT_425 = ";";
  protected final String TEXT_426 = NL + NL + "            java.text.DateFormat dateStrFormat = new java.text.SimpleDateFormat(\"yyyyMMddHHmmss\");" + NL + "\t\t\t";
  protected final String TEXT_427 = NL + "                org.talend.bigdata.launcher.altus.AltusJob instance = new org.talend.bigdata.launcher.altus.AltusSparkJob.Builder()" + NL + "                    .withTalendJobName(projectName + \"_\" + jobName + \"_\" + jobVersion.replace(\".\",\"_\") + \"_\" + dateStrFormat.format(new java.util.Date()))" + NL + "                    .withClusterName(";
  protected final String TEXT_428 = ")" + NL + "                    .setCredentials(";
  protected final String TEXT_429 = ")" + NL + "                    .withAccessKey(";
  protected final String TEXT_430 = ")" + NL + "                    .withSecretKey(";
  protected final String TEXT_431 = ")" + NL + "                    .withS3AccessKey(";
  protected final String TEXT_432 = ")" + NL + "                    .withS3SecretKey(decryptedPassword_";
  protected final String TEXT_433 = ")" + NL + "                    .withS3Region(";
  protected final String TEXT_434 = ")" + NL + "                    .withBucketName(";
  protected final String TEXT_435 = ")" + NL + "                    .withJarsBucket(";
  protected final String TEXT_436 = ")" + NL + "                    .withPathToAltusCLI(";
  protected final String TEXT_437 = ")" + NL + "                    .withJarToExecute(jobJar)" + NL + "                    .withMainClass(\"";
  protected final String TEXT_438 = ".";
  protected final String TEXT_439 = ".";
  protected final String TEXT_440 = "\")" + NL + "                    .withLibJars(libjars)" + NL + "                    .withConf(conf)" + NL + "                    .withArgs(altusJobArgs)" + NL;
  protected final String TEXT_441 = NL + "                            .withLogLevel(\"";
  protected final String TEXT_442 = "\")";
  protected final String TEXT_443 = NL + "                    .build();";
  protected final String TEXT_444 = NL + "                org.talend.bigdata.launcher.altus.AltusJob instance = new org.talend.bigdata.launcher.altus.AltusSparkWithClusterCreationJob.Builder()" + NL + "                    .withTalendJobName(projectName + \"_\" + jobName + \"_\" + jobVersion.replace(\".\",\"_\") + \"_\" + dateStrFormat.format(new java.util.Date()))" + NL + "                    .withClusterName(";
  protected final String TEXT_445 = ")" + NL + "                    .withEnvironmentName(";
  protected final String TEXT_446 = ")" + NL + "                    .withCloudProvider(";
  protected final String TEXT_447 = ")" + NL + "                    .withDeleteAfterExecution(";
  protected final String TEXT_448 = ")" + NL + "" + NL + "                    .setCredentials(";
  protected final String TEXT_449 = ")" + NL + "                    .withAccessKey(";
  protected final String TEXT_450 = ")" + NL + "                    .withSecretKey(";
  protected final String TEXT_451 = ")" + NL + "                    .withS3AccessKey(";
  protected final String TEXT_452 = ")" + NL + "                    .withS3SecretKey(decryptedPassword_";
  protected final String TEXT_453 = ")" + NL + "                    .withS3Region(";
  protected final String TEXT_454 = ")" + NL + "                    .withBucketName(";
  protected final String TEXT_455 = ")" + NL + "                    .withJarsBucket(";
  protected final String TEXT_456 = ")" + NL + "" + NL + "                    .withUseCustomJson(";
  protected final String TEXT_457 = ")";
  protected final String TEXT_458 = NL + "                        .withCustomJson(";
  protected final String TEXT_459 = ")";
  protected final String TEXT_460 = NL + "                        .withInstanceType(";
  protected final String TEXT_461 = ")" + NL + "                        .withWorkderNode(";
  protected final String TEXT_462 = ")" + NL + "                        .withSshPrivateKey(";
  protected final String TEXT_463 = ")" + NL + "                        .withClouderaManagerUsername(";
  protected final String TEXT_464 = ")" + NL + "                        .withClouderaManagerPassword(";
  protected final String TEXT_465 = ")";
  protected final String TEXT_466 = NL + NL + "                    .withPathToAltusCLI(";
  protected final String TEXT_467 = ")" + NL + "                    .withJarToExecute(jobJar)" + NL + "                    .withMainClass(\"";
  protected final String TEXT_468 = ".";
  protected final String TEXT_469 = ".";
  protected final String TEXT_470 = "\")" + NL + "                    .withLibJars(libjars)" + NL + "                    .withConf(conf)" + NL + "                    .withArgs(altusJobArgs)" + NL;
  protected final String TEXT_471 = NL + "                            .withLogLevel(\"";
  protected final String TEXT_472 = "\")";
  protected final String TEXT_473 = NL + "                    .build();";
  protected final String TEXT_474 = NL + NL + "                // Add JVM shutdown hook to send a cancel job request to the cluster" + NL + "                Runtime.getRuntime().addShutdownHook(new AltusShutdownHook(instance));" + NL + "                // Submit the actual job" + NL + "                int returnCode = instance.executeJob();" + NL + "                System.out.println(instance.getJobLog());" + NL + "                return returnCode;" + NL + "        }" + NL + "" + NL + "        class AltusShutdownHook extends Thread {" + NL + "                private org.talend.bigdata.launcher.altus.AltusJob job;" + NL + "" + NL + "                public AltusShutdownHook(org.talend.bigdata.launcher.altus.AltusJob job) {" + NL + "                    this.job = job;" + NL + "                }" + NL + "" + NL + "                @Override" + NL + "                public void run() {";
  protected final String TEXT_475 = " log.info(\"Calling Altus Shutdown Hook\"); ";
  protected final String TEXT_476 = NL + "                   try {" + NL + "                    // A cancel request will be actually sent to the cluster only if the job is still ongoing" + NL + "                        job.cancelJob();" + NL + "                    } catch (Exception e) {";
  protected final String TEXT_477 = " log.error(\"Could not send a job cancel request to altus : \"+e.getMessage()); ";
  protected final String TEXT_478 = NL + "                    }" + NL + "                }" + NL + "        }";
  protected final String TEXT_479 = NL + "    private void setupJobWideSSLConfigurations(){";
  protected final String TEXT_480 = NL + "                System.setProperty(\"java.protocol.handler.pkgs\", \"com.sun.net.ssl.internal.www.protocol\");" + NL + "                javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(" + NL + "                    new javax.net.ssl.HostnameVerifier(){" + NL + "                        public boolean verify(String hostName,javax.net.ssl.SSLSession session)" + NL + "                            {" + NL + "                                return true;" + NL + "                            }" + NL + "                    }" + NL + "                );";
  protected final String TEXT_481 = NL + "            System.setProperty(\"javax.net.ssl.trustStore\", ";
  protected final String TEXT_482 = ");" + NL + "            System.setProperty(\"javax.net.ssl.trustStoreType\", \"";
  protected final String TEXT_483 = "\");";
  protected final String TEXT_484 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_485 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_486 = ");";
  protected final String TEXT_487 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_488 = " = ";
  protected final String TEXT_489 = "; ";
  protected final String TEXT_490 = NL + "            System.setProperty(\"javax.net.ssl.trustStorePassword\", decryptedPassword_";
  protected final String TEXT_491 = ");";
  protected final String TEXT_492 = NL + "                System.setProperty(\"javax.net.ssl.keyStore\", ";
  protected final String TEXT_493 = ");" + NL + "                System.setProperty(\"javax.net.ssl.keyStoreType\", \"";
  protected final String TEXT_494 = "\");";
  protected final String TEXT_495 = NL + "                    String decryptedPwd_";
  protected final String TEXT_496 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_497 = ");";
  protected final String TEXT_498 = NL + "                    String decryptedPwd_";
  protected final String TEXT_499 = " = ";
  protected final String TEXT_500 = ";";
  protected final String TEXT_501 = NL + "                System.setProperty(\"javax.net.ssl.keyStorePassword\",decryptedPwd_";
  protected final String TEXT_502 = ");";
  protected final String TEXT_503 = NL + "                System.clearProperty(\"javax.net.ssl.keyStore\");" + NL + "                System.clearProperty(\"javax.net.ssl.keyStoreType\");" + NL + "                System.clearProperty(\"javax.net.ssl.keyStorePassword\");";
  protected final String TEXT_504 = NL + "            // No SSL configurations required";
  protected final String TEXT_505 = NL + "    }" + NL + "" + NL + "    private String genTempFolderForComponent(String name) {" + NL + "        java.io.File tempDir = new java.io.File(\"/tmp/\" + pid, name);" + NL + "        String tempDirPath = tempDir.getPath();" + NL + "        if (java.io.File.separatorChar != '/')" + NL + "            tempDirPath = tempDirPath.replace(java.io.File.separatorChar, '/');" + NL + "        return tempDirPath;" + NL + "    }" + NL + "" + NL + "    private void initContext(){" + NL + "        //get context" + NL + "        try{" + NL + "            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead." + NL + "            java.io.InputStream inContext = ";
  protected final String TEXT_506 = ".class.getClassLoader().getResourceAsStream(\"";
  protected final String TEXT_507 = "/contexts/\"+contextStr+\".properties\");" + NL + "            if(isDefaultContext && inContext == null){" + NL + "            }else{" + NL + "                if(inContext!=null){" + NL + "                    //defaultProps is in order to keep the original context value" + NL + "                    defaultProps.load(inContext);" + NL + "                    inContext.close();" + NL + "                    context = new ContextProperties(defaultProps);" + NL + "                }else{" + NL + "                    //print info and job continue to run, for case: context_param is not empty." + NL + "                    System.err.println(\"Could not find the context \" + contextStr);" + NL + "                }" + NL + "            }" + NL + "" + NL + "            if(!context_param.isEmpty()){" + NL + "                context.putAll(context_param);" + NL + "            }" + NL + "            context.loadValue(context_param,null);" + NL + "            if(parentContextMap != null && !parentContextMap.isEmpty()){";
  protected final String TEXT_508 = NL + "                    if(parentContextMap.containsKey(\"";
  protected final String TEXT_509 = "\")){" + NL + "                        context.";
  protected final String TEXT_510 = " = (";
  protected final String TEXT_511 = ") parentContextMap.get(\"";
  protected final String TEXT_512 = "\");" + NL + "                    }";
  protected final String TEXT_513 = NL + "            }" + NL + "        }catch (java.io.IOException ie){" + NL + "            System.err.println(\"Could not load context \"+contextStr);" + NL + "            ie.printStackTrace();" + NL + "        }";
  protected final String TEXT_514 = NL + "                // Cloudera Navigator Code";
  protected final String TEXT_515 = NL + "                // Atlas Code";
  protected final String TEXT_516 = NL + "    }" + NL + "" + NL + "    private void setContext(Configuration conf, org.apache.spark.api.java.JavaSparkContext ctx){" + NL + "        //get context" + NL + "        //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead." + NL + "        java.net.URL inContextUrl = ";
  protected final String TEXT_517 = ".class.getClassLoader().getResource(\"";
  protected final String TEXT_518 = "/contexts/\"+contextStr+\".properties\");" + NL + "        if(isDefaultContext && inContextUrl == null){" + NL + "" + NL + "        }else{" + NL + "            if(inContextUrl!=null){" + NL + "                conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr+\".properties\");";
  protected final String TEXT_519 = NL + "                    java.io.File contextFile = new java.io.File(inContextUrl.getPath());" + NL + "                    if(contextFile.exists()) {" + NL + "                        ctx.addFile(inContextUrl.getPath());" + NL + "                    } else {" + NL + "                        java.io.InputStream contextIn = ";
  protected final String TEXT_520 = ".class.getClassLoader().getResourceAsStream(\"";
  protected final String TEXT_521 = "/contexts/\"+contextStr+\".properties\");" + NL + "                        if(contextIn != null){" + NL + "                            java.io.File tmpFile = new java.io.File(System.getProperty(\"java.io.tmpdir\") + \"/\" + jobName,  contextStr+\".properties\");" + NL + "                            java.io.OutputStream contextOut = null;" + NL + "                            try {" + NL + "                                tmpFile.getParentFile().mkdir();" + NL + "                                if(tmpFile.exists()) { tmpFile.delete(); }" + NL + "                                tmpFile.createNewFile();" + NL + "                                contextOut = new java.io.FileOutputStream(tmpFile);" + NL + "" + NL + "                                int len = -1;" + NL + "                                byte[] b = new byte[4096];" + NL + "                                while ((len = contextIn.read(b)) != -1) {" + NL + "                                    contextOut.write(b, 0, len);" + NL + "                                }" + NL + "                            } catch(java.io.IOException ioe) {" + NL + "                                ioe.printStackTrace();" + NL + "                            } finally {" + NL + "                                try {" + NL + "                                    contextIn.close();" + NL + "                                    if(contextOut != null) {" + NL + "                                        contextOut.close();" + NL + "                                    }" + NL + "                                } catch (java.io.IOException ioe) {" + NL + "                                    ioe.printStackTrace();" + NL + "                                }" + NL + "                            }" + NL + "                            ctx.addFile(tmpFile.getPath());" + NL + "                            tmpFile.deleteOnExit();" + NL + "                        }" + NL + "                    }";
  protected final String TEXT_522 = NL + NL + "            }" + NL + "        }" + NL + "" + NL + "        if(!context_param.isEmpty()){" + NL + "            for(Object contextKey : context_param.keySet()){" + NL + "                conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX + contextKey, context.getProperty(contextKey.toString()));" + NL + "                conf.set(ContextProperties.CONTEXT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, \"\") + \" \" + contextKey);" + NL + "            }" + NL + "        }" + NL + "" + NL + "        if(parentContextMap != null && !parentContextMap.isEmpty()){";
  protected final String TEXT_523 = NL + "                if(parentContextMap.containsKey(\"";
  protected final String TEXT_524 = "\")){" + NL + "                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + \"";
  protected final String TEXT_525 = "\", parentContextMap.get(\"";
  protected final String TEXT_526 = "\").toString());" + NL + "                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, \"\") + \" \" + \"";
  protected final String TEXT_527 = "\");" + NL + "                }";
  protected final String TEXT_528 = NL + "        }" + NL + "    }" + NL;
  protected final String TEXT_529 = NL + "        private static boolean isCloudDriverCall(String[] args) {" + NL + "            List<String> argsList = java.util.Arrays.asList(args);" + NL + "            int indexlibjars = argsList.indexOf(\"-calledByLivy\");" + NL + "            return (indexlibjars != -1);" + NL + "        }";
  protected final String TEXT_530 = NL + "        private boolean isCloudDriverCall(String[] args) {" + NL + "            List<String> argsList = java.util.Arrays.asList(args);" + NL + "            int indexlibjars = argsList.indexOf(\"-calledByGoogleDataproc\");" + NL + "            return (indexlibjars != -1);" + NL + "        }";
  protected final String TEXT_531 = NL + "        private static boolean isCloudDriverCall(String[] args) {" + NL + "            List<String> argsList = java.util.Arrays.asList(args);" + NL + "            int indexlibjars = argsList.indexOf(\"-calledByAltus\");" + NL + "            return (indexlibjars != -1);" + NL + "        }";
  protected final String TEXT_532 = NL + NL + "    private void evalParam(String arg) {" + NL + "        if (arg.startsWith(\"--resuming_logs_dir_path\")) {" + NL + "            resuming_logs_dir_path = arg.substring(25);" + NL + "        } else if (arg.startsWith(\"--resuming_checkpoint_path\")) {" + NL + "            resuming_checkpoint_path = arg.substring(27);" + NL + "        } else if (arg.startsWith(\"--parent_part_launcher\")) {" + NL + "            parent_part_launcher = arg.substring(23);" + NL + "        } else if (arg.startsWith(\"--father_pid=\")) {" + NL + "            fatherPid = arg.substring(13);" + NL + "        } else if (arg.startsWith(\"--root_pid=\")) {" + NL + "            rootPid = arg.substring(11);" + NL + "        } else if (arg.startsWith(\"--pid=\")) {" + NL + "            pid = arg.substring(6);" + NL + "        } else if (arg.startsWith(\"--context=\")) {";
  protected final String TEXT_533 = NL + "                cloudApiArgs.add(arg);";
  protected final String TEXT_534 = NL + "            contextStr = arg.substring(\"--context=\".length());" + NL + "            isDefaultContext = false;" + NL + "        } else if (arg.startsWith(\"--context_param\")) {" + NL + "            String keyValue = arg.substring(\"--context_param\".length() + 1);" + NL + "            int index = -1;" + NL + "            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {" + NL + "                context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));" + NL + "            }";
  protected final String TEXT_535 = NL + "                cloudApiArgs.add(arg);";
  protected final String TEXT_536 = NL + "        } else if (arg.startsWith(\"--stat_port=\")) {" + NL + "            String portStatsStr = arg.substring(12);" + NL + "            if (portStatsStr != null && !portStatsStr.equals(\"null\")) {" + NL + "                portStats = Integer.parseInt(portStatsStr);" + NL + "            }" + NL + "        } else if (arg.startsWith(\"--client_host=\")) {" + NL + "            clientHost = arg.substring(14);" + NL + "        } else if (arg.startsWith(\"--log4jLevel=\")) {" + NL + "            log4jLevel = arg.substring(13);" + NL + "        } else if (arg.startsWith(\"--inspect\")) {" + NL + "            doInspect = Boolean.valueOf(arg.substring(\"--inspect=\".length()));" + NL + "        }" + NL + "    }" + NL + "" + NL + "    private void normalizeArgs(String[] args){" + NL + "        java.util.List<String> argsList = java.util.Arrays.asList(args);" + NL + "        int indexlibjars = argsList.indexOf(\"-libjars\") + 1;" + NL + "        libjars = indexlibjars == 0 ? null : argsList.get(indexlibjars);";
  protected final String TEXT_537 = NL + "            if(!isCloudDriverCall(args)) {" + NL + "                try {" + NL + "                    StringBuilder sb = new StringBuilder();" + NL + "                    routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "                    boolean isFirst = true;" + NL + "                    for(String libjar:libjars.split(\",\")) {" + NL + "                        if(!isFirst) {" + NL + "                            sb.append(\",\");" + NL + "                        }" + NL + "                        isFirst = false;" + NL + "                        sb.append(getJarsToRegister.replaceJarPaths(libjar));" + NL + "                    }" + NL + "                    libjars = sb.toString();" + NL + "                } catch (java.lang.Exception e) {";
  protected final String TEXT_538 = " log.error(e.getMessage()); ";
  protected final String TEXT_539 = NL + "                }" + NL + "            }";
  protected final String TEXT_540 = NL + "            try {" + NL + "                StringBuilder sb = new StringBuilder();" + NL + "                routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "                boolean isFirst = true;" + NL + "                for(String libjar:libjars.split(\",\")) {" + NL + "                    if(!isFirst) {" + NL + "                        sb.append(\",\");" + NL + "                    }" + NL + "                    isFirst = false;" + NL + "                    sb.append(getJarsToRegister.replaceJarPaths(libjar));" + NL + "                }" + NL + "                libjars = sb.toString();" + NL + "            } catch (java.lang.Exception e) {";
  protected final String TEXT_541 = " log.error(e.getMessage()); ";
  protected final String TEXT_542 = NL + "            }";
  protected final String TEXT_543 = NL + "    }" + NL + "" + NL + "    private final String[][] escapeChars = {" + NL + "        {\"\\\\\\\\\",\"\\\\\"},{\"\\\\n\",\"\\n\"},{\"\\\\'\",\"\\'\"},{\"\\\\r\",\"\\r\"}," + NL + "        {\"\\\\f\",\"\\f\"},{\"\\\\b\",\"\\b\"},{\"\\\\t\",\"\\t\"}" + NL + "        };" + NL + "    private String replaceEscapeChars (String keyValue) {" + NL + "" + NL + "        if (keyValue == null || (\"\").equals(keyValue.trim())) {" + NL + "            return keyValue;" + NL + "        }" + NL + "" + NL + "        StringBuilder result = new StringBuilder();" + NL + "        int currIndex = 0;" + NL + "        while (currIndex < keyValue.length()) {" + NL + "            int index = -1;" + NL + "            // judege if the left string includes escape chars" + NL + "            for (String[] strArray : escapeChars) {" + NL + "                index = keyValue.indexOf(strArray[0],currIndex);" + NL + "                if (index>=0) {" + NL + "" + NL + "                    result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));" + NL + "                    currIndex = index + strArray[0].length();" + NL + "                    break;" + NL + "                }" + NL + "            }" + NL + "            // if the left string doesn't include escape chars, append the left into the result" + NL + "            if (index < 0) {" + NL + "                result.append(keyValue.substring(currIndex));" + NL + "                currIndex = currIndex + keyValue.length();" + NL + "            }" + NL + "        }" + NL + "" + NL + "        return result.toString();" + NL + "    }" + NL + "" + NL + "    public String getStatus() {" + NL + "        return status;" + NL + "    }" + NL + "}";
  protected final String TEXT_544 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    IProcess process = (IProcess)v.get(0);
    List<INode> rootNodes = (List<INode>)v.get(1);
    List<INode> confNodes = new ArrayList<INode>(rootNodes);

    INode sparkConfig = (INode)v.get(2);
    confNodes.add(sparkConfig);
    org.talend.hadoop.distribution.ESparkVersion sparkVersion
            = org.talend.hadoop.distribution.spark.SparkVersionUtil.getSparkVersion(sparkConfig);

    Boolean isThereAtLeastOneMLComponent = (Boolean) v.get(4);

    String processId = process.getId();

    String cid = "Spark";
    String className = process.getName();
    boolean isTestContainer=ProcessUtils.isTestContainer(process);
    if (isTestContainer) {
        className = className + "Test";
    }
    String jobFolderName = JavaResourcesHelper.getJobFolderName(className, process.getVersion());

    String testCaseFolderName = "";
    IProcess baseProcess = ProcessUtils.getTestContainerBaseProcess(process);
    if (baseProcess != null) {
        testCaseFolderName = JavaResourcesHelper.getJobFolderName(baseProcess.getName(), baseProcess.getVersion()) + '/';
    }
    testCaseFolderName = testCaseFolderName + JavaResourcesHelper.getJobFolderName(process.getName(), process.getVersion());
    String jobClassPackage = codeGenArgument.getCurrentProjectName().toLowerCase() + '/' + testCaseFolderName;

    List<IContextParameter> params = new ArrayList<IContextParameter>();
    params=process.getContextManager().getDefaultContext().getContextParameterList();
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(process, "__LOG4J_ACTIVATE__"));
    boolean tuningProperties = "true".equals(ElementParameterParser.getValue(sparkConfig, "__ADVANCED_SETTINGS_CHECK__"));
    boolean setWebuiPort = "true".equals(ElementParameterParser.getValue(sparkConfig, "__WEB_UI_PORT_CHECK__"));
    boolean setExecutorCores = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SPARK_EXECUTOR_CORES_CHECK__"));

    String sparkMode = ElementParameterParser.getValue(sparkConfig, "__SPARK_MODE__");
    String sparkDistribution = ElementParameterParser.getValue(sparkConfig, "__DISTRIBUTION__");
    String sparkDistribVersion = ElementParameterParser.getValue(sparkConfig, "__SPARK_VERSION__");

    boolean useLocalMode = "true".equals(ElementParameterParser.getValue(process, "__SPARK_LOCAL_MODE__"));
    boolean useStandaloneMode = !useLocalMode && "CLUSTER".equals(sparkMode);
    final boolean useYarnClientMode = !useLocalMode && "YARN_CLIENT".equals(sparkMode);
    boolean useYarnClusterMode = !useLocalMode && "YARN_CLUSTER".equals(sparkMode);
    boolean useYarnMode = useYarnClusterMode || useYarnClientMode;

    boolean isExecutedThroughSparkJobServer = false;
    boolean isExecutedThroughLivy = false;
    boolean isGoogleDataprocDistribution = false;
    boolean isAltusDistribution = false;
    boolean isCloudDistribution = false;
    boolean useCloudLauncher = false;

    boolean useMapRTicket = ElementParameterParser.getBooleanValue(sparkConfig, "__USE_MAPRTICKET__");
    String mapRTicketUsername = ElementParameterParser.getValue(sparkConfig, "__USERNAME__");
    String mapRTicketCluster = ElementParameterParser.getValue(sparkConfig, "__MAPRTICKET_CLUSTER__");
    String mapRTicketDuration = ElementParameterParser.getValue(sparkConfig, "__MAPRTICKET_DURATION__");

    boolean setMapRHomeDir = ElementParameterParser.getBooleanValue(sparkConfig, "__SET_MAPR_HOME_DIR__");
    String mapRHomeDir = ElementParameterParser.getValue(sparkConfig, "__MAPR_HOME_DIR__");

    boolean setMapRHadoopLogin = ElementParameterParser.getBooleanValue(sparkConfig, "__SET_HADOOP_LOGIN__");
    String mapRHadoopLogin = ElementParameterParser.getValue(sparkConfig, "__HADOOP_LOGIN__");

    String passwordFieldName = "";

    boolean isCustom = false;
    boolean sparkUseKrb = "true".equals(ElementParameterParser.getValue(sparkConfig, "__USE_KRB__"));
    org.talend.hadoop.distribution.component.SparkBatchComponent sparkBatchDistrib = null;

    if(!useLocalMode) {
        try {
            sparkBatchDistrib = (org.talend.hadoop.distribution.component.SparkBatchComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(sparkDistribution, sparkDistribVersion);
        } catch (java.lang.Exception e) {
            e.printStackTrace();
            return "";
        }

        isCustom = sparkBatchDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;
        isExecutedThroughSparkJobServer = !isCustom && sparkBatchDistrib.isExecutedThroughSparkJobServer();
        isExecutedThroughLivy = !isCustom && sparkBatchDistrib.isExecutedThroughLivy();
        isGoogleDataprocDistribution = !isCustom && sparkBatchDistrib.isGoogleDataprocDistribution();
        isAltusDistribution = !isCustom && sparkBatchDistrib.isAltusDistribution();
        isCloudDistribution = !isCustom && sparkBatchDistrib.isCloudDistribution();
        useCloudLauncher = !isCustom && sparkBatchDistrib.useCloudLauncher();
    }

    // Use to define if the spark version currently used is 1.3.
    boolean isSpark13 = sparkVersion == org.talend.hadoop.distribution.ESparkVersion.SPARK_1_3;

    List<Map<String, String>> sparkAdvancedProperties = (List<Map<String,String>>)ElementParameterParser.getObjectValue(sparkConfig, "__SPARK_ADVANCED_PROPERTIES__");

    java.util.List<String> jarsToRegister = null;

    boolean stats = codeGenArgument.isStatistics();

    // Kerberos variables
    boolean useKrb = false;
    boolean useKeytab = false;
    String keytabPrincipal = null;
    String keytabPath = null;

    String username = null;

    // Spark configurations for caching
    // In case we have multiple components that set different configurations, the last component in the list imposes its configurations
    List<INode> cacheConfNodes = new ArrayList<INode>();

    // Compressing RDDs is a global Spark config
    // So when at least one tCache use compression we use compression globally
    // tCacheOut nodes
    boolean cacheCompressRdd = false; // Compress the cached RDD by the tCache Component
    for (INode pNode : process.getNodesOfType("tCacheOut")) {
        cacheConfNodes.add(pNode);
        cacheCompressRdd = ("true").equals(ElementParameterParser.getValue(pNode, "__COMPRESSRDD__"));
        if(cacheCompressRdd) break;
    }

    // Compressing the RDDs is a global Spark config
    // So when at least one tReplicate use compression we use compression globally
    // tReplicate nodes
    boolean replicateCompressRdd = false; // Compress the chached RDD by the tReplicate Component
    for (INode pNode : process.getNodesOfType("tReplicate")) {
        cacheConfNodes.add(pNode);
        replicateCompressRdd = ("true").equals(ElementParameterParser.getValue(pNode, "__CACHEOUTPUT__")) && ("true").equals(ElementParameterParser.getValue(pNode, "__COMPRESSRDD__"));
        if(replicateCompressRdd) break;
    }

    boolean generatedCompressionConfig = false; // Only generate compression config once
    boolean generatedTachyonConfig = false; // Only generate tachyon config once

    // Parse nodes and setup configuration map
    for (INode pNode : cacheConfNodes) {
        boolean compressRdd = cacheCompressRdd || replicateCompressRdd;
        String compressCodec = ElementParameterParser.getValue(pNode, "__COMPRESSCODEC__");
        String storageLevel = ElementParameterParser.getValue(pNode, "__STORAGELEVEL__");
        String tachyonStoreUrl = ElementParameterParser.getValue(pNode, "__TACHYON_STORE_URL__");
        String tachyonStoreBaseDir = ElementParameterParser.getValue(pNode, "__TACHYON_STORE_BASEDIR__");

        if(!generatedTachyonConfig && storageLevel.equals("OFF_HEAP")){
            generatedTachyonConfig = true;
            Map<String, String> tachyonStoreUrlMap = new HashMap<String, String>();
            tachyonStoreUrlMap.put("PROPERTY","\"spark.tachyonStore.url\"");
            tachyonStoreUrlMap.put("VALUE",tachyonStoreUrl);
            sparkAdvancedProperties.add(tachyonStoreUrlMap);
            Map<String, String> tachyonStoreBaseDirMap = new HashMap<String, String>();
            tachyonStoreBaseDirMap.put("PROPERTY","\"spark.tachyonStore.baseDir\"");
            tachyonStoreBaseDirMap.put("VALUE",tachyonStoreBaseDir);
            sparkAdvancedProperties.add(tachyonStoreBaseDirMap);
        }

        if(!generatedCompressionConfig && compressRdd && (storageLevel.equals("MEMORY_ONLY_SER") || storageLevel.equals("MEMORY_AND_DISK_SER") || storageLevel.equals("MEMORY_AND_DISK_SER") || storageLevel.equals("MEMORY_ONLY_SER_2") || storageLevel.equals("MEMORY_AND_DISK_SER_2"))){
            generatedCompressionConfig = true;
            Map<String, String> rddCompression = new HashMap<String, String>();
            rddCompression.put("PROPERTY", "\"spark.rdd.compress\"");
            rddCompression.put("VALUE", "\"true\"");
            sparkAdvancedProperties.add(rddCompression);
            Map<String, String> rddCompressionCodec = new HashMap<String, String>();
            rddCompressionCodec.put("PROPERTY", "\"spark.io.compression.codec\"");
            rddCompressionCodec.put("VALUE", '"'+compressCodec+'"');
            sparkAdvancedProperties.add(rddCompressionCodec);
        }
    }

    // tHadoopConfManager component generation.
    List<INode> hcmNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tHadoopConfManager")) {
        hcmNodes.add(pNode);
    }

    if(hcmNodes.size() > 1) {
        
    stringBuffer.append(TEXT_1);
    
    }

    // tGSConfiguration component generation.
    List<INode> gsNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tGSConfiguration")) {
        gsNodes.add(pNode);
    }

    if(gsNodes.size() > 1) {
        
    stringBuffer.append(TEXT_2);
    
    }
    if(gsNodes.size() == 1) {
        INode pNode = gsNodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);

        Map<String, String> gsProperties = new HashMap<String, String>();
        gsProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.impl\"");
        gsProperties.put("VALUE", "\"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"");
        sparkAdvancedProperties.add(gsProperties);

        gsProperties = new HashMap<String, String>();
        gsProperties.put("PROPERTY", "\"spark.hadoop.fs.AbstractFileSystem.gs.impl\"");
        gsProperties.put("VALUE", "\"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS\"");
        sparkAdvancedProperties.add(gsProperties);

        gsProperties = new HashMap<String, String>();
        gsProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.system.bucket\"");
        gsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__BUCKET_NAME__"));
        sparkAdvancedProperties.add(gsProperties);

        if(isGoogleDataprocDistribution) {
            gsProperties = new HashMap<String, String>();
            gsProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.project.id\"");
            gsProperties.put("VALUE", ElementParameterParser.getValue(sparkConfig, "__GOOGLE_PROJECT_ID__"));
            sparkAdvancedProperties.add(gsProperties);
        } else {
	        gsProperties = new HashMap<String, String>();
	        gsProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.project.id\"");
	        gsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__GOOGLE_PROJECT_ID__"));
	        sparkAdvancedProperties.add(gsProperties);

	        gsProperties = new HashMap<String, String>();
	        gsProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.enable\"");
	        gsProperties.put("VALUE", "\"true\"");
	        sparkAdvancedProperties.add(gsProperties);

	        boolean useP12CredentialsFileFormat = ElementParameterParser.getBooleanValue(pNode, "__SET_P12_CREDENTIALS_FILE__");

	        if(!useP12CredentialsFileFormat) {
	        	gsProperties = new HashMap<String, String>();
		        gsProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.json.keyfile\"");
		        gsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__PATH_TO_GOOGLE_CREDENTIALS__"));
		        sparkAdvancedProperties.add(gsProperties);
	        } else {
		        gsProperties = new HashMap<String, String>();
		        gsProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.email\"");
		        gsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__SERVICE_ACCOUNT_ID__"));
		        sparkAdvancedProperties.add(gsProperties);

	        	gsProperties = new HashMap<String, String>();
	        	gsProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.keyfile\"");
	        	gsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__SERVICE_ACCOUNT_P12_KEYFILE__"));
	        	sparkAdvancedProperties.add(gsProperties);
	        }
        }
    }
    //End of tGSConfiguration component generation.

    // tS3Configuration component generation.
    List<INode> s3Nodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tS3Configuration")) {
        s3Nodes.add(pNode);
    }

    if(s3Nodes.size() > 1) {
        
    stringBuffer.append(TEXT_3);
    
    }
    if(s3Nodes.size() == 1) {
        INode pNode = s3Nodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);

        Map<String, String> s3Properties = null;
        String s3FileSystem = "\"org.apache.hadoop.fs.s3native.NativeS3FileSystem\"";

        Boolean useS3a = ElementParameterParser.getBooleanValue(pNode, "__USE_S3A__");
        if (useS3a) {
            s3FileSystem = "\"org.apache.hadoop.fs.s3a.S3AFileSystem\"";
            s3Properties = new HashMap<String, String>();
            s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3n.impl\"");
            s3Properties.put("VALUE", s3FileSystem);
            sparkAdvancedProperties.add(s3Properties);
            if (!isAltusDistribution) {
	            s3Properties = new HashMap<String, String>();
	            s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3a.user.agent.prefix\"");
	            s3Properties.put("VALUE", "routines.system.Constant.TALEND_USER_AGENT");
            	sparkAdvancedProperties.add(s3Properties);
            }
        }

        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3.impl\"");
        s3Properties.put("VALUE", s3FileSystem);
        sparkAdvancedProperties.add(s3Properties);
        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3n.impl\"");
        s3Properties.put("VALUE", s3FileSystem);
        sparkAdvancedProperties.add(s3Properties);
        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3.awsAccessKeyId\"");
        s3Properties.put("VALUE", ElementParameterParser.getValue(pNode, "__ACCESS_KEY__"));
        sparkAdvancedProperties.add(s3Properties);
        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3n.awsAccessKeyId\"");
        s3Properties.put("VALUE", ElementParameterParser.getValue(pNode, "__ACCESS_KEY__"));
        sparkAdvancedProperties.add(s3Properties);
        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3.awsSecretAccessKey\"");
        passwordFieldName = "__SECRET_KEY__";
        String password = "";
        if (ElementParameterParser.canEncrypt(pNode, passwordFieldName)) {
            password = ElementParameterParser.getEncryptedValue(pNode, passwordFieldName);
            password = "routines.system.PasswordEncryptUtil.decryptPassword(" + password + ")";
        } else {
            password = ElementParameterParser.getValue(pNode, passwordFieldName);
        }
        s3Properties.put("VALUE", password);
        sparkAdvancedProperties.add(s3Properties);
        s3Properties = new HashMap<String, String>();
        s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3n.awsSecretAccessKey\"");
        s3Properties.put("VALUE", password);
        sparkAdvancedProperties.add(s3Properties);

        if (useS3a) {
            s3Properties = new HashMap<String, String>();
            s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3a.awsAccessKeyId\"");
            s3Properties.put("VALUE", ElementParameterParser.getValue(pNode, "__ACCESS_KEY__"));
            sparkAdvancedProperties.add(s3Properties);
            s3Properties = new HashMap<String, String>();
            s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3a.awsSecretAccessKey\"");
            s3Properties.put("VALUE", password);
            sparkAdvancedProperties.add(s3Properties);
        }
        // Set endpoint
        Boolean setEndpoint = ElementParameterParser.getBooleanValue(pNode, "__SET_ENDPOINT__");
        Boolean setRegion = ElementParameterParser.getBooleanValue(pNode, "__SET_REGION__");
        if (setEndpoint || setRegion) {
            org.talend.hadoop.distribution.component.HadoopComponent s3Distrib = null;
            if(!useLocalMode) {
	            try {
	                s3Distrib = (org.talend.hadoop.distribution.component.HadoopComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(
	                        sparkDistribution, sparkDistribVersion);
	            } catch (java.lang.Exception e) {
	                e.printStackTrace();
	                return "";
	            }
            }
            if (((s3Distrib != null) && (s3Distrib.doSupportS3V4())) || (useLocalMode)) {
                String endpoint = ElementParameterParser.getValue(pNode, "__ENDPOINT__");
                if (setRegion) {
                    endpoint = ElementParameterParser.getValue(pNode, "__REGION__");
                }

                s3Properties = new HashMap<String, String>();
                s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3.endpoint\"");
                s3Properties.put("VALUE", endpoint);
                sparkAdvancedProperties.add(s3Properties);

                s3Properties = new HashMap<String, String>();
                s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3n.endpoint\"");
                s3Properties.put("VALUE", endpoint);
                sparkAdvancedProperties.add(s3Properties);

                s3Properties = new HashMap<String, String>();
                s3Properties.put("PROPERTY", "\"hadoop.fs.s3.endpoint\"");
                s3Properties.put("VALUE", endpoint);
                sparkAdvancedProperties.add(s3Properties);

                s3Properties = new HashMap<String, String>();
                s3Properties.put("PROPERTY", "\"hadoop.fs.s3n.endpoint\"");
                s3Properties.put("VALUE", endpoint);
                sparkAdvancedProperties.add(s3Properties);

                if (useS3a) {
                    s3Properties = new HashMap<String, String>();
                    s3Properties.put("PROPERTY", "\"spark.hadoop.fs.s3a.endpoint\"");
                    s3Properties.put("VALUE", endpoint);
                    sparkAdvancedProperties.add(s3Properties);

                    s3Properties = new HashMap<String, String>();
                    s3Properties.put("PROPERTY", "\"hadoop.fs.s3a.endpoint\"");
                    s3Properties.put("VALUE", endpoint);
                    sparkAdvancedProperties.add(s3Properties);
                }
            }
        }
    }
    //End of tS3Configuration component generation.

    // tHiveConfiguration component generation.
    List<INode> hiveNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tHiveConfiguration")) {
        hiveNodes.add(pNode);
    }
    if ((hiveNodes.size() > 0) && !useLocalMode && ("MAPR".equals(sparkDistribution))) {
        Map<String, String> hiveProperties = null;
        hiveProperties = new HashMap<String, String>();
        hiveProperties.put("PROPERTY", "\"spark.sql.hive.metastore.sharedPrefixes\"");
        hiveProperties.put("VALUE", "\"com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,com.mapr.fs.shim.LibraryLoader,com.mapr.security.JNISecurity,com.mapr.fs.jni\"");
        sparkAdvancedProperties.add(hiveProperties);
    }
    if ((hiveNodes.size() > 0) && !useLocalMode && sparkBatchDistrib.doRequireMetastoreVersionOverride()
    		&& (sparkBatchDistrib.getHiveMetastoreVersionForSpark() != null)) {
    	String sparkHiveVersion = sparkBatchDistrib.getHiveMetastoreVersionForSpark();
        Map<String, String> hiveMetastoreVersionProperty = null;
        hiveMetastoreVersionProperty = new HashMap<String, String>();
        hiveMetastoreVersionProperty.put("PROPERTY", "\"spark.sql.hive.metastore.version\"");
        hiveMetastoreVersionProperty.put("VALUE", "\"" + sparkHiveVersion + "\"");
        sparkAdvancedProperties.add(hiveMetastoreVersionProperty);

        Map<String, String> hiveMetastoreJarsProperty = null;
        hiveMetastoreJarsProperty = new HashMap<String, String>();
        hiveMetastoreJarsProperty.put("PROPERTY", "\"spark.sql.hive.metastore.jars\"");
        hiveMetastoreJarsProperty.put("VALUE", "\"maven\"");
        sparkAdvancedProperties.add(hiveMetastoreJarsProperty);
    }
    //End of tHiveConfiguration component generation.

    // tTachyonConfiguration component generation.
    List<INode> tTachyonNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tTachyonConfiguration")) {
        tTachyonNodes.add(pNode);
    }

    if(tTachyonNodes.size() > 1) {

    stringBuffer.append(TEXT_4);
    
    }
    if(tTachyonNodes.size() == 1) {
        INode pNode = tTachyonNodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);

        Map<String, String> tachyonProperties = new HashMap<String, String>();
        tachyonProperties.put("PROPERTY", "\"spark.hadoop.fs.tachyon.impl\"");
        tachyonProperties.put("VALUE", "\"tachyon.hadoop.TFS\"");
        sparkAdvancedProperties.add(tachyonProperties);

        //Set underFS username
        //May conflict with the username set HDFSConfiguration
        //The username is used when the Spark worker is not on the same node as the tachyon worker, in this case the the write goes directly
        // to the UnderFS which requires a authentication (in the case of HDFS).
        username = ElementParameterParser.getValue(pNode, "__UNDERFS_USERNAME__");

    }
    //End of tTachyonConfiguration component generation.

    // tHDFSConfiguration component generation.
    List<INode> hdfsNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tHDFSConfiguration")) {
        hdfsNodes.add(pNode);
    }

    if(hdfsNodes.size() > 1) {
    
    stringBuffer.append(TEXT_5);
    
    }
    if(hdfsNodes.size() == 1) {
        INode pNode = hdfsNodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);

        Map<String, String> hdfsProperties = new HashMap<String, String>();
        hdfsProperties.put("PROPERTY", "\"spark.hadoop.fs.defaultFS\"");
        hdfsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__FS_DEFAULT_NAME__"));
        sparkAdvancedProperties.add(hdfsProperties);

        String hadoopDistribution = ElementParameterParser.getValue(pNode, "__DISTRIBUTION__");
        String hadoopVersion = ElementParameterParser.getValue(pNode, "__DB_VERSION__");

        org.talend.hadoop.distribution.component.HDFSComponent hdfsDistrib = null;
        try {
            hdfsDistrib = (org.talend.hadoop.distribution.component.HDFSComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(hadoopDistribution, hadoopVersion);
        } catch (java.lang.Exception e) {
            e.printStackTrace();
            return "";
        }

        boolean mrUseDatanodeHostname = "true".equals(ElementParameterParser.getValue(pNode, "__USE_DATANODE_HOSTNAME__")) && hdfsDistrib.doSupportUseDatanodeHostname();
        if(mrUseDatanodeHostname) {
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.dfs.client.use.datanode.hostname\"");
            hdfsProperties.put("VALUE", "\"true\"");
            sparkAdvancedProperties.add(hdfsProperties);
        }

        useKrb = "true".equals(ElementParameterParser.getValue(pNode, "__USE_KRB__")) && (hdfsDistrib.doSupportKerberos());
        if(useKrb) {
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.dfs.namenode.kerberos.principal\"");
            hdfsProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__NAMENODE_PRINCIPAL__"));
            sparkAdvancedProperties.add(hdfsProperties);
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.hadoop.security.authorization\"");
            hdfsProperties.put("VALUE", "\"true\"");
            sparkAdvancedProperties.add(hdfsProperties);
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.hadoop.security.authentication\"");
            hdfsProperties.put("VALUE", "\"kerberos\"");
            sparkAdvancedProperties.add(hdfsProperties);
            useKeytab = "true".equals(ElementParameterParser.getValue(pNode, "__USE_KEYTAB__"));
            if(useKeytab) {
                keytabPrincipal = ElementParameterParser.getValue(pNode, "__PRINCIPAL__");
                keytabPath = ElementParameterParser.getValue(pNode, "__KEYTAB_PATH__");
            }
        } else {
            username = ElementParameterParser.getValue(pNode, "__USERNAME__");
        }

        List<Map<String, String>> hadoopProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(pNode, "__HADOOP_ADVANCED_PROPERTIES__");
        for(Map<String, String> item : hadoopProps){
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.\" + " + item.get("PROPERTY"));
            hdfsProperties.put("VALUE", item.get("VALUE"));
            sparkAdvancedProperties.add(hdfsProperties);
        }

        boolean useHDFSEnc = ElementParameterParser.getBooleanValue(pNode, "__USE_HDFS_ENCRYPTION__");
        String hdfsKMS = ElementParameterParser.getValue(pNode, "__HDFS_ENCRYPTION_KEY_PROVIDER__");
        if(useHDFSEnc){
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.hadoop.security.key.provider.path\"");
            hdfsProperties.put("VALUE", hdfsKMS);
            sparkAdvancedProperties.add(hdfsProperties);
            hdfsProperties = new HashMap<String, String>();
            hdfsProperties.put("PROPERTY", "\"spark.hadoop.dfs.encryption.key.provider.uri\"");
            hdfsProperties.put("VALUE", hdfsKMS);
            sparkAdvancedProperties.add(hdfsProperties);
        }

    }
    //End of tHDFSConfiguration component generation.

    // tCassandraConfiguration component generation.
    List<INode> cassandraNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tCassandraConfiguration")) {
        cassandraNodes.add(pNode);
    }

    if(cassandraNodes.size() > 1) {
    
    stringBuffer.append(TEXT_6);
    
    }
    if(cassandraNodes.size() == 1) {
        INode pNode = cassandraNodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);
        
    
class CassandraConfiguration_Helper{
	public Map<String, String> getProperties(INode node){
		java.util.Map<String, String> properties = new java.util.HashMap<String, String>();
		
        java.util.List<java.util.Map<String, String>> configurations = (java.util.List<java.util.Map<String, String>>)ElementParameterParser.getObjectValue(node, "__CASSANDRA_CONFIGURATION__");
        //remove some key from the configuration table, but can remove it from migration task, so ignore them on code generate stage
        java.util.List<String> ignoreConfList = new java.util.ArrayList<String>();
        ignoreConfList.add("connection_rpc_port");//"spark.cassandra.connection.rpc.port"
		ignoreConfList.add("connection_native_port");//"spark.cassandra.connection.native.port"
        java.util.Map<String, String> confMapping = new java.util.HashMap<String, String>();
        confMapping.put("connection_conf_factory","spark.cassandra.connection.conf.factory");
        confMapping.put("connection_keep_alive_ms","spark.cassandra.connection.keep_alive_ms");
        confMapping.put("connection_timeout_ms","spark.cassandra.connection.timeout_ms");
        confMapping.put("reconnection_delay_ms_min","spark.cassandra.connection.reconnection_delay_ms.min");
        confMapping.put("connection_reconnection_delay_ms_max","spark.cassandra.connection.reconnection_delay_ms.max");
        confMapping.put("connection_local_dc","spark.cassandra.connection.local_dc");
        confMapping.put("auth_conf_factory","spark.cassandra.auth.conf.factory");
        confMapping.put("query_retry_count","spark.cassandra.query.retry.count");
        confMapping.put("read_timeout_ms","spark.cassandra.read.timeout_ms");   
        confMapping.put("input_split_size","spark.cassandra.input.split.size");
        confMapping.put("input_page_row_size","spark.cassandra.input.page.row.size");
        confMapping.put("input_consistency_level","spark.cassandra.input.consistency.level");
        for(java.util.Map<String, String> conf : configurations){
            String confKey = conf.get("KEY");
            if(ignoreConfList.contains(confKey)){
            	continue;
            }
            String propertyKey = confMapping.containsKey(confKey) ? "\"" + confMapping.get(confKey) + "\"" : confKey;
            properties.put(propertyKey, conf.get("VALUE"));
        }
        String host = ElementParameterParser.getValue(node,"__HOST__");
        if(!"".equals(host)){
        	properties.put("\"spark.cassandra.connection.host\"", host);
        }
        String port = ElementParameterParser.getValue(node,"__PORT__");
        if(!"".equals(port)){
        	properties.put("\"spark.cassandra.connection.port\"", port);
        }
        boolean authentication="true".equalsIgnoreCase(ElementParameterParser.getValue(node, "__REQUIRED_AUTHENTICATION__"));
        String userName = ElementParameterParser.getValue(node, "__USERNAME__");
        String passWord = ElementParameterParser.getPasswordValue(node, "__PASSWORD__");
        if(authentication){
            properties.put("\"spark.cassandra.auth.username\"", userName);
            properties.put("\"spark.cassandra.auth.password\"", passWord);
        }  
        TSetKeystoreUtil tSetKeystoreUtil = new TSetKeystoreUtil(node);
        if(tSetKeystoreUtil.useHTTPS()){
        	properties.put("\"spark.cassandra.connection.ssl.enabled\"", "\"true\"");
        	properties.put("\"spark.cassandra.connection.ssl.trustStore.type\"", tSetKeystoreUtil.getTrustStoreType());
        	properties.put("\"spark.cassandra.connection.ssl.trustStore.path\"", tSetKeystoreUtil.getTrustStorePath());
        	properties.put("\"spark.cassandra.connection.ssl.trustStore.password\"", tSetKeystoreUtil.getTrustStorePassword());
        	if(tSetKeystoreUtil.needClientAuth()){
        		properties.put("\"spark.cassandra.connection.ssl.clientAuth.enabled\"", "\"true\"");
	        	properties.put("\"spark.cassandra.connection.ssl.keyStore.type\"", tSetKeystoreUtil.getKeyStoreType());
	        	properties.put("\"spark.cassandra.connection.ssl.keyStore.path\"", tSetKeystoreUtil.getKeyStorePath());
	        	properties.put("\"spark.cassandra.connection.ssl.keyStore.password\"", tSetKeystoreUtil.getKeyStorePassword());
        	}
        }
        return properties; 
	}
	
	public Map<String, String> getPropertiesForOutput(INode node){
		java.util.Map<String, String> properties = new java.util.HashMap<String, String>();
		
        java.util.List<java.util.Map<String, String>> configurations = (java.util.List<java.util.Map<String, String>>)ElementParameterParser.getObjectValue(node, "__CASSANDRA_CONFIGURATION__");
        java.util.Map<String, String> confMapping = new java.util.HashMap<String, String>();
        confMapping.put("output_batch_size_rows","spark.cassandra.output.batch.size.rows");
        confMapping.put("output_batch_size_bytes","spark.cassandra.output.batch.size.bytes");
        confMapping.put("output_batch_grouping_key","spark.cassandra.output.batch.grouping.key");
        confMapping.put("output_batch_buffer_size","spark.cassandra.output.batch.buffer.size");
        confMapping.put("output_concurrent_writes","spark.cassandra.output.concurrent.writes");
        confMapping.put("output_consistency_level","spark.cassandra.output.consistency.level");
        confMapping.put("output_throughput_mb_per_sec","spark.cassandra.output.throughput_mb_per_sec");
        for(java.util.Map<String, String> conf : configurations){
            String confKey = conf.get("KEY");
            String propertyKey = confMapping.containsKey(confKey) ? "\"" + confMapping.get(confKey) + "\"" : confKey;
            properties.put(propertyKey, conf.get("VALUE"));
        }
        
        return properties; 
	}
}	

    
        Map<String, String> cassandraProperties = (new CassandraConfiguration_Helper()).getProperties(pNode);
        for(String cPropKey : cassandraProperties.keySet()){
            Map<String, String> cProperty = new HashMap<String, String>();
            cProperty.put("PROPERTY", cPropKey);
            cProperty.put("VALUE", cassandraProperties.get(cPropKey));
            sparkAdvancedProperties.add(cProperty);
        }
    }
    //End of tCassandraConfiguration component generation.

    // tBigQueryConfiguration component generation.
    List<INode> bigQueryNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tBigQueryConfiguration")) {
        bigQueryNodes.add(pNode);
    }

    if(bigQueryNodes.size() > 1) {
    
    stringBuffer.append(TEXT_7);
    
    }
    if(bigQueryNodes.size() == 1) {
        INode pNode = bigQueryNodes.get(0);
        // the configuration components must not be considered as a root node.
        rootNodes.remove(pNode);

        // If dataproc cluster, use dataproc's project id and authenication settings
        Map<String, String> bigqueryProperties = new HashMap<String, String>();
        if(!isGoogleDataprocDistribution) {
	        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.enable\"");
	        bigqueryProperties.put("VALUE", "\"true\"");
	        sparkAdvancedProperties.add(bigqueryProperties);

	        boolean enableP12 = ElementParameterParser.getBooleanValue(pNode, "__SET_P12_CREDENTIALS_FILE__");
	        if(enableP12){
	        	bigqueryProperties = new HashMap<String, String>();
		        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.email\"");
		        bigqueryProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__SERVICE_ACCOUNT_ID__"));
		        sparkAdvancedProperties.add(bigqueryProperties);

		        bigqueryProperties = new HashMap<String, String>();
		        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.keyfile\"");
		        bigqueryProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__SERVICE_ACCOUNT_P12_KEYFILE__"));
		        sparkAdvancedProperties.add(bigqueryProperties);
	        }else{
		        bigqueryProperties = new HashMap<String, String>();
		        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.google.cloud.auth.service.account.json.keyfile\"");
		        bigqueryProperties.put("VALUE", ElementParameterParser.getValue(pNode, "__PATH_TO_GOOGLE_CREDENTIALS__"));
		        sparkAdvancedProperties.add(bigqueryProperties);
	        }
	    }

        bigqueryProperties = new HashMap<String, String>();
        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.impl\"");
        bigqueryProperties.put("VALUE", "\"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"");
        sparkAdvancedProperties.add(bigqueryProperties);

        bigqueryProperties = new HashMap<String, String>();
        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.fs.AbstractFileSystem.gs.impl\"");
        bigqueryProperties.put("VALUE", "\"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS\"");
        sparkAdvancedProperties.add(bigqueryProperties);

        String googleProjectId = isGoogleDataprocDistribution ? ElementParameterParser.getValue(sparkConfig, "__GOOGLE_PROJECT_ID__") : ElementParameterParser.getValue(pNode, "__GOOGLE_PROJECT_ID__");
        bigqueryProperties = new HashMap<String, String>();
        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.fs.gs.project.id\"");
        bigqueryProperties.put("VALUE", googleProjectId);
        sparkAdvancedProperties.add(bigqueryProperties);

        bigqueryProperties = new HashMap<String, String>();
        bigqueryProperties.put("PROPERTY", "\"spark.hadoop.mapred.bq.project.id\"");
        bigqueryProperties.put("VALUE", googleProjectId);
        sparkAdvancedProperties.add(bigqueryProperties);
    }
    //End of tCassandraConfiguration component generation.
    
    stringBuffer.append(TEXT_8);
    
    if(isThereAtLeastOneMLComponent) {

    
	// Everything related to our TalendPipelineModel used in machine learning jobs 
	// should be written there to avoid code duplication.
	
	// This file is intended to be included in both spark_footer.javajet and sparkstreaming_footer.javajet

    stringBuffer.append(TEXT_9);
    
	// Everything related to our TalendPipeline used in machine learning jobs 
	// should be written there to avoid code duplication.
	
	// This file is intended to be included in both spark_footer.javajet and sparkstreaming_footer.javajet

    stringBuffer.append(TEXT_10);
    
    }

    stringBuffer.append(TEXT_11);
    
	// Kryo registrator and all custom kryo serializers should be written there to avoid code duplication
	
	// This file is intended to be included in both spark_footer.javajet and sparkstreaming_footer.javajet

    stringBuffer.append(TEXT_12);
     
	boolean hasAvroGenericRecord = false;
	for (INode rootNode : rootNodes) {
		if(rootNode.getComponent().getName() != null && (rootNode.getComponent().getName().startsWith("tHMap") || rootNode.getComponent().getName().startsWith("tHConvert"))) {
			hasAvroGenericRecord = true;
			break;
		}
	}
	if(hasAvroGenericRecord) {

    stringBuffer.append(TEXT_13);
    
	} // end if(hasAvroGenericRecord)

    stringBuffer.append(TEXT_14);
     
		if(isThereAtLeastOneMLComponent) {
	
    stringBuffer.append(TEXT_15);
    
		}

		Iterable<String> structNameList = (Iterable<String>) v.get(3);		
		for(String structName : structNameList) {
	
    stringBuffer.append(TEXT_16);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_17);
    
		} 
	
    stringBuffer.append(TEXT_18);
     
	if(isThereAtLeastOneMLComponent) {

    stringBuffer.append(TEXT_19);
     
	} // end if(isThereAtLeastOneMLComponent)

    stringBuffer.append(TEXT_20);
     if(useCloudLauncher) { 
    stringBuffer.append(TEXT_21);
     } 
    stringBuffer.append(TEXT_22);
    stringBuffer.append(codeGenArgument.getContextName());
    stringBuffer.append(TEXT_23);
    stringBuffer.append(className );
    stringBuffer.append(TEXT_24);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_25);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_26);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_27);
    }
    stringBuffer.append(TEXT_28);
     if(isExecutedThroughLivy || isAltusDistribution) { 
    stringBuffer.append(TEXT_29);
     } 
    stringBuffer.append(TEXT_30);
     if(isExecutedThroughLivy || isAltusDistribution) { 
    stringBuffer.append(TEXT_31);
     } 
    stringBuffer.append(TEXT_32);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_33);
    
    if (isTestContainer) {
        List<String> instanceList =  ProcessUtils.getTestInstances(process);
        for(String instance : instanceList) {
            String context = ProcessUtils.getInstanceContext(process,instance);
            
    stringBuffer.append(TEXT_34);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_35);
    
                int assertNum = ProcessUtils.getAssertAmount(process);
                
    stringBuffer.append(TEXT_36);
    stringBuffer.append(assertNum);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(className );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(className );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(className );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(context);
    stringBuffer.append(TEXT_43);
    
                String lineSeparator = (String) java.security.AccessController.doPrivileged(
                        new sun.security.action.GetPropertyAction("line.separator"));
                for(String testDataName : ProcessUtils.getTestData(process,instance)){
                    String testData =  ProcessUtils.getTestDataValue(process, instance, testDataName);
                    if(testData==null||testData.length()<=0){
                        continue;
                    }
                    String testDataEnCodeStr = "";
                    try {
                        if (testData != null) {
                            testDataEnCodeStr = (new sun.misc.BASE64Encoder()).encode(testData.getBytes("UTF-8"));
                        }
                    } catch (java.io.UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    StringBuilder testDataBase64 = new StringBuilder();
                    for(String item : testDataEnCodeStr.split(lineSeparator))
                        testDataBase64.append('"').append(item).append("\"+");
                    // Remove trailing commas.
                    if (testDataBase64.length() > 0)
                        testDataBase64.setLength(testDataBase64.length() - 1);

                    if(testDataBase64 != null && testDataBase64.length() > 0){
                        
    stringBuffer.append(TEXT_44);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(testDataName);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(testDataBase64);
    stringBuffer.append(TEXT_48);
    
                    }
                }
                
    stringBuffer.append(TEXT_49);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(instance);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(className );
    stringBuffer.append(TEXT_54);
    
        }
    } // isTestContainer
    
    stringBuffer.append(TEXT_55);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_56);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_57);
    }
    stringBuffer.append(TEXT_58);
    
        if(stats && !isCloudDistribution) {

    stringBuffer.append(TEXT_59);
    
        }

        if(username != null && !"".equals(username)) {

    stringBuffer.append(TEXT_60);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_62);
    
        }
        // this snippet to solve an issue in Spark Driver on MacOSX with snappy
        // you have to run with -Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib

    stringBuffer.append(TEXT_63);
    
        if(sparkConfig!=null) {

    stringBuffer.append(TEXT_64);
    
                if(hcmNodes.size() == 1) {
                    INode pNode = hcmNodes.get(0);

    stringBuffer.append(TEXT_65);
    stringBuffer.append(pNode.getUniqueName());
    stringBuffer.append(TEXT_66);
    
                }

    stringBuffer.append(TEXT_67);
    
                if(isExecutedThroughSparkJobServer) {

    stringBuffer.append(TEXT_68);
    
                } else {
                    if(useCloudLauncher) {

    stringBuffer.append(TEXT_69);
    
                            if(!useLocalMode && sparkUseKrb && (isCustom || sparkBatchDistrib != null && sparkBatchDistrib.doSupportKerberos()) && !isSpark13) {
                                // The following part of code is needed to spawn the thread for the ticket renewal in a secured environment.
                                // Fail silently or log a warning if log4j is activated.

    stringBuffer.append(TEXT_70);
     if(isLog4jEnabled) { 
    stringBuffer.append(TEXT_71);
     } 
    stringBuffer.append(TEXT_72);
    
                            }

    stringBuffer.append(TEXT_73);
    
                    } else { // normal case no livy, no dataproc
                        if(!useLocalMode && sparkUseKrb && (isCustom || sparkBatchDistrib != null && sparkBatchDistrib.doSupportKerberos()) && !isSpark13) {
                            // The following part of code is needed to spawn the thread for the ticket renewal in a secured environment.
                            // Fail silently or log a warning if log4j is activated.

    stringBuffer.append(TEXT_74);
     if(isLog4jEnabled) { 
    stringBuffer.append(TEXT_75);
     } 
    stringBuffer.append(TEXT_76);
    
                        }

    stringBuffer.append(TEXT_77);
    
                    }
                } // else part of if(isExecutedThroughSparkJobServer)

    stringBuffer.append(TEXT_78);
    
        }

    stringBuffer.append(TEXT_79);
    
class TalendLineageAPI{

    /**
    * Find the configuration node given the current node
    * 
    * @param node
    **/
    public INode findConfigurationNode(INode node){
        INode configurationNode = null;
        for (INode pNode : node.getProcess().getNodesOfType("tMRConfiguration")) {
            configurationNode = pNode;
            break;
        }
        for (INode pNode : node.getProcess().getNodesOfType("tSparkConfiguration")) {
            // spark compatibility, will not be run on Map Reduce
            configurationNode = pNode;
            break;
        }
        return configurationNode;
    }

    /**
     * Does the job require lineage generation
     */
    public boolean doRequireLineageSupport(INode node){
        INode configurationNode = findConfigurationNode(node);
        if (configurationNode != null) {
            Boolean useClouderaNavigator = ElementParameterParser.getBooleanValue(configurationNode,"__USE_CLOUDERA_NAVIGATOR__");
            Boolean useAtlas = ElementParameterParser.getBooleanValue(configurationNode,"__USE_ATLAS__");
            return (useClouderaNavigator && doRequireClouderaNavigatorSupport(configurationNode)) || (useAtlas && doRequireAtlasSupport(configurationNode));
        }
        return false;
    }

    /**
     * Does the job require Basic Atlas authentification
     */
    public boolean doRequireBasicAtlasAuthentification(INode configurationNode){
        String distribution = ElementParameterParser.getValue(configurationNode,"__DISTRIBUTION__");
        String version = ElementParameterParser.getValue(configurationNode,"__MR_VERSION__");
        // spark compatibility
        if ((version == null) || ("".equals(version))) { 
            version = ElementParameterParser.getValue(configurationNode,"__SPARK_VERSION__");
        }
        try {
            org.talend.hadoop.distribution.component.MRComponent currentDistribution = (org.talend.hadoop.distribution.component.MRComponent)org.talend.hadoop.distribution.DistributionFactory.buildDistribution(distribution, version);
            return currentDistribution.doSupportBasicAtlasAuthentification();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Does the job require Cloudera Navigator lineage generation
     */
    public boolean doRequireClouderaNavigatorSupport(INode configurationNode){
        String distribution = ElementParameterParser.getValue(configurationNode,"__DISTRIBUTION__");
        String version = ElementParameterParser.getValue(configurationNode,"__MR_VERSION__");
        Boolean useClouderaNavigator = ElementParameterParser.getBooleanValue(configurationNode,"__USE_CLOUDERA_NAVIGATOR__");
        // spark compatibility
        if ((version == null) || ("".equals(version))) { 
            version = ElementParameterParser.getValue(configurationNode,"__SPARK_VERSION__");
        }
        try {
            org.talend.hadoop.distribution.component.MRComponent currentDistribution = (org.talend.hadoop.distribution.component.MRComponent)org.talend.hadoop.distribution.DistributionFactory.buildDistribution(distribution, version);
            return useClouderaNavigator && currentDistribution.doSupportClouderaNavigator();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Does the job require Atlas lineage generation
     */
    public boolean doRequireAtlasSupport(INode configurationNode){
        String distribution = ElementParameterParser.getValue(configurationNode,"__DISTRIBUTION__");
        String version = ElementParameterParser.getValue(configurationNode,"__MR_VERSION__");
        Boolean useAtlas = ElementParameterParser.getBooleanValue(configurationNode,"__USE_ATLAS__");
        // spark compatibility
        if ((version == null) || ("".equals(version))) { 
           version = ElementParameterParser.getValue(configurationNode,"__SPARK_VERSION__");
        }
        try {
            org.talend.hadoop.distribution.component.MRComponent currentDistribution = (org.talend.hadoop.distribution.component.MRComponent)org.talend.hadoop.distribution.DistributionFactory.buildDistribution(distribution, version);
            return useAtlas && currentDistribution.doSupportAtlas();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get Cloudera Navigator API version
     */
    public int getClouderaNavigatorAPIVersion(INode configurationNode){
        String distribution = ElementParameterParser.getValue(configurationNode,"__DISTRIBUTION__");
        String version = ElementParameterParser.getValue(configurationNode,"__MR_VERSION__");
        // spark compatibility
        if ((version == null) || ("".equals(version))) { 
           version = ElementParameterParser.getValue(configurationNode,"__SPARK_VERSION__");
        }
        try {
            org.talend.hadoop.distribution.component.MRComponent currentDistribution = (org.talend.hadoop.distribution.component.MRComponent)org.talend.hadoop.distribution.DistributionFactory.buildDistribution(distribution, version);
            return currentDistribution.getClouderaNavigatorAPIVersion();
        } catch (Exception e) {
            return 8;
        }
    }

    /**
    *
    * generates a Cloudera Navigator lineage creator
    *
    */
    public void generateClouderaNavigatorLinageCreator(INode configurationNode) {
        String usernameCN = ElementParameterParser.getValue(configurationNode,"__CLOUDERA_NAVIGATOR_USERNAME__");
        String urlCN = ElementParameterParser.getValue(configurationNode,"__CLOUDERA_NAVIGATOR_URL__");
        String urlMetadataCN = ElementParameterParser.getValue(configurationNode,"__CLOUDERA_NAVIGATOR_METADATA_URL__");
        String clientUrlCN = ElementParameterParser.getValue(configurationNode,"__CLOUDERA_NAVIGATOR_CLIENT_URL__");
        Boolean useAutocommit = ElementParameterParser.getBooleanValue(configurationNode,"__CLOUDERA_NAVIGATOR_AUTOCOMMIT__");
        Boolean disableSslValidation = ElementParameterParser.getBooleanValue(configurationNode,"__CLOUDERA_NAVIGATOR_DISABLE_SSL_VALIDATION__");
        int apiVersion = getClouderaNavigatorAPIVersion(configurationNode);

        if (ElementParameterParser.canEncrypt(configurationNode, "__CLOUDERA_NAVIGATOR_PASSWORD__")) {
            
    stringBuffer.append(TEXT_80);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(configurationNode, "__CLOUDERA_NAVIGATOR_PASSWORD__"));
    stringBuffer.append(TEXT_81);
    
        } else {
            
    stringBuffer.append(TEXT_82);
    stringBuffer.append( ElementParameterParser.getValue(configurationNode, "__CLOUDERA_NAVIGATOR_PASSWORD__"));
    stringBuffer.append(TEXT_83);
    
        }
        
    stringBuffer.append(TEXT_84);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(clientUrlCN);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(TEXT_87);
    stringBuffer.append(urlCN);
    stringBuffer.append(TEXT_88);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(urlMetadataCN);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(TEXT_91);
    stringBuffer.append(usernameCN);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(useAutocommit);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(disableSslValidation);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(apiVersion);
    stringBuffer.append(TEXT_98);
    
    }

    /**
    *
    * generates an Atlas lineage creator
    *
    */
    public void generateAtlasLinageCreator(INode configurationNode) {
        boolean doSetAtlasApplicationProperties = ElementParameterParser.getBooleanValue(configurationNode,"__SET_ATLAS_APPLICATION_PROPERTIES__");
        if(doSetAtlasApplicationProperties){
            String atlasApplicationPropertiesFilePath = ElementParameterParser.getValue(configurationNode,"__ATLAS_APPLICATION_PROPERTIES__");
            
    stringBuffer.append(TEXT_99);
    stringBuffer.append(atlasApplicationPropertiesFilePath);
    stringBuffer.append(TEXT_100);
    
        }
        String atlasURL = ElementParameterParser.getValue(configurationNode,"__ATLAS_URL__");
        if(doRequireBasicAtlasAuthentification(configurationNode)){
            String atlasUsername = ElementParameterParser.getValue(configurationNode,"__ATLAS_USERNAME__");
            String passwordFieldName = "__ATLAS_PASSWORD__";
            if (ElementParameterParser.canEncrypt(configurationNode, passwordFieldName)) {
                
    stringBuffer.append(TEXT_101);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(configurationNode, passwordFieldName));
    stringBuffer.append(TEXT_102);
    
            } else {
                
    stringBuffer.append(TEXT_103);
    stringBuffer.append( ElementParameterParser.getValue(configurationNode, passwordFieldName));
    stringBuffer.append(TEXT_104);
    
            }
            
    stringBuffer.append(TEXT_105);
    stringBuffer.append(atlasURL);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(atlasUsername);
    stringBuffer.append(TEXT_107);
    
        } else {
            
    stringBuffer.append(TEXT_108);
    stringBuffer.append(atlasURL);
    stringBuffer.append(TEXT_109);
    
        }
        
    stringBuffer.append(TEXT_110);
    
    }

    /**
    * returns the dieOnError value
    */
    public Boolean getDieOnError(INode configurationNode){
        if(doRequireClouderaNavigatorSupport(configurationNode)){
            return ElementParameterParser.getBooleanValue(configurationNode,"__CLOUDERA_NAVIGATOR_DIE_ON_ERROR__");
        } else if (doRequireAtlasSupport(configurationNode)){
            return ElementParameterParser.getBooleanValue(configurationNode,"__ATLAS_DIE_ON_ERROR__");
        }
        return false;
    }

    /**
     * Generate a Map containing the columns information.
     * The map contain the column name as the key, and the column type as the value.
     */
    public void generateColumnList(IMetadataTable metadataTable, String cid) {
        
    stringBuffer.append(TEXT_111);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_112);
    
        for (IMetadataColumn column: metadataTable.getListColumns()) {
            
    stringBuffer.append(TEXT_113);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_115);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_116);
    
        }
    }

    /**
     * Generate a Map containing the columns information.
     * The map contain the column name as the key, and the column type as the value.
     * This function will generate the full output mapping of a component, in order to display any output field.
     * 
     * If the component contain output link, the map will be  generate from these links,
     * otherwise the metadata of the component will be used.
     */
    public void generateColumnListFromOutputLink(INode node, String cid) {
        
    stringBuffer.append(TEXT_117);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_118);
    
        if ((node.getOutgoingConnections() != null)
            && (node.getOutgoingConnections().size() > 0)) {
            for (IConnection connection: node.getOutgoingConnections()) {
                if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                    for (IMetadataColumn column: connection.getMetadataTable().getListColumns()) {
                        
    stringBuffer.append(TEXT_119);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_121);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_122);
    
                    }
                }
            }
        } else  {
            for (IMetadataTable metadataTable: node.getMetadataList()) {
                for (IMetadataColumn column: metadataTable.getListColumns()) {
                    
    stringBuffer.append(TEXT_123);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_125);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_126);
    
                }
            }
        }
    }

    /**
     * Generate the code to call the method addDataset of rg.talend.cloudera.navigator.api.LineageCreator.
     * 
     * @param cid the cid of the current component
     * @param componentName name of the graphical component
     * @param folderPath The path to the folder containing data into HDFS
     * @param folderType The type of the folder, must be defined into com.cloudera.nav.sdk.model.entities.FileFormat
     */
    public void addDataset(String cid, String componentName, String folderPath, String folderType) {
        
    stringBuffer.append(TEXT_127);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_128);
    stringBuffer.append(componentName);
    stringBuffer.append(TEXT_129);
    stringBuffer.append(folderPath);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(folderType);
    stringBuffer.append(TEXT_131);
    
    }

    /**
     * Generate list of input nodes for a given component.
     * 
     * @param node the currentNode
     */
    public void generateInputNodeList(INode node) {
        
    stringBuffer.append(TEXT_132);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_133);
    
        java.util.List<IConnection> inputs = (java.util.List<IConnection>)node.getIncomingConnections();
        for (IConnection connection: inputs) {
            if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                
    stringBuffer.append(TEXT_134);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_135);
    stringBuffer.append(connection.getSource().getUniqueName());
    stringBuffer.append(TEXT_136);
    
            }
        }
    }

    /**
     * Generate list of output nodes for a given component.
     * 
     * @param node the currentNode
     */
    public void generateOutputNodeList(INode node) {
        
    stringBuffer.append(TEXT_137);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_138);
    
        java.util.List<IConnection> outputs = (java.util.List<IConnection>)node.getOutgoingConnections();
        for (IConnection connection: outputs) {
            if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                
    stringBuffer.append(TEXT_139);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_140);
    stringBuffer.append(connection.getTarget().getUniqueName());
    stringBuffer.append(TEXT_141);
    
            }
        }
    }

    /**
     * Generate the code to call the method addNodeToLineage of rg.talend.cloudera.navigator.api.LineageCreator.
     * 
     * @param cid the cid of the current component
     */
    public void addNodeToLineage(String cid) {
        
    stringBuffer.append(TEXT_142);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_146);
    
    }

}

    
    TalendLineageAPI talendLineageAPI = new TalendLineageAPI();
    if (talendLineageAPI.doRequireLineageSupport(sparkConfig)) {
        
    stringBuffer.append(TEXT_147);
    
    }
    
    stringBuffer.append(TEXT_148);
    
        // set checkpoint directory
        boolean useCheckpoint = ElementParameterParser.getBooleanValue(sparkConfig, "__USE_CHECKPOINT__");
        String checkpointDir = ElementParameterParser.getValue(sparkConfig, "__CHECKPOINT_DIR__");
        if(useCheckpoint){

    stringBuffer.append(TEXT_149);
    stringBuffer.append(checkpointDir);
    stringBuffer.append(TEXT_150);
    
        }
        if(useKrb) {
            if(((isCustom || sparkBatchDistrib != null && sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                
    stringBuffer.append(TEXT_151);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_152);
    stringBuffer.append(setMapRHadoopLogin ? mapRHadoopLogin : "\"kerberos\"");
    stringBuffer.append(TEXT_153);
    
            }
            if(useKeytab) {

    stringBuffer.append(TEXT_154);
    stringBuffer.append(keytabPrincipal);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(keytabPath);
    stringBuffer.append(TEXT_156);
    
            }
            if(((isCustom || sparkBatchDistrib != null && sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                
    stringBuffer.append(TEXT_157);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_159);
    
            }
        }  else {
            // Mapr ticket
            if(((isCustom || sparkBatchDistrib != null && sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                passwordFieldName = "__MAPRTICKET_PASSWORD__";
                
    stringBuffer.append(TEXT_160);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_161);
    
                if (setMapRHadoopLogin) {
                    
    stringBuffer.append(TEXT_162);
    stringBuffer.append(mapRHadoopLogin);
    stringBuffer.append(TEXT_163);
    
                } else {
                    
    stringBuffer.append(TEXT_164);
    
                }
                INode node = sparkConfig;
                
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_167);
    } else {
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_170);
    }
    stringBuffer.append(TEXT_171);
    
                if(sparkBatchDistrib.doSupportMaprTicketV52API()){
                    
    stringBuffer.append(TEXT_172);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_176);
    
                } else {
                    
    stringBuffer.append(TEXT_177);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_179);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_180);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_181);
    
                }
            }
        }

        if(stats && !isCloudDistribution) {

    stringBuffer.append(TEXT_182);
    
        }
        if(isLog4jEnabled && !isCloudDistribution) {

    stringBuffer.append(TEXT_183);
    
        }

    stringBuffer.append(TEXT_184);
    
        if(isLog4jEnabled) {

    stringBuffer.append(TEXT_185);
    
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
            
    stringBuffer.append(TEXT_186);
    stringBuffer.append(level.substring(0, 1).toUpperCase() + level.substring(1));
    stringBuffer.append(TEXT_187);
    
            }
            
    stringBuffer.append(TEXT_188);
    stringBuffer.append(level);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(logID);
    stringBuffer.append(TEXT_190);
    for(String message : messages){
    stringBuffer.append(TEXT_191);
    stringBuffer.append(message);
    stringBuffer.append(TEXT_192);
    }
    stringBuffer.append(TEXT_193);
    
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
    
    stringBuffer.append(TEXT_194);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_195);
    
       			 if(log4jEnabled){
       			 
    stringBuffer.append(TEXT_196);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_197);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_199);
    
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
                    
    stringBuffer.append(TEXT_200);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_201);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(password);
    stringBuffer.append(TEXT_203);
    
                }else{
                    String value = org.talend.core.model.utils.NodeUtil.getNormalizeParameterValue(node, ep);
                    
    stringBuffer.append(TEXT_204);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_205);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(value);
    stringBuffer.append(TEXT_207);
    
                }   
                
    stringBuffer.append(TEXT_208);
    stringBuffer.append(var("log4jParamters"));
    stringBuffer.append(TEXT_209);
    
            }
        }
		debug(var("log4jParamters"));
		
    stringBuffer.append(TEXT_210);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_211);
    
    }
    
    //no use for now, because we log the data by rowStruct
    public void traceData(String rowStruct, java.util.List<org.talend.core.model.metadata.IMetadataColumn> columnList, String nbline){
        if(log4jEnabled){
        
    stringBuffer.append(TEXT_212);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_213);
    
            for(org.talend.core.model.metadata.IMetadataColumn column : columnList){
                org.talend.core.model.metadata.types.JavaType javaType = org.talend.core.model.metadata.types.JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                String columnName = column.getLabel();
                boolean isPrimit = org.talend.core.model.metadata.types.JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
                if(isPrimit){
                
    stringBuffer.append(TEXT_214);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_215);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_217);
    
                }else{
                
    stringBuffer.append(TEXT_218);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_222);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_224);
    stringBuffer.append(rowStruct);
    stringBuffer.append(TEXT_225);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_226);
    
                }
                
    stringBuffer.append(TEXT_227);
    stringBuffer.append(var("log4jSb"));
    stringBuffer.append(TEXT_228);
    
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

    
            for (INode confNode : confNodes) {
                String componentName = confNode.getComponent().getName();
                if(componentName.endsWith("Configuration")) {

    stringBuffer.append(TEXT_229);
    stringBuffer.append(confNode.getUniqueName());
    stringBuffer.append(TEXT_230);
    
                    log = new LogUtil(confNode);
                    log.logCompSetting();
                }
            }
        }

        for (INode rootNode : rootNodes) {
            String componentName = rootNode.getComponent().getName();
            String uniqueName = rootNode.getUniqueName();
            if(rootNode.getOutgoingConnections().size() > 0 || "tJava".equals(componentName) || "tRunJob".equals(componentName) || "tMatchModel".equals(componentName) ||
              (componentName != null && componentName.startsWith("tHConvert")) || (componentName != null && componentName.startsWith("tHMap"))) {
            
    stringBuffer.append(TEXT_231);
    stringBuffer.append(TEXT_232);
    stringBuffer.append(rootNode.getUniqueName());
    stringBuffer.append(TEXT_233);
    
                    if(stats && !isCloudDistribution) {

    stringBuffer.append(TEXT_234);
    
                    }

    stringBuffer.append(TEXT_235);
    
                // Validate result of test cases
                if (isTestContainer) {
                    
    stringBuffer.append(TEXT_236);
    
                }
            }
        }
        if (talendLineageAPI.doRequireLineageSupport(sparkConfig)) {
            List<? extends INode> graphicalNodes = process.getGraphicalNodes();
            for (INode gNode: graphicalNodes) {
                if (gNode.isActivate()) {
                    talendLineageAPI.generateColumnListFromOutputLink(gNode, gNode.getUniqueName());
                    talendLineageAPI.generateInputNodeList(gNode);
                    talendLineageAPI.generateOutputNodeList(gNode);
                    talendLineageAPI.addNodeToLineage(gNode.getUniqueName());
                }
            }
            Boolean dieOnError = talendLineageAPI.getDieOnError(sparkConfig);
            
    stringBuffer.append(TEXT_237);
    stringBuffer.append(dieOnError);
    stringBuffer.append(TEXT_238);
    
        }
        
    stringBuffer.append(TEXT_239);
    
        String master = "\"local[*]\"";
        String deployMode = null;

        if(useStandaloneMode) {
            master = ElementParameterParser.getValue(sparkConfig, "__SPARK_HOST__");
        }

        if(useYarnClientMode) {
            master = org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) > 0 ? "\"yarn-client\"" : "\"yarn\"";
            deployMode = org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) > 0 ? null : "\"client\"";
        }

        if(useYarnClusterMode) {
            master = org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) > 0 ? "\"yarn-cluster\"" : "\"yarn\"";
            deployMode = org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) > 0 ? null : "\"cluster\"";
        }

        if(!useCloudLauncher) {

    stringBuffer.append(TEXT_240);
    stringBuffer.append(master);
    stringBuffer.append(TEXT_241);
     if(isTestContainer){ 
    stringBuffer.append(TEXT_242);
    stringBuffer.append(jobFolderName);
    stringBuffer.append(TEXT_243);
     } 
    stringBuffer.append(TEXT_244);
     if(isTestContainer){ 
    stringBuffer.append(TEXT_245);
     } 
    stringBuffer.append(TEXT_246);
    
            if (deployMode != null) {
                
    stringBuffer.append(TEXT_247);
    stringBuffer.append(deployMode);
    stringBuffer.append(TEXT_248);
     if(isLog4jEnabled) { 
    stringBuffer.append(TEXT_249);
     } 
    stringBuffer.append(TEXT_250);
    
            }
            
    stringBuffer.append(TEXT_251);
    
            // We need to look into the job command line to get the real path of the winutils.exe binary.
            String[] commandLine = new String[] {"<command>"};
            try {
                commandLine = ProcessorUtilities.getCommandLine("win32",true, processId, "",org.talend.designer.runprocess.IProcessor.NO_STATISTICS,org.talend.designer.runprocess.IProcessor.NO_TRACES, new String[]{});
            } catch (ProcessorException e) {
                e.printStackTrace();
            }
            java.util.List<String> jars = null;
            for (int j = 0; j < commandLine.length; j++) {
                if(commandLine[j].contains("jar")) {
                    jars = java.util.Arrays.asList(commandLine[j].split(";"));
                    break;
                }
            }
            if(jars != null) {
                for(int j=0; j<jars.size(); j++) {
                    if(jars.get(j).endsWith(".jar")) {

    stringBuffer.append(TEXT_252);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_SCRATCH_DIR__"));
    stringBuffer.append(TEXT_253);
    stringBuffer.append(jars.get(j).substring(0, jars.get(j).lastIndexOf("/") + 1));
    stringBuffer.append(TEXT_254);
    
                        break;
                    }
                }
            }

            // prepare Spark 2 jars to upload them to the cluster through spark.yarn.jars
            if (org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) <= 0
                    && sparkBatchDistrib != null && sparkBatchDistrib.getVersion() != null
                    && sparkBatchDistrib.generateSparkJarsPaths(jars) != null && !sparkBatchDistrib.generateSparkJarsPaths(jars).isEmpty()){
                
    stringBuffer.append(TEXT_255);
    stringBuffer.append(sparkBatchDistrib.generateSparkJarsPaths(jars));
    stringBuffer.append(TEXT_256);
     if(isLog4jEnabled) { 
    stringBuffer.append(TEXT_257);
     } 
    stringBuffer.append(TEXT_258);
     if(isLog4jEnabled) { 
    stringBuffer.append(TEXT_259);
     } 
    stringBuffer.append(TEXT_260);
    
            }

    stringBuffer.append(TEXT_261);
    
        }

        if(!useLocalMode && !useCloudLauncher) { // If the spark mode is not local and not executed through livy.
            boolean defineDriverHost = "true".equals(ElementParameterParser.getValue(sparkConfig, "__DEFINE_SPARK_DRIVER_HOST__"));
            if(defineDriverHost) {
                String driverHost = ElementParameterParser.getValue(sparkConfig, "__SPARK_DRIVER_HOST__");
                if(driverHost != null && !"".equals(driverHost)) {

    stringBuffer.append(TEXT_262);
    stringBuffer.append(driverHost);
    stringBuffer.append(TEXT_263);
    stringBuffer.append(driverHost);
    stringBuffer.append(TEXT_264);
    
                }
            }
            if(useStandaloneMode) {
                String sparkHome = ElementParameterParser.getValue(sparkConfig, "__SPARK_HOME__");

    stringBuffer.append(TEXT_265);
    stringBuffer.append(sparkHome);
    stringBuffer.append(TEXT_266);
    
            } else if(useYarnMode) {
                // Set the YARN parameters in the SparkConf
                String resourceManager = ElementParameterParser.getValue(sparkConfig, "__RESOURCE_MANAGER__");
                boolean setResourceManagerSchedulerAddress = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SET_SCHEDULER_ADDRESS__"));
                boolean setJobHistoryAddress = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SET_JOBHISTORY_ADDRESS__"));
                boolean setStagingDirectory = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SET_STAGING_DIRECTORY__"));
                boolean setMemory = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SET_MEMORY__"));

    stringBuffer.append(TEXT_267);
    stringBuffer.append(sparkBatchDistrib.getYarnApplicationClasspath());
    stringBuffer.append(TEXT_268);
    stringBuffer.append(resourceManager);
    stringBuffer.append(TEXT_269);
    if(setResourceManagerSchedulerAddress) { 
    stringBuffer.append(TEXT_270);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__RESOURCEMANAGER_SCHEDULER_ADDRESS__"));
    stringBuffer.append(TEXT_271);
     } 
    if(setJobHistoryAddress) { 
    stringBuffer.append(TEXT_272);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__JOBHISTORY_ADDRESS__"));
    stringBuffer.append(TEXT_273);
     } 
    if(setStagingDirectory) { 
    stringBuffer.append(TEXT_274);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__STAGING_DIRECTORY__"));
    stringBuffer.append(TEXT_275);
     } 
    if(setMemory) { 
    stringBuffer.append(TEXT_276);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__MAPREDUCE_MAP_MEMORY_MB__"));
    stringBuffer.append(TEXT_277);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__MAPREDUCE_REDUCE_MEMORY_MB__"));
    stringBuffer.append(TEXT_278);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__YARN_APP_MAPREDUCE_AM_RESOURCE_MB__"));
    stringBuffer.append(TEXT_279);
     } 
    
                if(sparkUseKrb && (isCustom || sparkBatchDistrib.doSupportKerberos())) {

    stringBuffer.append(TEXT_280);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__RESOURCEMANAGER_PRINCIPAL__"));
    stringBuffer.append(TEXT_281);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__JOBHISTORY_PRINCIPAL__"));
    stringBuffer.append(TEXT_282);
    
                    if(((isCustom || sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                        
    stringBuffer.append(TEXT_283);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_284);
    stringBuffer.append(setMapRHadoopLogin ? mapRHadoopLogin : "\"kerberos\"");
    stringBuffer.append(TEXT_285);
    
                    }
                    boolean sparkUseKeytab = "true".equals(ElementParameterParser.getValue(sparkConfig, "__USE_KEYTAB__"));
                    if(sparkUseKeytab) {

    stringBuffer.append(TEXT_286);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__PRINCIPAL__"));
    stringBuffer.append(TEXT_287);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__KEYTAB_PATH__"));
    stringBuffer.append(TEXT_288);
    
                    }
                    if(((isCustom || sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                        
    stringBuffer.append(TEXT_289);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_291);
    
                    }
                } else if(!useCloudLauncher) {
                    // Mapr ticket
                    if(((isCustom || sparkBatchDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                        passwordFieldName = "__MAPRTICKET_PASSWORD__";
                        
    stringBuffer.append(TEXT_292);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_293);
    
                        if (setMapRHadoopLogin) {
                            
    stringBuffer.append(TEXT_294);
    stringBuffer.append(mapRHadoopLogin);
    stringBuffer.append(TEXT_295);
    
                        } else {
                            
    stringBuffer.append(TEXT_296);
    
                        }
                        INode node = sparkConfig;
                        
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_297);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_298);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_299);
    } else {
    stringBuffer.append(TEXT_300);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_301);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_302);
    }
    
                        if(sparkBatchDistrib.doSupportMaprTicketV52API()){
                            
    stringBuffer.append(TEXT_303);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_304);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_305);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_306);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_307);
    
                        } else {
                            
    stringBuffer.append(TEXT_308);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_309);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_310);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_311);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_312);
    
                        }
                    }
                    String sparkUsername = ElementParameterParser.getValue(sparkConfig, "__USERNAME__");
                    if(sparkUsername != null) {
                        if(username != null) {

    stringBuffer.append(TEXT_313);
    stringBuffer.append(sparkUsername);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_315);
    
                        }
                        if(!"".equals(sparkUsername)) {

    stringBuffer.append(TEXT_316);
    stringBuffer.append(sparkUsername);
    stringBuffer.append(TEXT_317);
    stringBuffer.append(sparkUsername);
    stringBuffer.append(TEXT_318);
    
                        }
                    }
                }
            }
        } // End of: If the spark mode is not local.

        boolean defineHadoopHomeDir = "true".equals(ElementParameterParser.getValue(sparkConfig, "__DEFINE_HADOOP_HOME_DIR__"));
        if(defineHadoopHomeDir && !useCloudLauncher) {
        
    stringBuffer.append(TEXT_319);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__HADOOP_HOME_DIR__"));
    stringBuffer.append(TEXT_320);
    
        }

        if(tuningProperties) {
            // Set Web UI port
            if(setWebuiPort) {

    stringBuffer.append(TEXT_321);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__WEB_UI_PORT__"));
    stringBuffer.append(TEXT_322);
    
            }

    stringBuffer.append(TEXT_323);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_EXECUTOR_MEM__"));
    stringBuffer.append(TEXT_324);
    
            // executor memory overhead is a YARN only parameter
            boolean setExecutorMemoryOverhead = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SET_SPARK_EXECUTOR_MEM_OVERHEAD__"));
            if(setExecutorMemoryOverhead && (useYarnClientMode || useYarnClusterMode)) {

    stringBuffer.append(TEXT_325);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_EXECUTOR_MEM_OVERHEAD__"));
    stringBuffer.append(TEXT_326);
    
            }

            if(useStandaloneMode || useYarnClusterMode) {

    stringBuffer.append(TEXT_327);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_DRIVER_CORES__"));
    stringBuffer.append(TEXT_328);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_DRIVER_MEM__"));
    stringBuffer.append(TEXT_329);
    
            } else if(useYarnClientMode) {
            	// application master tuning properties
            	boolean setApplicationMasterTuningProperties = "true".equals(ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_AM_SETTINGS_CHECK__"));
            	if(setApplicationMasterTuningProperties) {

    stringBuffer.append(TEXT_330);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_AM_CORES__"));
    stringBuffer.append(TEXT_331);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_AM_MEM__"));
    stringBuffer.append(TEXT_332);
    
            	}
            }
            if(setExecutorCores && !useLocalMode) {

    stringBuffer.append(TEXT_333);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_EXECUTOR_CORES__"));
    stringBuffer.append(TEXT_334);
    
            }

            if(useYarnMode) {
                String allocationMode = ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_ALLOC_TYPE__");
                // we do nothing in AUTO mode
                if("FIXED".equalsIgnoreCase(allocationMode)) {

    stringBuffer.append(TEXT_335);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_EXECUTOR_INSTANCES__"));
    stringBuffer.append(TEXT_336);
    
                } else if("DYNAMIC".equalsIgnoreCase(allocationMode) && (isCustom  || sparkBatchDistrib != null && sparkBatchDistrib.doSupportDynamicMemoryAllocation())) {

    stringBuffer.append(TEXT_337);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_DYN_INIT__"));
    stringBuffer.append(TEXT_338);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_DYN_MIN__"));
    stringBuffer.append(TEXT_339);
    
                    String dynMaxValue = ElementParameterParser.getValue(sparkConfig, "__SPARK_YARN_DYN_MAX__");
                    if(dynMaxValue.contains("MAX")) {

    stringBuffer.append(TEXT_340);
    
                    }
                    else {

    stringBuffer.append(TEXT_341);
    stringBuffer.append(dynMaxValue);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(dynMaxValue);
    stringBuffer.append(TEXT_343);
    
                    }

    stringBuffer.append(TEXT_344);
    
                }
            }

            // The broadcast factory is unused in Spark 2.
            if (org.talend.hadoop.distribution.ESparkVersion.SPARK_2_0.compareTo(sparkVersion) > 0) {
                String broadcastFactory = ElementParameterParser.getValue(sparkConfig, "__SPARK_BROADCAST_FACTORY__");
                // we do nothing in auto mode
                if("TORRENT".equalsIgnoreCase(broadcastFactory)) {

    stringBuffer.append(TEXT_345);
    
                } else if ("HTTP".equalsIgnoreCase(broadcastFactory)) {

    stringBuffer.append(TEXT_346);
    
                }
            }

            boolean customizeSparkSerializer = "true".equals(ElementParameterParser.getValue(sparkConfig, "__CUSTOMIZE_SPARK_SERIALIZER__"));
            if(customizeSparkSerializer) {

    stringBuffer.append(TEXT_347);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_SERIALIZER__"));
    stringBuffer.append(TEXT_348);
    
            }
        } // end set of tuning properties

        // SPARK LOGGING / HISTORY parameters
        // These parameters are valied for YARN and mesos
        boolean enableSparkEventLogging = "true".equals(ElementParameterParser.getValue(sparkConfig, "__ENABLE_SPARK_EVENT_LOGGING__"));
        if(enableSparkEventLogging && (useYarnClientMode || useYarnClusterMode)){

    stringBuffer.append(TEXT_349);
    
            boolean compressSparkEventLogs = "true".equals(ElementParameterParser.getValue(sparkConfig, "__COMPRESS_SPARK_EVENT_LOGS__"));
            if(compressSparkEventLogs){

    stringBuffer.append(TEXT_350);
    
            }

    stringBuffer.append(TEXT_351);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_EVENT_LOG_DIR__"));
    stringBuffer.append(TEXT_352);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARKHISTORY_ADDRESS__"));
    stringBuffer.append(TEXT_353);
    
        }

        if(useLocalMode) {
            // Scratch dir must be absolute. System.getProperty("user.dir")
            
    stringBuffer.append(TEXT_354);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_SCRATCH_DIR__"));
    stringBuffer.append(TEXT_355);
    
        }

        // Advanced properties
        for(Map<String, String> property : sparkAdvancedProperties){

    stringBuffer.append(TEXT_356);
    stringBuffer.append(property.get("PROPERTY"));
    stringBuffer.append(TEXT_357);
    stringBuffer.append(property.get("VALUE"));
    stringBuffer.append(TEXT_358);
    
        }

        // Set hdp.version for driver & application master
        boolean setHdpVersion = ElementParameterParser.getBooleanValue(sparkConfig, "__SET_HDP_VERSION__");
        String hdpVersion = ElementParameterParser.getValue(sparkConfig, "__HDP_VERSION__");
        if(setHdpVersion && hdpVersion != null){

    stringBuffer.append(TEXT_359);
    stringBuffer.append(hdpVersion);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(hdpVersion);
    stringBuffer.append(TEXT_361);
    
            }

    stringBuffer.append(TEXT_362);
     if(!useCloudLauncher) { 
    stringBuffer.append(TEXT_363);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__SPARK_SCRATCH_DIR__"));
    stringBuffer.append(TEXT_364);
     } 
    stringBuffer.append(TEXT_365);
    



    // This part of code is generated only if the used distribution execute Spark jobs through the Spark Job Server. ONly used by HD Insight currently.
    if(isExecutedThroughSparkJobServer) {

    stringBuffer.append(TEXT_366);
    
            passwordFieldName = "__HDINSIGHT_PASSWORD__";
            if (ElementParameterParser.canEncrypt(sparkConfig, passwordFieldName)) {

    stringBuffer.append(TEXT_367);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_368);
    
            } else {

    stringBuffer.append(TEXT_369);
    stringBuffer.append( ElementParameterParser.getValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_370);
    
            }

    stringBuffer.append(TEXT_371);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__HDINSIGHT_ENDPOINT__"));
    stringBuffer.append(TEXT_372);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__HDINSIGHT_USERNAME__"));
    stringBuffer.append(TEXT_373);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_374);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_375);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_376);
    
    }

    if(isExecutedThroughLivy) {

    stringBuffer.append(TEXT_377);
    
            passwordFieldName = "__HDINSIGHT_PASSWORD__";
            if (ElementParameterParser.canEncrypt(sparkConfig, passwordFieldName)) {

    stringBuffer.append(TEXT_378);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_379);
    
            } else {

    stringBuffer.append(TEXT_380);
    stringBuffer.append( ElementParameterParser.getValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_381);
    
            }

            passwordFieldName = "__WASB_PASSWORD__";
            if (ElementParameterParser.canEncrypt(sparkConfig, passwordFieldName)) {

    stringBuffer.append(TEXT_382);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_383);
    
            } else {

    stringBuffer.append(TEXT_384);
    stringBuffer.append( ElementParameterParser.getValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_385);
    
            }

    stringBuffer.append(TEXT_386);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__WASB_USERNAME__"));
    stringBuffer.append(TEXT_387);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__WASB_CONTAINER__"));
    stringBuffer.append(TEXT_388);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__HDINSIGHT_USERNAME__"));
    stringBuffer.append(TEXT_389);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_390);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_391);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_392);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__LIVY_HOST__"));
    stringBuffer.append(TEXT_393);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__LIVY_PORT__"));
    stringBuffer.append(TEXT_394);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__REMOTE_FOLDER__"));
    stringBuffer.append(TEXT_395);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__LIVY_USERNAME__"));
    stringBuffer.append(TEXT_396);
    
                if(tuningProperties) {

    stringBuffer.append(TEXT_397);
    
                    if(setExecutorCores && !useLocalMode) {

    stringBuffer.append(TEXT_398);
    
                    }

    stringBuffer.append(TEXT_399);
    
                }

    stringBuffer.append(TEXT_400);
    
    } // end of livy mode


    if(isGoogleDataprocDistribution) {

    stringBuffer.append(TEXT_401);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__GOOGLE_CLUSTER_ID__"));
    stringBuffer.append(TEXT_402);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__GOOGLE_REGION__"));
    stringBuffer.append(TEXT_403);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__GOOGLE_PROJECT_ID__"));
    stringBuffer.append(TEXT_404);
    
						if(ElementParameterParser.getBooleanValue(sparkConfig, "__DEFINE_PATH_TO_GOOGLE_CREDENTIALS__")) {

    stringBuffer.append(TEXT_405);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__PATH_TO_GOOGLE_CREDENTIALS__"));
    stringBuffer.append(TEXT_406);
    
						}

    stringBuffer.append(TEXT_407);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__GOOGLE_JARS_BUCKET__"));
    stringBuffer.append(TEXT_408);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_409);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_410);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_411);
    
        	boolean customLogLevel  = "true".equals(ElementParameterParser.getValue(process, "__LOG4J_RUN_ACTIVATE__"));
        	if(customLogLevel){
        	String runLevel = ElementParameterParser.getValue(process, "__LOG4J_RUN_LEVEL__").toUpperCase();
        	
    stringBuffer.append(TEXT_412);
    stringBuffer.append(runLevel);
    stringBuffer.append(TEXT_413);
    }
    stringBuffer.append(TEXT_414);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_415);
    }
    stringBuffer.append(TEXT_416);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_417);
    }
    stringBuffer.append(TEXT_418);
    
    } // end of dataproc

    if(isAltusDistribution) {
        
    stringBuffer.append(TEXT_419);
    //Sync with name rule of Job server.
            passwordFieldName = "__ALTUS_S3_SECRET_KEY__";
            
    if (ElementParameterParser.canEncrypt(sparkConfig, passwordFieldName)) {
    stringBuffer.append(TEXT_420);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_421);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_422);
    } else {
    stringBuffer.append(TEXT_423);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_424);
    stringBuffer.append( ElementParameterParser.getValue(sparkConfig, passwordFieldName));
    stringBuffer.append(TEXT_425);
    }
    stringBuffer.append(TEXT_426);
     if (ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_REUSE_CLUSTER__")) {
    stringBuffer.append(TEXT_427);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLUSTER_NAME__"));
    stringBuffer.append(TEXT_428);
    stringBuffer.append(ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_SET_CREDENTIALS__"));
    stringBuffer.append(TEXT_429);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_ACCESS_KEY__"));
    stringBuffer.append(TEXT_430);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_SECRET_KEY__"));
    stringBuffer.append(TEXT_431);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_S3_ACCESS_KEY__"));
    stringBuffer.append(TEXT_432);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_433);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_S3_REGION__"));
    stringBuffer.append(TEXT_434);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_BUCKET_NAME__"));
    stringBuffer.append(TEXT_435);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_JARS_BUCKET__"));
    stringBuffer.append(TEXT_436);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLI_PATH__"));
    stringBuffer.append(TEXT_437);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_438);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_439);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_440);
    
                    boolean customLogLevel  = "true".equals(ElementParameterParser.getValue(process, "__LOG4J_RUN_ACTIVATE__"));
                    if(customLogLevel){
                        String runLevel = ElementParameterParser.getValue(process, "__LOG4J_RUN_LEVEL__").toUpperCase();
                        
    stringBuffer.append(TEXT_441);
    stringBuffer.append(runLevel);
    stringBuffer.append(TEXT_442);
    }
    stringBuffer.append(TEXT_443);
     } else { 
    stringBuffer.append(TEXT_444);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLUSTER_NAME__"));
    stringBuffer.append(TEXT_445);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_ENVIRONMENT_NAME__"));
    stringBuffer.append(TEXT_446);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLOUD_PROVIDER__"));
    stringBuffer.append(TEXT_447);
    stringBuffer.append(ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_DELETE_AFTER_EXECUTION__"));
    stringBuffer.append(TEXT_448);
    stringBuffer.append(ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_SET_CREDENTIALS__"));
    stringBuffer.append(TEXT_449);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_ACCESS_KEY__"));
    stringBuffer.append(TEXT_450);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_SECRET_KEY__"));
    stringBuffer.append(TEXT_451);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_S3_ACCESS_KEY__"));
    stringBuffer.append(TEXT_452);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_453);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_S3_REGION__"));
    stringBuffer.append(TEXT_454);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_BUCKET_NAME__"));
    stringBuffer.append(TEXT_455);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_JARS_BUCKET__"));
    stringBuffer.append(TEXT_456);
    stringBuffer.append(ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_USE_CUSTOM_JSON__"));
    stringBuffer.append(TEXT_457);
    
                    if (ElementParameterParser.getBooleanValue(sparkConfig, "__ALTUS_USE_CUSTOM_JSON__")) {
                        String lineSeparator = (String) java.security.AccessController.doPrivileged(
                                new sun.security.action.GetPropertyAction("line.separator"));
                        String altusJson = ElementParameterParser.getValue(sparkConfig, "__ALTUS_CUSTOM_JSON__");
                        StringBuilder altusJsonFormatted = new StringBuilder();
                        for(String item : altusJson.split(lineSeparator))
                            altusJsonFormatted.append("\"").append(item.replace("\"", "\\\"")).append("\" +");
                        // Remove trailing plus.
                        if (altusJsonFormatted.length() > 0)
                            altusJsonFormatted.setLength(altusJsonFormatted.length() - 1);
                        
    stringBuffer.append(TEXT_458);
    stringBuffer.append(altusJsonFormatted);
    stringBuffer.append(TEXT_459);
     } else { 
    stringBuffer.append(TEXT_460);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_INSTANCE_TYPE__"));
    stringBuffer.append(TEXT_461);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_WORKER_NODE__"));
    stringBuffer.append(TEXT_462);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_SSH_PRIVATE_KEY__"));
    stringBuffer.append(TEXT_463);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLOUDERA_MANAGER_USERNAME__"));
    stringBuffer.append(TEXT_464);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLOUDERA_MANAGER_PASSWORD__"));
    stringBuffer.append(TEXT_465);
     } 
    stringBuffer.append(TEXT_466);
    stringBuffer.append(ElementParameterParser.getValue(sparkConfig, "__ALTUS_CLI_PATH__"));
    stringBuffer.append(TEXT_467);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_468);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_469);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_470);
    
                    boolean customLogLevel  = "true".equals(ElementParameterParser.getValue(process, "__LOG4J_RUN_ACTIVATE__"));
                    if(customLogLevel){
                        String runLevel = ElementParameterParser.getValue(process, "__LOG4J_RUN_LEVEL__").toUpperCase();
                        
    stringBuffer.append(TEXT_471);
    stringBuffer.append(runLevel);
    stringBuffer.append(TEXT_472);
    }
    stringBuffer.append(TEXT_473);
    }
    stringBuffer.append(TEXT_474);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_475);
    }
    stringBuffer.append(TEXT_476);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_477);
    }
    stringBuffer.append(TEXT_478);
    
    } // end of altus

    
    // job wide SSL configurations
    // Find the tSetKeystore node
    List<INode> tSetKeystoreNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tSetKeystore")) {
        tSetKeystoreNodes.add(pNode);
    }
    // We don't want to generate the job wide SSL configurations unless there's a tMongoDBConfiguration
    // to avoid breaking existing component specific SSL configurations
    // YOu can add other components beside tMongoDBConfiguration
    // Find the tMongoDBConfiguration node
    List<INode> tMongoDBConfigurationSSLNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tMongoDBConfiguration")) {
        boolean usesSSL = ElementParameterParser.getBooleanValue(pNode, "__USE_SSL__");
        if(usesSSL) tMongoDBConfigurationSSLNodes.add(pNode);
    }

    stringBuffer.append(TEXT_479);
    
        // Generate job wide ssl configuration when :
        if(tSetKeystoreNodes.size() > 0){
            // Use the configurations of the first keystore
            INode node = tSetKeystoreNodes.get(0);
            String trustStoreType = ElementParameterParser.getValue(node,"__TRUSTSTORE_TYPE__");
            String trustStorePath = ElementParameterParser.getValue(node,"__SSL_TRUSTSERVER_TRUSTSTORE__");
            String trustStorePwd = ElementParameterParser.getValue(node,"__SSL_TRUSTSERVER_PASSWORD__");
            boolean needClientAuth = "true".equals(ElementParameterParser.getValue(node,"__NEED_CLIENT_AUTH__"));
            String keyStoreType = ElementParameterParser.getValue(node,"__KEYSTORE_TYPE__");
            String keyStorePath = ElementParameterParser.getValue(node,"__SSL_KEYSTORE__");
            String keyStorePwd = ElementParameterParser.getValue(node,"__SSL_KEYSTORE_PASSWORD__");
            boolean verifyName = ("true").equals(ElementParameterParser.getValue(node,"__VERIFY_NAME__"));

            if(!verifyName){

    stringBuffer.append(TEXT_480);
    
            }

    stringBuffer.append(TEXT_481);
    stringBuffer.append(trustStorePath );
    stringBuffer.append(TEXT_482);
    stringBuffer.append(trustStoreType );
    stringBuffer.append(TEXT_483);
    
            passwordFieldName = "__SSL_TRUSTSERVER_PASSWORD__";

    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_484);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_485);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_486);
    } else {
    stringBuffer.append(TEXT_487);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_488);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_489);
    }
    stringBuffer.append(TEXT_490);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_491);
    
            if(needClientAuth){

    stringBuffer.append(TEXT_492);
    stringBuffer.append(keyStorePath );
    stringBuffer.append(TEXT_493);
    stringBuffer.append(keyStoreType);
    stringBuffer.append(TEXT_494);
    
                passwordFieldName = "__SSL_KEYSTORE_PASSWORD__";
                if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {

    stringBuffer.append(TEXT_495);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_496);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_497);
    
                } else {

    stringBuffer.append(TEXT_498);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_499);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_500);
    
                }

    stringBuffer.append(TEXT_501);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_502);
    
            }else{

    stringBuffer.append(TEXT_503);
    
            }
        } else {
            // Do not any Generate SSL configurations
            
    stringBuffer.append(TEXT_504);
    
        }

    stringBuffer.append(TEXT_505);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_506);
    stringBuffer.append(jobClassPackage);
    stringBuffer.append(TEXT_507);
    
                for(IContextParameter ctxParam :params){
                    String typeToGenerate = "String";
                    if(ctxParam.getType().equals("id_List Of Value") || ctxParam.getType().equals("id_File") || ctxParam.getType().equals("id_Directory")){
                        typeToGenerate = "String";
                    }else{
                        typeToGenerate = JavaTypesManager.getTypeToGenerate(ctxParam.getType(),true);
                    }
                    
    stringBuffer.append(TEXT_508);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_509);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_510);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_511);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_512);
    
                }
                
    stringBuffer.append(TEXT_513);
    
        if (talendLineageAPI.doRequireLineageSupport(sparkConfig)) {
            if(talendLineageAPI.doRequireClouderaNavigatorSupport(sparkConfig)){
                
    stringBuffer.append(TEXT_514);
    
                talendLineageAPI.generateClouderaNavigatorLinageCreator(sparkConfig);
            } else if(talendLineageAPI.doRequireAtlasSupport(sparkConfig)){
                
    stringBuffer.append(TEXT_515);
    
                talendLineageAPI.generateAtlasLinageCreator(sparkConfig);
            }
        }
        
    stringBuffer.append(TEXT_516);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_517);
    stringBuffer.append(jobClassPackage);
    stringBuffer.append(TEXT_518);
    
                if(!useLocalMode) {

    stringBuffer.append(TEXT_519);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_520);
    stringBuffer.append(jobClassPackage);
    stringBuffer.append(TEXT_521);
    
                }

    stringBuffer.append(TEXT_522);
    
            for(IContextParameter ctxParam : params){
            
    stringBuffer.append(TEXT_523);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_524);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_525);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_526);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_527);
    
            }
            
    stringBuffer.append(TEXT_528);
    
    if(isExecutedThroughLivy) {

    stringBuffer.append(TEXT_529);
    
    } else if (isGoogleDataprocDistribution) {

    stringBuffer.append(TEXT_530);
    
    } else if (isAltusDistribution) {

    stringBuffer.append(TEXT_531);
    
    }

    stringBuffer.append(TEXT_532);
     if(useCloudLauncher) { 
    stringBuffer.append(TEXT_533);
     } 
    stringBuffer.append(TEXT_534);
     if(useCloudLauncher) { 
    stringBuffer.append(TEXT_535);
     } 
    stringBuffer.append(TEXT_536);
     if(useCloudLauncher) { 
    stringBuffer.append(TEXT_537);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_538);
    }
    stringBuffer.append(TEXT_539);
     } else if(isExecutedThroughSparkJobServer) { 
    stringBuffer.append(TEXT_540);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_541);
    }
    stringBuffer.append(TEXT_542);
     } 
    stringBuffer.append(TEXT_543);
    stringBuffer.append(TEXT_544);
    return stringBuffer.toString();
  }
}
