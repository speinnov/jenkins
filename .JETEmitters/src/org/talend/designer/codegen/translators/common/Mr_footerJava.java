package org.talend.designer.codegen.translators.common;

import org.talend.core.model.process.IProcess;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.CorePlugin;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import java.util.Vector;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import org.talend.core.model.process.IContextParameter;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.utils.JavaResourcesHelper;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.runprocess.ProcessorException;
import org.talend.designer.runprocess.ProcessorUtilities;

public class Mr_footerJava
{
  protected static String nl;
  public static synchronized Mr_footerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Mr_footerJava result = new Mr_footerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        throw new java.lang.RuntimeException(\"A Map/Reduce job can't have more than 1 generated tHadoopConfManager in the process.\");";
  protected final String TEXT_2 = NL + NL + "    public String resuming_logs_dir_path = null;" + NL + "    public String resuming_checkpoint_path = null;" + NL + "    public String parent_part_launcher = null;" + NL + "    private String resumeEntryMethodName = null;" + NL + "    ResumeUtil resumeUtil = null;" + NL + "    private boolean globalResumeTicket = false;" + NL + "" + NL + "    public String pid = \"0\";" + NL + "    public String rootPid = null;" + NL + "    public String fatherPid = null;" + NL + "" + NL + "    public boolean isChildJob = false;" + NL + "" + NL + "    private MRRunStat runStat = new MRRunStat();" + NL + "    // portStats is null, it means don't execute the statistics" + NL + "    public Integer portStats = null;" + NL + "    public String clientHost;" + NL + "    public String defaultClientHost = \"localhost\";" + NL + "    private boolean execStat = true;" + NL + "    public String log4jLevel = \"\";" + NL;
  protected final String TEXT_3 = NL + "        private java.util.List<String> cloudApiArgs = new java.util.ArrayList<>();" + NL + "        private String cloudLibjars = \"\";";
  protected final String TEXT_4 = NL + "    public String contextStr = \"";
  protected final String TEXT_5 = "\";" + NL + "    public boolean isDefaultContext = true;" + NL + "" + NL + "    private java.util.Properties context_param = new java.util.Properties();" + NL + "    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();" + NL + "" + NL + "    public String status= \"\";" + NL + "" + NL + "    private String mr_temp_dir = \"\";" + NL + "" + NL + "" + NL + "    public static void main(String[] args){" + NL + "        final ";
  protected final String TEXT_6 = " ";
  protected final String TEXT_7 = "Class = new ";
  protected final String TEXT_8 = "();" + NL + "        int exitCode = ";
  protected final String TEXT_9 = "Class.runJobInTOS(args);" + NL + "        System.exit(exitCode);" + NL + "    }" + NL + "" + NL + "    public String[][] runJob(String[] args){" + NL + "        int exitCode = runJobInTOS(args);" + NL + "        String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };" + NL + "        return bufferValue;" + NL + "    }" + NL + "" + NL + "    public int runJobInTOS (String[] args) {" + NL + "    \targs = normalizeArgs(args);" + NL + "" + NL + "        String lastStr = \"\";" + NL + "        for (String arg : args) {" + NL + "            if (arg.equalsIgnoreCase(\"--context_param\")) {" + NL + "                lastStr = arg;" + NL + "            } else if (lastStr.equals(\"\")) {" + NL + "                evalParam(arg);" + NL + "            } else {" + NL + "                evalParam(lastStr + \" \" + arg);" + NL + "                lastStr = \"\";" + NL + "            }" + NL + "        }" + NL;
  protected final String TEXT_10 = NL + "            if(!\"\".equals(log4jLevel)){" + NL + "                if(\"trace\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.TRACE);" + NL + "                }else if(\"debug\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.DEBUG);" + NL + "                }else if(\"info\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.INFO);" + NL + "                }else if(\"warn\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.WARN);" + NL + "                }else if(\"error\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.ERROR);" + NL + "                }else if(\"fatal\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.FATAL);" + NL + "                }else if (\"off\".equalsIgnoreCase(log4jLevel)){" + NL + "                    log.setLevel(org.apache.log4j.Level.OFF);" + NL + "                }" + NL + "                org.apache.log4j.Logger.getRootLogger().setLevel(log.getLevel());" + NL + "            }" + NL + "            log.info(\"TalendJob: '";
  protected final String TEXT_11 = "' - Start.\");";
  protected final String TEXT_12 = NL + "\t\t\ttry{" + NL + "\t\t\t\tint exitCode = 0;";
  protected final String TEXT_13 = NL + "\t\t\t\t\tif(!isTempletonCall(args)) {" + NL + "                        exitCode = runTempletonJob(args);";
  protected final String TEXT_14 = NL + "                    if(!isGoogleDataprocCall(args)) {" + NL + "                        exitCode = runCloudJob(args);";
  protected final String TEXT_15 = NL + "\t\t\t\t\t} else {";
  protected final String TEXT_16 = NL + "            Configuration conf = new Configuration();" + NL + "" + NL + "            if(runInRuntime && args.length>0){" + NL + "                conf.set(\"tmpjars\",args[args.length-1]);" + NL + "            }" + NL;
  protected final String TEXT_17 = NL + "                initContext();";
  protected final String TEXT_18 = NL + "                    ";
  protected final String TEXT_19 = "Process(null);";
  protected final String TEXT_20 = NL + "                initMapReduceJob(conf);";
  protected final String TEXT_21 = NL + NL + "            exitCode = ToolRunner.run(conf, this, args);";
  protected final String TEXT_22 = NL + "\t\t\t\t\t}";
  protected final String TEXT_23 = NL + "\t\t\t\treturn exitCode;" + NL + "\t\t\t}catch(Exception ex){" + NL + "\t\t\t\tex.printStackTrace();" + NL + "\t\t\t\treturn 1;" + NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_24 = NL + "    }";
  protected final String TEXT_25 = NL + "\t\tpublic int runTempletonJob(String[] args) throws Exception {" + NL + "\t\t\tString lastStr = \"\";" + NL + "\t\t\tfor (String arg : args) {" + NL + "\t\t\t\tif (arg.equalsIgnoreCase(\"--context_param\")) {" + NL + "\t\t\t\t\tlastStr = arg;" + NL + "\t\t\t\t} else if (lastStr.equals(\"\")) {" + NL + "\t\t\t\t\tevalParam(arg);" + NL + "\t\t\t\t} else {" + NL + "\t\t\t\t\tevalParam(lastStr + \" \" + arg);" + NL + "\t\t\t\t\tlastStr = \"\";" + NL + "\t\t\t\t}" + NL + "\t\t\t}";
  protected final String TEXT_26 = NL + "                initContext();";
  protected final String TEXT_27 = NL + "\t\t\tStringBuilder libjars = new StringBuilder();" + NL + "            String jobJar = \"\";" + NL + "            String[] jars = cloudLibjars.split(\",\");" + NL + "\t\t\tfor(int i=0; i<jars.length; i++) {" + NL + "\t\t\t\tString jar = jars[i];" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                }" + NL + "\t\t\t}";
  protected final String TEXT_28 = NL + "\t\t\t\tfinal String hdInsightPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_29 = ");";
  protected final String TEXT_30 = NL + "\t\t\t\tfinal String hdInsightPassword = ";
  protected final String TEXT_31 = ";";
  protected final String TEXT_32 = NL + "\t\t\t\tfinal String wasbPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_33 = ");";
  protected final String TEXT_34 = NL + "\t\t\t\tfinal String wasbPassword = ";
  protected final String TEXT_35 = ";";
  protected final String TEXT_36 = NL + NL + "\t\t\torg.talend.bigdata.launcher.fs.FileSystem azureFs = new org.talend.bigdata.launcher.fs.AzureFileSystem(\"DefaultEndpointsProtocol=https;\"" + NL + "\t\t\t\t+ \"AccountName=\"" + NL + "\t\t\t\t+ ";
  protected final String TEXT_37 = NL + "\t\t\t\t+ \";\"" + NL + "\t\t\t\t+ \"AccountKey=\" + wasbPassword, ";
  protected final String TEXT_38 = ");" + NL + "" + NL + "\t\t\torg.talend.bigdata.launcher.webhcat.WebHCatJob instance = new org.talend.bigdata.launcher.webhcat.MapReduceJob(azureFs);" + NL + "" + NL + "\t\t\tinstance.setCredentials(new org.talend.bigdata.launcher.security.HDInsightCredentials(";
  protected final String TEXT_39 = ", hdInsightPassword));" + NL + "\t\t\tinstance.setUsername(";
  protected final String TEXT_40 = ");" + NL + "\t\t\tinstance.setWebhcatEndpoint(\"https\", ";
  protected final String TEXT_41 = " + \":\" + ";
  protected final String TEXT_42 = ");" + NL + "\t\t\tinstance.setStatusFolder(org.talend.bigdata.launcher.utils.Utils.removeFirstSlash(";
  protected final String TEXT_43 = "));" + NL + "\t\t\tinstance.setRemoteFolder(org.talend.bigdata.launcher.utils.Utils.removeFirstSlash(";
  protected final String TEXT_44 = "));" + NL + "\t\t\t((org.talend.bigdata.launcher.webhcat.MapReduceJob)instance).setJarToExecute(jobJar);" + NL + "\t\t\t((org.talend.bigdata.launcher.webhcat.MapReduceJob)instance).setClassToExecute(\"";
  protected final String TEXT_45 = ".";
  protected final String TEXT_46 = ".";
  protected final String TEXT_47 = "\");" + NL + "\t\t\tinstance.setLibJars(cloudLibjars);" + NL + "\t\t\tinstance.setArgs(cloudApiArgs);" + NL + "\t\t\tinstance.callWS(instance.sendFiles());" + NL + "\t\t\tint returnCode = instance.execute();" + NL + "" + NL + "\t\t\tjava.io.InputStream is = instance.getStdOut();" + NL + "\t\t\tif(is!=null) {" + NL + "\t\t\t\tjava.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(is));" + NL + "\t\t\t\tString s;" + NL + "\t\t\t\twhile ((s = reader.readLine()) != null) {" + NL + "\t\t\t\t\tSystem.out.println(s);" + NL + "\t\t\t\t}" + NL + "\t\t\t}" + NL + "" + NL + "\t\t\treturn returnCode;" + NL + "\t\t}";
  protected final String TEXT_48 = NL + "        public int runCloudJob(String[] args) throws Exception {" + NL + "" + NL + "            java.util.List<String> dataprocJobArgs = java.util.Arrays.asList(args);" + NL + "" + NL + "            initContext();" + NL + "            initPids();" + NL + "            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "" + NL + "            String jobJar = \"\";" + NL + "            for (String jar: cloudLibjars.toString().split(\",\")) {" + NL + "                if(jar.contains(jobName.toLowerCase())) {" + NL + "                    jobJar = jar;" + NL + "                    break;" + NL + "                }" + NL + "            }" + NL + "" + NL + "            // Empty Hadoop configuration for the REST API" + NL + "            java.util.Map<String, String> conf = new java.util.HashMap<>();" + NL + "" + NL + "            org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob instance = new org.talend.bigdata.launcher.google.dataproc.DataprocMRJob.Builder()" + NL + "                .withTalendJobName(projectName + \"_\" + jobName + \"_\" + jobVersion.replace(\".\",\"_\") + \"_\" + pid)" + NL + "                .withClusterName(";
  protected final String TEXT_49 = ")" + NL + "                .withRegion(";
  protected final String TEXT_50 = ")" + NL + "                .withProjectId(";
  protected final String TEXT_51 = ")";
  protected final String TEXT_52 = NL + "                            .withServiceAccountCredentialsPath(";
  protected final String TEXT_53 = ")";
  protected final String TEXT_54 = NL + "                .withJarsBucket(";
  protected final String TEXT_55 = ")" + NL + "                .withJarToExecute(jobJar)" + NL + "                .withMainClass(\"";
  protected final String TEXT_56 = ".";
  protected final String TEXT_57 = ".";
  protected final String TEXT_58 = "\")" + NL + "                .withLibJars(cloudLibjars)" + NL + "" + NL + "                .withConf(conf)" + NL + "                .withArgs(dataprocJobArgs)" + NL;
  protected final String TEXT_59 = NL + "                .withLogLevel(\"";
  protected final String TEXT_60 = "\")";
  protected final String TEXT_61 = NL + "                .build();" + NL + "" + NL + "                // Add JVM shutdown hook to send a cancel job request to the cluster" + NL + "                Runtime.getRuntime().addShutdownHook(new DataprocShutdownHook(instance));" + NL + "                // Submit the actual job" + NL + "                int returnCode = instance.executeJob();" + NL + "                System.out.println(instance.getJobLog());" + NL + "                return returnCode;" + NL + "        }" + NL + "" + NL + "        class DataprocShutdownHook extends Thread {" + NL + "                private org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob job;" + NL + "                " + NL + "                public DataprocShutdownHook(org.talend.bigdata.launcher.google.dataproc.GoogleDataprocJob job) {" + NL + "                    this.job = job;" + NL + "                }" + NL + "                " + NL + "                @Override" + NL + "                public void run() {";
  protected final String TEXT_62 = " log.info(\"Calling Dataproc Shutdown Hook\"); ";
  protected final String TEXT_63 = NL + "                   try {" + NL + "                    // A cancel request will be actually sent to the cluster only if the job is still ongoing " + NL + "                        job.cancelJob();" + NL + "                    } catch (Exception e) {";
  protected final String TEXT_64 = " log.error(\"Could not send a job cancel request to Dataproc : \"+e.getMessage()); ";
  protected final String TEXT_65 = NL + "                    }" + NL + "                }" + NL + "        } ";
  protected final String TEXT_66 = NL + NL + "    public int run(String[] args) throws Exception {" + NL + "" + NL + "        String lastStr = \"\";" + NL + "        for (String arg : args) {" + NL + "            if (arg.equalsIgnoreCase(\"--context_param\")) {" + NL + "                lastStr = arg;" + NL + "            } else if (lastStr.equals(\"\")) {" + NL + "                evalParam(arg);" + NL + "            } else {" + NL + "                evalParam(lastStr + \" \" + arg);" + NL + "                lastStr = \"\";" + NL + "            }" + NL + "        }" + NL + "" + NL + "        startStat();" + NL;
  protected final String TEXT_67 = NL + "            initContext();" + NL + "            initMapReduceJob(getConf());";
  protected final String TEXT_68 = NL + NL + "        initResume();" + NL + "" + NL + "\t\tList<String> parametersToEncrypt = new java.util.ArrayList<String>();" + NL + "\t\t";
  protected final String TEXT_69 = NL + "\t\t\tparametersToEncrypt.add(\"";
  protected final String TEXT_70 = "\");" + NL + "\t\t\t";
  protected final String TEXT_71 = NL + "        resumeUtil.addLog(\"JOB_STARTED\", \"JOB:\" + jobName, parent_part_launcher," + NL + "            Thread.currentThread().getId() + \"\", \"\", \"\", \"\", \"\"," + NL + "            ResumeUtil.convertToJsonText(context,parametersToEncrypt));" + NL + "" + NL + "" + NL + "        mr_temp_dir = (new java.io.File(";
  protected final String TEXT_72 = ", jobName)).toString();" + NL + "        validTempFolder(mr_temp_dir);" + NL + "        globalMap = new GlobalVar(getConf());";
  protected final String TEXT_73 = NL + "                try{";
  protected final String TEXT_74 = NL + "                    ";
  protected final String TEXT_75 = "Process(globalMap);" + NL + "                } catch(Exception e) {" + NL + "\t\t\t\t    e.printStackTrace();" + NL + "\t\t\t\t    e.printStackTrace(errorMessagePS);" + NL + "\t\t\t\t    status = \"failure\";" + NL + "                }";
  protected final String TEXT_76 = NL + "\t\t\t\tclearTempFolder();";
  protected final String TEXT_77 = NL + NL + "\t\t\tendStat();" + NL + "" + NL + "\t\t\tint returnCode = 0;" + NL + "\t\t\tif(errorCode == null){" + NL + "\t\t\t\treturnCode = status != null && status.equals(\"failure\") ? 1 : 0;" + NL + "\t\t\t}else{" + NL + "\t\t\t\treturnCode = errorCode.intValue();" + NL + "\t\t\t}" + NL + "\t\t\tresumeUtil.addLog(\"JOB_ENDED\", \"JOB:\" + jobName, parent_part_launcher, Thread.currentThread().getId() + \"\", \"\",\"\" + returnCode, \"\", \"\",\"\");" + NL + "\t\t\treturn returnCode;" + NL + "\t} // run(String[] args)" + NL + "" + NL + "\t\tprivate void runMRJob(JobConf job, int groupID, int mrjobIDInGroup) throws IOException{" + NL + "\t\t\tString currentClientPathSeparator = System.getProperty(\"path.separator\");" + NL + "\t\t\tSystem.setProperty(\"path.separator\", ";
  protected final String TEXT_78 = ");" + NL + "\t\t\tif(job.get(\"mapred.reducer.class\") == null){" + NL + "\t\t\t\tjob.setNumReduceTasks(0);" + NL + "\t\t\t}" + NL + "\t\t\tmrJobClient.setGroupID(groupID);" + NL + "\t\t\tmrJobClient.setMRJobIDInGroup(mrjobIDInGroup);" + NL + "\t\t\tmrJobClient.runJob(job);" + NL + "\t\t\tSystem.setProperty(\"path.separator\", currentClientPathSeparator);" + NL + "\t\t}" + NL + "" + NL + "\t\tprivate void initMapReduceJob(Configuration conf) throws IOException{";
  protected final String TEXT_79 = NL + "\t\t\t//set basic info" + NL + "\t\t\tFileSystem.setDefaultUri(conf, ";
  protected final String TEXT_80 = ");";
  protected final String TEXT_81 = NL + "\t        conf.set(\"mapred.compress.map.output\", \"true\");" + NL + "\t        conf.set(\"mapred.map.output.compression.codec\", \"org.apache.hadoop.io.compress.DefaultCodec\");";
  protected final String TEXT_82 = NL + "            conf.set(\"hadoop.security.key.provider.path\", ";
  protected final String TEXT_83 = ");" + NL + "            conf.set(\"dfs.encryption.key.provider.uri\", ";
  protected final String TEXT_84 = ");";
  protected final String TEXT_85 = NL + "\t\t\tconf.set(\"mapreduce.job.map.output.collector.class\", \"org.apache.hadoop.mapred.MapRFsOutputBuffer\");" + NL + "\t        conf.set(\"mapreduce.job.reduce.shuffle.consumer.plugin.class\", \"org.apache.hadoop.mapreduce.task.reduce.DirectShuffle\");" + NL + "\t        ";
  protected final String TEXT_86 = NL + "\t\t\t\tconf.set(\"mapreduce.framework.name\", \"yarn\");" + NL + "\t\t\t\tconf.set(\"yarn.resourcemanager.address\", ";
  protected final String TEXT_87 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_88 = NL + "\t\t\t\t\tconf.set(\"mapreduce.jobhistory.address\", ";
  protected final String TEXT_89 = ");" + NL + "\t    \t\t\t";
  protected final String TEXT_90 = NL + "\t\t\t\t\tconf.set(\"yarn.resourcemanager.scheduler.address\", ";
  protected final String TEXT_91 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_92 = NL + "\t\t\t\t\tconf.set(\"mapreduce.app-submission.cross-platform\",\"true\");" + NL + "\t\t\t\t";
  protected final String TEXT_93 = NL + "\t\t\t\t\tconf.set(\"mapreduce.application.classpath\", \"";
  protected final String TEXT_94 = "\");";
  protected final String TEXT_95 = NL + "\t\t\t\tconf.set(\"yarn.application.classpath\", \"";
  protected final String TEXT_96 = "\");" + NL + "" + NL + "\t\t\t\t";
  protected final String TEXT_97 = NL + "\t\t\t\t\tconf.set(\"yarn.app.mapreduce.am.staging-dir\", ";
  protected final String TEXT_98 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_99 = NL + "\t\t\t\tconf.set(\"mapreduce.map.memory.mb\", ";
  protected final String TEXT_100 = ");" + NL + "\t\t\t\tconf.set(\"mapreduce.reduce.memory.mb\", ";
  protected final String TEXT_101 = ");" + NL + "\t\t\t\tconf.set(\"yarn.app.mapreduce.am.resource.mb\", ";
  protected final String TEXT_102 = ");" + NL + "\t\t\t";
  protected final String TEXT_103 = NL + "\t\t\tconf.set(\"mapred.job.tracker\", ";
  protected final String TEXT_104 = ");" + NL + "\t\t";
  protected final String TEXT_105 = NL + "                System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_106 = ");" + NL + "                System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_107 = ");";
  protected final String TEXT_108 = NL + "                org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(";
  protected final String TEXT_109 = ", ";
  protected final String TEXT_110 = ");";
  protected final String TEXT_111 = NL + "\t\t\tconf.set(\"dfs.namenode.kerberos.principal\", ";
  protected final String TEXT_112 = ");" + NL + "\t\t";
  protected final String TEXT_113 = NL + "\t\t\t\tconf.set(\"mapreduce.jobtracker.kerberos.principal\", ";
  protected final String TEXT_114 = ");" + NL + "\t\t";
  protected final String TEXT_115 = NL + "\t\t\t\tconf.set(\"yarn.resourcemanager.principal\", ";
  protected final String TEXT_116 = ");" + NL + "\t\t\t\tconf.set(\"mapreduce.jobhistory.principal\", ";
  protected final String TEXT_117 = ");" + NL + "\t\t";
  protected final String TEXT_118 = NL + "                com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();" + NL + "                maprLogin.getMapRCredentialsViaKerberos(";
  protected final String TEXT_119 = ", ";
  protected final String TEXT_120 = ");";
  protected final String TEXT_121 = NL + "                System.setProperty(\"pname\", \"MapRLogin\");" + NL + "                System.setProperty(\"https.protocols\", \"TLSv1.2\");" + NL + "                System.setProperty(\"mapr.home.dir\", ";
  protected final String TEXT_122 = ");" + NL + "                com.mapr.login.client.MapRLoginHttpsClient maprLogin = new com.mapr.login.client.MapRLoginHttpsClient();";
  protected final String TEXT_123 = NL + "                    System.setProperty(\"hadoop.login\", ";
  protected final String TEXT_124 = ");";
  protected final String TEXT_125 = NL + "                    maprLogin.setCheckUGI(false);";
  protected final String TEXT_126 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_127 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_128 = ");";
  protected final String TEXT_129 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_130 = " = ";
  protected final String TEXT_131 = "; ";
  protected final String TEXT_132 = NL;
  protected final String TEXT_133 = NL + "                    maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_134 = ", ";
  protected final String TEXT_135 = ", decryptedPassword_";
  protected final String TEXT_136 = ", ";
  protected final String TEXT_137 = ", \"\");";
  protected final String TEXT_138 = NL + "                    maprLogin.getMapRCredentialsViaPassword(";
  protected final String TEXT_139 = ", ";
  protected final String TEXT_140 = ", decryptedPassword_";
  protected final String TEXT_141 = ", ";
  protected final String TEXT_142 = ");";
  protected final String TEXT_143 = NL + "\t\t\tconf.set(\"dfs.client.use.datanode.hostname\", \"true\");" + NL + "\t\t\t";
  protected final String TEXT_144 = NL + NL + "\t\t";
  protected final String TEXT_145 = NL + "        \tconf.set(\"mapred.job.map.memory.mb\", ";
  protected final String TEXT_146 = ");" + NL + "        \tconf.set(\"mapred.job.reduce.memory.mb\", ";
  protected final String TEXT_147 = ");" + NL + "\t\t";
  protected final String TEXT_148 = NL + NL + "\t\t//tunning m/r jobs" + NL + "\t\tsetDefaultMapReduceConfig(conf);" + NL + "" + NL + "\t\t//set custom hadoop properties" + NL + "\t\tsetCustomHadoopProperties(conf);" + NL + "" + NL + "\t\t//set context" + NL + "\t\tsetContext(conf);" + NL + "" + NL + "\t\t//init MRJobClient" + NL + "\t\tmrJobClient = new MRJobClient();" + NL + "\t\tif(execStat){" + NL + "\t\t\tmrJobClient.setRunStat(runStat);" + NL + "\t\t}" + NL + "\t}" + NL + "" + NL + "\tprivate void setCustomHadoopProperties(Configuration conf){" + NL + "\t\t";
  protected final String TEXT_149 = NL + "\t\t\t\tconf.set(String.valueOf(";
  protected final String TEXT_150 = "), String.valueOf(";
  protected final String TEXT_151 = "));" + NL + "\t\t\t";
  protected final String TEXT_152 = NL + "\t}" + NL + "" + NL + "\tprivate void setDefaultMapReduceConfig(Configuration conf) throws IOException{" + NL + "\t\t//set default reduce number" + NL + "\t\tJobConf jobConf = new JobConf(conf);" + NL + "\t\tsetCustomHadoopProperties(jobConf);" + NL + "\t\tJobClient client = new JobClient(jobConf);" + NL + "\t\tint maxReduceNum = client.getClusterStatus()" + NL + "\t\t\t\t.getMaxReduceTasks();" + NL + "" + NL + "\t\tint reduceNum = (int) (maxReduceNum * 0.99);" + NL + "\t\treduceNum = reduceNum > 0 ? reduceNum : 1;" + NL + "" + NL + "\t\tconf.setInt(\"mapred.reduce.tasks\", reduceNum);" + NL + "\t\t//set distributedcache" + NL + "\t\torg.apache.hadoop.filecache.DistributedCache.createSymlink(conf);" + NL + "\t}" + NL + "" + NL + "\tprivate void validTempFolder(String mr_temp_dir) throws Exception{" + NL + "\t\tjava.io.File[] rootFoldersArray = java.io.File.listRoots();" + NL + "\t\tjava.util.List<java.io.File> listRootFoldersReadOnly = java.util.Arrays.asList(rootFoldersArray);" + NL + "\t\tjava.util.List<java.io.File> listRootFolders = new java.util.ArrayList<java.io.File>(listRootFoldersReadOnly);" + NL + "\t\tlistRootFolders.add(new java.io.File(System.getProperty(\"user.home\")));" + NL + "\t\t";
  protected final String TEXT_153 = NL + "\t\tlistRootFolders.add(new java.io.File(\"/user/\" + ";
  protected final String TEXT_154 = "));" + NL + "\t\t";
  protected final String TEXT_155 = NL + "\t\tlistRootFolders.add(new java.io.File(\"/user/\" + System.getProperty(\"user.name\")));" + NL + "\t\tif(listRootFolders.contains(new java.io.File(mr_temp_dir))) {" + NL + "\t\t\tthrow new Exception(\"Using a root folder or a home folder as the temporary directory is not recommended, please choose another one.\");" + NL + "\t\t}" + NL + "\t}" + NL + NL;
  protected final String TEXT_156 = NL + "            final String clouderaManagerPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_157 = ");";
  protected final String TEXT_158 = NL + "            final String clouderaManagerPassword = ";
  protected final String TEXT_159 = ";";
  protected final String TEXT_160 = NL + "        this.lineageCreator = new org.talend.lineage.cloudera.LineageCreator(";
  protected final String TEXT_161 = NL + "                ";
  protected final String TEXT_162 = ",";
  protected final String TEXT_163 = NL + "                ";
  protected final String TEXT_164 = ",";
  protected final String TEXT_165 = NL + "                ";
  protected final String TEXT_166 = ",";
  protected final String TEXT_167 = NL + "                ";
  protected final String TEXT_168 = "," + NL + "                clouderaManagerPassword," + NL + "                jobName + \"_\" + jobVersion.replace(\".\", \"_\")," + NL + "                projectName,";
  protected final String TEXT_169 = NL + "                ";
  protected final String TEXT_170 = ",";
  protected final String TEXT_171 = NL + "                ";
  protected final String TEXT_172 = ",";
  protected final String TEXT_173 = NL + "                ";
  protected final String TEXT_174 = ");";
  protected final String TEXT_175 = NL + "            System.setProperty(\"atlas.conf\", ";
  protected final String TEXT_176 = ");";
  protected final String TEXT_177 = NL + "                String decryptedAtlasUserPassword = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_178 = ");";
  protected final String TEXT_179 = NL + "                String decryptedAtlasUserPassword = ";
  protected final String TEXT_180 = ";";
  protected final String TEXT_181 = NL + "            this.lineageCreator = new org.talend.lineage.atlas.AtlasLineageCreator(";
  protected final String TEXT_182 = ", ";
  protected final String TEXT_183 = ", decryptedAtlasUserPassword);";
  protected final String TEXT_184 = NL + "            this.lineageCreator = new org.talend.lineage.atlas.AtlasLineageCreator(";
  protected final String TEXT_185 = ");";
  protected final String TEXT_186 = NL + "        java.util.Map<String, Object> lineageCreatorJobMetadata = new java.util.HashMap<String, Object>();" + NL + "        lineageCreatorJobMetadata.put(\"name\", jobName);" + NL + "        lineageCreatorJobMetadata.put(\"description\", jobName);" + NL + "        lineageCreatorJobMetadata.put(\"purpose\", \"Talend BigData Job\");" + NL + "        lineageCreatorJobMetadata.put(\"author\", System.getProperty(\"user.name\"));" + NL + "        lineageCreatorJobMetadata.put(\"version\", jobVersion);" + NL + "        lineageCreatorJobMetadata.put(\"jobType\", \"Talend BigData Job\");" + NL + "        lineageCreatorJobMetadata.put(\"framework\", \"Talend BigData\");" + NL + "        lineageCreatorJobMetadata.put(\"status\", \"FINISHED\");" + NL + "        lineageCreatorJobMetadata.put(\"creationDate\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"lastModificationDate\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"startTime\", System.currentTimeMillis());" + NL + "        lineageCreatorJobMetadata.put(\"endTime\", System.currentTimeMillis());" + NL + "" + NL + "        this.lineageCreator.addJobInfo(lineageCreatorJobMetadata);";
  protected final String TEXT_187 = NL + "        java.util.Map<String, String> columns_";
  protected final String TEXT_188 = " = new java.util.HashMap<>();";
  protected final String TEXT_189 = NL + "            columns_";
  protected final String TEXT_190 = ".put(\"";
  protected final String TEXT_191 = "\", \"";
  protected final String TEXT_192 = "\");";
  protected final String TEXT_193 = NL + "        java.util.Map<String, String> columns_";
  protected final String TEXT_194 = " = new java.util.HashMap<>();";
  protected final String TEXT_195 = NL + "                        columns_";
  protected final String TEXT_196 = ".put(\"";
  protected final String TEXT_197 = "\", \"";
  protected final String TEXT_198 = "\");";
  protected final String TEXT_199 = NL + "                    columns_";
  protected final String TEXT_200 = ".put(\"";
  protected final String TEXT_201 = "\", \"";
  protected final String TEXT_202 = "\");";
  protected final String TEXT_203 = NL + "        lineageCreator.addDataset(columns_";
  protected final String TEXT_204 = ", \"";
  protected final String TEXT_205 = "\", ";
  protected final String TEXT_206 = ", \"";
  protected final String TEXT_207 = "\");";
  protected final String TEXT_208 = NL + "        java.util.List<String> inputNodes_";
  protected final String TEXT_209 = " = new java.util.ArrayList<String>();";
  protected final String TEXT_210 = NL + "                inputNodes_";
  protected final String TEXT_211 = ".add(\"";
  protected final String TEXT_212 = "\");";
  protected final String TEXT_213 = NL + "        java.util.List<String> outputNodes_";
  protected final String TEXT_214 = " = new java.util.ArrayList<String>();";
  protected final String TEXT_215 = NL + "                outputNodes_";
  protected final String TEXT_216 = ".add(\"";
  protected final String TEXT_217 = "\");";
  protected final String TEXT_218 = NL + "        this.lineageCreator.addNodeToLineage(\"";
  protected final String TEXT_219 = "\", columns_";
  protected final String TEXT_220 = ", inputNodes_";
  protected final String TEXT_221 = ", outputNodes_";
  protected final String TEXT_222 = ", new java.util.HashMap<String, Object>());";
  protected final String TEXT_223 = NL + "        org.talend.lineage.common.ILineageCreator lineageCreator;";
  protected final String TEXT_224 = NL + NL + "\tprivate void startStat(){" + NL + "\t\tif(clientHost == null){" + NL + "\t\t\tclientHost = defaultClientHost;" + NL + "\t\t}" + NL + "\t\tif(portStats != null){" + NL + "\t\t\t// portStats = -1; //for testing" + NL + "\t\t\tif(portStats < 0 || portStats > 65535){" + NL + "\t\t\t\t// issue:10869, the portStats is invalid, so this client socket" + NL + "\t\t\t\t// can't open" + NL + "\t\t\t\tSystem.err.println(\"The statistics socket port \" + portStats" + NL + "\t\t\t\t\t\t+ \" is invalid.\");" + NL + "\t\t\t\texecStat = false;" + NL + "\t\t\t}" + NL + "\t\t} else {" + NL + "\t\t\texecStat = false;" + NL + "\t\t}" + NL + "\t\tif(execStat){" + NL + "\t\t\ttry{" + NL + "\t\t\t\trunStat.startThreadStat(clientHost, portStats);" + NL + "\t\t\t}catch(java.io.IOException ioException){" + NL + "\t\t\t\tioException.printStackTrace();" + NL + "\t\t\t}" + NL + "\t\t}" + NL + "" + NL + "\t}" + NL + "" + NL + "\tprivate void endStat(){" + NL + "\t\tif(execStat){" + NL + "\t\t\trunStat.stopThreadStat();" + NL + "\t\t}" + NL;
  protected final String TEXT_225 = NL + "            lineageCreator.sendToLineageProvider(";
  protected final String TEXT_226 = ");";
  protected final String TEXT_227 = NL + "\t}" + NL + "" + NL + "\tprivate void initContext(){" + NL + "\t\t//get context" + NL + "\t\ttry{" + NL + "\t        //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead." + NL + "\t        java.io.InputStream inContext = ";
  protected final String TEXT_228 = ".class.getClassLoader().getResourceAsStream(\"";
  protected final String TEXT_229 = "/";
  protected final String TEXT_230 = "/contexts/\"+contextStr+\".properties\");" + NL + "\t        if(isDefaultContext && inContext == null){" + NL + "" + NL + "\t        }else{" + NL + "\t            if(inContext!=null){" + NL + "\t            \t//defaultProps is in order to keep the original context value" + NL + "                    defaultProps.load(inContext);" + NL + "                    inContext.close();" + NL + "                    context = new ContextProperties(defaultProps);" + NL + "                }else{" + NL + "                    //print info and job continue to run, for case: context_param is not empty." + NL + "                    System.err.println(\"Could not find the context \" + contextStr);" + NL + "                }" + NL + "            }" + NL + "" + NL + "            if(!context_param.isEmpty()){" + NL + "                context.putAll(context_param);" + NL + "            }" + NL + "            context.loadValue(context_param,null);" + NL + "            if(parentContextMap != null && !parentContextMap.isEmpty()){";
  protected final String TEXT_231 = NL + "                    if(parentContextMap.containsKey(\"";
  protected final String TEXT_232 = "\")){" + NL + "                        context.";
  protected final String TEXT_233 = " = (";
  protected final String TEXT_234 = ") parentContextMap.get(\"";
  protected final String TEXT_235 = "\");" + NL + "                    }";
  protected final String TEXT_236 = NL + "            }" + NL + "        }catch (java.io.IOException ie){" + NL + "            System.err.println(\"Could not load context \"+contextStr);" + NL + "            ie.printStackTrace();" + NL + "        }";
  protected final String TEXT_237 = NL + "    }" + NL + "" + NL + "    private void setContext(Configuration conf){" + NL + "        //get context" + NL + "        try{" + NL + "            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead." + NL + "            java.net.URL inContextUrl = ";
  protected final String TEXT_238 = ".class.getClassLoader().getResource(\"";
  protected final String TEXT_239 = "/";
  protected final String TEXT_240 = "/contexts/\"+contextStr+\".properties\");" + NL + "            if(isDefaultContext && inContextUrl == null){" + NL + "" + NL + "            }else{" + NL + "                if(inContextUrl!=null){" + NL + "                    conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr+\".properties\");" + NL + "                    java.io.File contextFile = null;" + NL + "                    try {" + NL + "                    \tjava.net.URI fileURI = new java.net.URI(inContextUrl.getProtocol(),inContextUrl.getHost(),inContextUrl.getPath(), contextStr+\".properties\");" + NL + "                    \tcontextFile = new java.io.File(fileURI);" + NL + "                    } catch (java.lang.Exception urie) {" + NL + "                    \t// Ignore the exception. It will be handled below." + NL + "                    }" + NL + "                    if(contextFile!=null && contextFile.exists()) {" + NL + "                        org.talend.hadoop.mapred.lib.DistributedCache.addCacheFile(contextFile.toURI(), conf);" + NL + "                    } else {" + NL + "                        java.io.InputStream contextIn = ";
  protected final String TEXT_241 = ".class.getClassLoader().getResourceAsStream(\"";
  protected final String TEXT_242 = "/";
  protected final String TEXT_243 = "/contexts/\"+contextStr+\".properties\");" + NL + "                        if(contextIn != null){" + NL + "                            java.io.File tmpFile = new java.io.File(System.getProperty(\"java.io.tmpdir\") + \"/\" + jobName,  contextStr+\".properties\");" + NL + "                            java.io.OutputStream contextOut = null;" + NL + "                            try {" + NL + "                                tmpFile.getParentFile().mkdir();" + NL + "                                if(tmpFile.exists()) { tmpFile.delete(); }" + NL + "                                tmpFile.createNewFile();" + NL + "                                contextOut = new java.io.FileOutputStream(tmpFile);" + NL + "                                " + NL + "                                int len = -1;" + NL + "                                byte[] b = new byte[4096];" + NL + "                                while ((len = contextIn.read(b)) != -1) {" + NL + "                                    contextOut.write(b, 0, len);" + NL + "                                }" + NL + "                            } catch(java.io.IOException ioe) {" + NL + "                                ioe.printStackTrace();" + NL + "                            } finally {" + NL + "                                try {" + NL + "                                    contextIn.close();" + NL + "                                    if(contextOut != null) {" + NL + "                                        contextOut.close();" + NL + "                                    }" + NL + "                                } catch (java.io.IOException ioe) {" + NL + "                                    ioe.printStackTrace();" + NL + "                                }" + NL + "                            }" + NL + "" + NL + "                            org.talend.hadoop.mapred.lib.DistributedCache.addCacheFile(tmpFile.toURI(), conf);" + NL + "                        }" + NL + "                    }" + NL + "                }" + NL + "            }" + NL + "" + NL + "            if(!context_param.isEmpty()){" + NL + "                for(Object contextKey : context_param.keySet()){" + NL + "                    conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX + contextKey, context.getProperty(contextKey.toString()));" + NL + "                    conf.set(ContextProperties.CONTEXT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, \"\") + \" \" + contextKey);" + NL + "                }" + NL + "            }" + NL + "" + NL + "            if(parentContextMap != null && !parentContextMap.isEmpty()){";
  protected final String TEXT_244 = NL + "                    if(parentContextMap.containsKey(\"";
  protected final String TEXT_245 = "\")){" + NL + "                        conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + \"";
  protected final String TEXT_246 = "\", parentContextMap.get(\"";
  protected final String TEXT_247 = "\").toString());" + NL + "                        conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, \"\") + \" \" + \"";
  protected final String TEXT_248 = "\");" + NL + "                    }";
  protected final String TEXT_249 = NL + "            }" + NL + "        }catch (java.lang.Exception e){" + NL + "            System.err.println(\"Could not load context \"+contextStr);" + NL + "            e.printStackTrace();" + NL + "        }" + NL + "    }" + NL + "" + NL + "\tprivate void initPids() {" + NL + "        if (pid == null || \"0\".equals(pid)) {" + NL + "            pid = TalendString.getAsciiRandomString(6);" + NL + "        }" + NL + "" + NL + "        if (rootPid == null) {" + NL + "            rootPid = pid;" + NL + "        }" + NL + "" + NL + "        if (fatherPid == null) {" + NL + "            fatherPid = pid;" + NL + "        } else {" + NL + "            isChildJob = true;" + NL + "        }" + NL + "\t} " + NL + "" + NL + "    private void initResume() {" + NL + "\t\tinitPids();" + NL + "" + NL + "        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);" + NL + "        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);" + NL + "        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);" + NL + "    }" + NL + "" + NL + "    private void clearTempFolder(){" + NL + "        try{";
  protected final String TEXT_250 = NL + "                final String mr_temp_dir = this.mr_temp_dir;" + NL + "                UserGroupInformation ugi = UserGroupInformation.createRemoteUser(";
  protected final String TEXT_251 = ");" + NL + "                ugi.doAs(new PrivilegedExceptionAction<Void>() {" + NL + "                    public Void run() throws Exception {";
  protected final String TEXT_252 = NL + "            FileSystem fs = FileSystem.get(getConf());" + NL + "            Path pathToDelete_mrTmp = new Path(mr_temp_dir);" + NL + "            if (fs.exists(pathToDelete_mrTmp)) {" + NL + "                fs.delete(pathToDelete_mrTmp, true);" + NL + "            }" + NL;
  protected final String TEXT_253 = NL + "                        return null;" + NL + "                    }" + NL + "                });";
  protected final String TEXT_254 = NL + "        } catch (Exception e) {" + NL + "            e.printStackTrace();" + NL + "        }" + NL + "    }" + NL + "" + NL + "    private String genTempFolderForComponent(String name){" + NL + "        java.io.File tempDir = new java.io.File(";
  protected final String TEXT_255 = " + \"/\" + pid, name);" + NL + "        String tempDirPath = tempDir.getPath();" + NL + "        if(java.io.File.separatorChar != '/')" + NL + "            tempDirPath = tempDirPath.replace(java.io.File.separatorChar, '/');" + NL + "        return tempDirPath;" + NL + "    }" + NL + "" + NL + "    private static boolean runInRuntime;" + NL + "" + NL + "    private String[] normalizeArgs(String[] args){" + NL + "        if(args.length > 0) {" + NL + "            List<String> argsList = java.util.Arrays.asList(args);" + NL + "            int indexlibjars = argsList.indexOf(\"-libjars\");" + NL + "            if(indexlibjars > -1 && indexlibjars + 1 < args.length) {" + NL + "                indexlibjars += 1;" + NL + "                String newlibjars = argsList.get(indexlibjars);" + NL + "                newlibjars = newlibjars.replaceAll(\" \", \"%20\");" + NL + "                java.lang.StringBuilder libJarsWithRealPath = new java.lang.StringBuilder();";
  protected final String TEXT_256 = NL + "                        if(!isTempletonCall(args)) {";
  protected final String TEXT_257 = NL + "                        if(!isGoogleDataprocCall(args)) {";
  protected final String TEXT_258 = NL + "                boolean isFirst = true;" + NL + "                try {" + NL + "                    routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "                    for(String jar:newlibjars.split(\",\")) {" + NL + "                        if(!isFirst) {" + NL + "                            libJarsWithRealPath.append(\",\");" + NL + "                        }";
  protected final String TEXT_259 = NL + "                                libJarsWithRealPath.append(getJarsToRegister.replaceJarPaths(jar));";
  protected final String TEXT_260 = NL + "                                libJarsWithRealPath.append(getJarsToRegister.replaceJarPaths(jar, \"file:///\"));";
  protected final String TEXT_261 = NL + "                        isFirst = false;" + NL + "                    }" + NL + "                } catch (java.lang.Exception e) {";
  protected final String TEXT_262 = " log.error(e.getMessage()); ";
  protected final String TEXT_263 = NL + "                }";
  protected final String TEXT_264 = NL + "                    args[indexlibjars] = libJarsWithRealPath.toString();" + NL + "                    } else {" + NL + "                        args[indexlibjars] = newlibjars;";
  protected final String TEXT_265 = NL + "                            args[indexlibjars - 1] = \"NOT_USED\";";
  protected final String TEXT_266 = NL + "                    }" + NL + "                    cloudLibjars = libJarsWithRealPath.toString();";
  protected final String TEXT_267 = NL + "                    args[indexlibjars] = libJarsWithRealPath.toString();";
  protected final String TEXT_268 = NL + "                " + NL + "                return args;" + NL + "            }" + NL + "        }";
  protected final String TEXT_269 = NL + "\t\t\t\t\trunInRuntime = true;" + NL + "\t\t\t\t\tjava.util.Collection<String> jars = new java.util.HashSet<String>();";
  protected final String TEXT_270 = NL + "\t\t\t\t\t\tjars.add(\"";
  protected final String TEXT_271 = "\");";
  protected final String TEXT_272 = NL + "                    try {" + NL + "                        String libjars = (String) Class.forName(\"org.talend.cloud.MRHelper\")" + NL + "                            .getDeclaredMethod(\"getLibJars\", Class.class, java.util.Collection.class)" + NL + "                            .invoke(null, getClass(), jars);" + NL + "                        int len = args.length;" + NL + "                        args = java.util.Arrays.copyOf(args, len + 2);" + NL + "                        args[len] = \"-libjars\";" + NL + "                        args[len + 1] = libjars;" + NL + "                    } catch (Exception e) {" + NL + "                    }";
  protected final String TEXT_273 = NL + "        return args;" + NL + "    }" + NL;
  protected final String TEXT_274 = NL + "\t\tprivate boolean isTempletonCall(String[] args) {" + NL + "\t\t\tList<String> argsList = java.util.Arrays.asList(args);" + NL + "\t\t\tint indexlibjars = argsList.indexOf(\"-calledByTempleton\");" + NL + "\t\t\treturn (indexlibjars!=-1);" + NL + "\t\t}";
  protected final String TEXT_275 = NL + "        private boolean isGoogleDataprocCall(String[] args) {" + NL + "            List<String> argsList = java.util.Arrays.asList(args);" + NL + "            int indexlibjars = argsList.indexOf(\"-calledByGoogleDataproc\");" + NL + "            return (indexlibjars!=-1);" + NL + "        }";
  protected final String TEXT_276 = NL + NL + "    private void evalParam(String arg) {" + NL + "        if (arg.startsWith(\"--resuming_logs_dir_path\")) {" + NL + "            resuming_logs_dir_path = arg.substring(25);" + NL + "        } else if (arg.startsWith(\"--resuming_checkpoint_path\")) {" + NL + "            resuming_checkpoint_path = arg.substring(27);" + NL + "        } else if (arg.startsWith(\"--parent_part_launcher\")) {" + NL + "            parent_part_launcher = arg.substring(23);" + NL + "        } else if (arg.startsWith(\"--father_pid=\")) {" + NL + "            fatherPid = arg.substring(13);" + NL + "        } else if (arg.startsWith(\"--root_pid=\")) {" + NL + "            rootPid = arg.substring(11);" + NL + "        } else if (arg.startsWith(\"--pid=\")) {" + NL + "            pid = arg.substring(6);" + NL + "        } else if (arg.startsWith(\"--context=\")) {";
  protected final String TEXT_277 = NL + "                cloudApiArgs.add(arg);";
  protected final String TEXT_278 = NL + "            contextStr = arg.substring(\"--context=\".length());" + NL + "            isDefaultContext = false;" + NL + "        } else if (arg.startsWith(\"--context_param\")) {" + NL + "            String keyValue = arg.substring(\"--context_param\".length() + 1);" + NL + "            int index = -1;" + NL + "            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {" + NL + "                context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));" + NL + "            }";
  protected final String TEXT_279 = NL + "                cloudApiArgs.add(arg);";
  protected final String TEXT_280 = NL + "        } else if (arg.startsWith(\"--stat_port=\")) {" + NL + "            String portStatsStr = arg.substring(12);" + NL + "            if (portStatsStr != null && !portStatsStr.equals(\"null\")) {" + NL + "                portStats = Integer.parseInt(portStatsStr);" + NL + "            }" + NL + "        } else if (arg.startsWith(\"--client_host=\")) {" + NL + "            clientHost = arg.substring(14);" + NL + "        } else if (arg.startsWith(\"--log4jLevel=\")) {" + NL + "            log4jLevel = arg.substring(13);" + NL + "        }" + NL + "    }" + NL + "" + NL + "    private final String[][] escapeChars = {" + NL + "        {\"\\\\\\\\\",\"\\\\\"},{\"\\\\n\",\"\\n\"},{\"\\\\'\",\"\\'\"},{\"\\\\r\",\"\\r\"}," + NL + "        {\"\\\\f\",\"\\f\"},{\"\\\\b\",\"\\b\"},{\"\\\\t\",\"\\t\"}" + NL + "        };" + NL + "    private String replaceEscapeChars (String keyValue) {" + NL + "" + NL + "        if (keyValue == null || (\"\").equals(keyValue.trim())) {" + NL + "            return keyValue;" + NL + "        }" + NL + "" + NL + "        StringBuilder result = new StringBuilder();" + NL + "        int currIndex = 0;" + NL + "        while (currIndex < keyValue.length()) {" + NL + "            int index = -1;" + NL + "            // judege if the left string includes escape chars" + NL + "            for (String[] strArray : escapeChars) {" + NL + "                index = keyValue.indexOf(strArray[0],currIndex);" + NL + "                if (index>=0) {" + NL + "" + NL + "                    result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));" + NL + "                    currIndex = index + strArray[0].length();" + NL + "                    break;" + NL + "                }" + NL + "            }" + NL + "            // if the left string doesn't include escape chars, append the left into the result" + NL + "            if (index < 0) {" + NL + "                result.append(keyValue.substring(currIndex));" + NL + "                currIndex = currIndex + keyValue.length();" + NL + "            }" + NL + "        }" + NL + "" + NL + "        return result.toString();" + NL + "    }" + NL + "" + NL + "    public Integer getErrorCode() {" + NL + "        return errorCode;" + NL + "    }" + NL + "" + NL + "    public String getStatus() {" + NL + "        return status;" + NL + "    }" + NL + "}";
  protected final String TEXT_281 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    IProcess process = (IProcess)v.get(0);

    boolean isLog4jEnabled = ("true").equals(org.talend.core.model.process.ElementParameterParser.getValue(process, "__LOG4J_ACTIVATE__"));

    List<INode> rootNodes = (List<INode>)v.get(1);
    INode mrconn = (INode)v.get(2);
    String jobFolderName = JavaResourcesHelper.getJobFolderName(process.getName(), process.getVersion());
    List<IContextParameter> params = new ArrayList<IContextParameter>();
    params=process.getContextManager().getDefaultContext().getContextParameterList();

    String mrNameNode = ElementParameterParser.getValue(mrconn,"__NAMENODE__");
    String mrJobTracker = ElementParameterParser.getValue(mrconn,"__JOBTRACKER__");

    boolean useYarn = "true".equals(ElementParameterParser.getValue(mrconn, "__USE_YARN__"));
    String resourceManager = ElementParameterParser.getValue(mrconn,"__RESOURCE_MANAGER__");
	String mrDistribution = ElementParameterParser.getValue(mrconn,"__DISTRIBUTION__");
	String mrVersion = ElementParameterParser.getValue(mrconn,"__MR_VERSION__");
	boolean mrUseKrb = false;

	org.talend.hadoop.distribution.component.MRComponent mrDistrib = null;

	try {
		mrDistrib = (org.talend.hadoop.distribution.component.MRComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(mrDistribution, mrVersion);
	} catch (java.lang.Exception e) {
		e.printStackTrace();
		return "";
	}
	boolean isCustom = mrDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;

	boolean isExecutedThroughWebHCat = mrDistrib.isExecutedThroughWebHCat();
    boolean isCloudDistribution = !isCustom && mrDistrib.isCloudDistribution();
    boolean useCloudLauncher = !isCustom && mrDistrib.useCloudLauncher();
    boolean isGoogleDataprocDistribution = !isCustom && mrDistrib.isGoogleDataprocDistribution();

    String className = process.getName();

    if(isCustom || mrDistrib.doSupportKerberos()){
        mrUseKrb = "true".equals(ElementParameterParser.getValue(mrconn,"__USE_KRB__"));
    }
    String mrNNPrincipal = ElementParameterParser.getValue(mrconn,"__NAMENODE_PRINCIPAL__");
    String mrJTPrincipal = ElementParameterParser.getValue(mrconn,"__JOBTRACKER_PRINCIPAL__");
    String mrRMPrincipal = ElementParameterParser.getValue(mrconn,"__RESOURCEMANAGER_PRINCIPAL__");
    String mrJHPrincipal = ElementParameterParser.getValue(mrconn, "__JOBHISTORY_PRINCIPAL__");
    boolean useKeytab = "true".equals(ElementParameterParser.getValue(mrconn, "__USE_KEYTAB__"));
    String userPrincipal = ElementParameterParser.getValue(mrconn, "__PRINCIPAL__");
    String keytabPath = ElementParameterParser.getValue(mrconn, "__KEYTAB_PATH__");
    List<Map<String, String>> mrCustomProps = (List<Map<String,String>>)ElementParameterParser.getObjectValue(mrconn,"__HADOOP_ADVANCED_PROPERTIES__");
    String mrUserName = ElementParameterParser.getValue(mrconn,"__USERNAME__");
    boolean mrNeedUserName = !(mrUserName == null || "".equals(mrUserName) || "\"\"".equals(mrUserName) || mrUseKrb);
    String mrServerPathSeparator = ElementParameterParser.getValue(mrconn,"__SERVER_PATH_SEPARATOR__");
    boolean mrClearTempFolder = "true".equals(ElementParameterParser.getValue(mrconn,"__RM_TEMP_FOLDER__"));
    String mrTempFolder = ElementParameterParser.getValue(mrconn,"__TEMP_FOLDER__");
    boolean mrUseDatanodeHostname = "true".equals(ElementParameterParser.getValue(mrconn, "__USE_DATANODE_HOSTNAME__"));
    boolean mrCompressMapIO = "true".equals(ElementParameterParser.getValue(mrconn,"__MAPRED_JOB_COMPRESS_MAP_IO__"));
    
    boolean useMapRTicket = ElementParameterParser.getBooleanValue(mrconn, "__USE_MAPRTICKET__");
    String mapRTicketUsername = ElementParameterParser.getValue(mrconn, "__USERNAME__");
    String mapRTicketCluster = ElementParameterParser.getValue(mrconn, "__MAPRTICKET_CLUSTER__");
    String mapRTicketDuration = ElementParameterParser.getValue(mrconn, "__MAPRTICKET_DURATION__");

    boolean setMapRHomeDir = ElementParameterParser.getBooleanValue(mrconn, "__SET_MAPR_HOME_DIR__");
    String mapRHomeDir = ElementParameterParser.getValue(mrconn, "__MAPR_HOME_DIR__");

    boolean setMapRHadoopLogin = ElementParameterParser.getBooleanValue(mrconn, "__SET_HADOOP_LOGIN__");
    String mapRHadoopLogin = ElementParameterParser.getValue(mrconn, "__HADOOP_LOGIN__");
    boolean preloadAuthentication = ElementParameterParser.getBooleanValue(mrconn, "__PRELOAD_AUTHENTIFICATION__");
    if (!useMapRTicket) {
        preloadAuthentication = false;
    }
    String passwordFieldName = "";
    boolean useHDFSEnc = ElementParameterParser.getBooleanValue(mrconn, "__USE_HDFS_ENCRYPTION__");
    String hdfsKMS = ElementParameterParser.getValue(mrconn, "__HDFS_ENCRYPTION_KEY_PROVIDER__");

    // tHadoopConfManager component generation.
    List<INode> hcmNodes = new ArrayList<INode>();
    for (INode pNode : process.getNodesOfType("tHadoopConfManager")) {
        hcmNodes.add(pNode);
    }

    if(hcmNodes.size() > 1) {
 
    stringBuffer.append(TEXT_1);
    
    }

    
    stringBuffer.append(TEXT_2);
     if(isCloudDistribution) { 
    stringBuffer.append(TEXT_3);
     } 
    stringBuffer.append(TEXT_4);
    stringBuffer.append(codeGenArgument.getContextName());
    stringBuffer.append(TEXT_5);
    stringBuffer.append(process.getName() );
    stringBuffer.append(TEXT_6);
    stringBuffer.append(process.getName() );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(process.getName() );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(process.getName() );
    stringBuffer.append(TEXT_9);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_10);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_11);
    }

		if(mrconn == null){
			System.err.println("use tMRConnection component to config the hadoop environment");
		}else{
			
    stringBuffer.append(TEXT_12);
    
				if(isCloudDistribution){
                    if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_13);
    
                    }
                    else 
                    if(isGoogleDataprocDistribution) {

    stringBuffer.append(TEXT_14);
    
                    }

    stringBuffer.append(TEXT_15);
    
				}

    stringBuffer.append(TEXT_16);
     if (preloadAuthentication || hcmNodes.size() == 1) {
    stringBuffer.append(TEXT_17);
    
                if(hcmNodes.size() == 1) {
                    INode pNode = hcmNodes.get(0);

    stringBuffer.append(TEXT_18);
    stringBuffer.append(pNode.getUniqueName());
    stringBuffer.append(TEXT_19);
    
                }

    stringBuffer.append(TEXT_20);
     } 
    stringBuffer.append(TEXT_21);
    
				if(isCloudDistribution) {

    stringBuffer.append(TEXT_22);
    
				}

    stringBuffer.append(TEXT_23);
    
		}
		
    stringBuffer.append(TEXT_24);
    
	if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_25);
     if (!preloadAuthentication) {
    stringBuffer.append(TEXT_26);
     } 
    
			String cid = "MR";


    stringBuffer.append(TEXT_27);
                

		    passwordFieldName = "__HDINSIGHT_PASSWORD__";
			if (ElementParameterParser.canEncrypt(mrconn, passwordFieldName)) {

    stringBuffer.append(TEXT_28);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(mrconn, passwordFieldName));
    stringBuffer.append(TEXT_29);
    
			} else {

    stringBuffer.append(TEXT_30);
    stringBuffer.append( ElementParameterParser.getValue(mrconn, passwordFieldName));
    stringBuffer.append(TEXT_31);
    
			}

			passwordFieldName = "__WASB_PASSWORD__";
			if (ElementParameterParser.canEncrypt(mrconn, passwordFieldName)) {

    stringBuffer.append(TEXT_32);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(mrconn, passwordFieldName));
    stringBuffer.append(TEXT_33);
    
			} else {

    stringBuffer.append(TEXT_34);
    stringBuffer.append( ElementParameterParser.getValue(mrconn, passwordFieldName));
    stringBuffer.append(TEXT_35);
    
			}

    stringBuffer.append(TEXT_36);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__WASB_USERNAME__"));
    stringBuffer.append(TEXT_37);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__WASB_CONTAINER__"));
    stringBuffer.append(TEXT_38);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__HDINSIGHT_USERNAME__"));
    stringBuffer.append(TEXT_39);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__WEBHCAT_USERNAME__"));
    stringBuffer.append(TEXT_40);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__WEBHCAT_HOST__"));
    stringBuffer.append(TEXT_41);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__WEBHCAT_PORT__"));
    stringBuffer.append(TEXT_42);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__STATUSDIR__"));
    stringBuffer.append(TEXT_43);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__REMOTE_FOLDER__"));
    stringBuffer.append(TEXT_44);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_45);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(process.getName(), process.getVersion()));
    stringBuffer.append(TEXT_46);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_47);
    
	}
    else 
    if(isGoogleDataprocDistribution) {
    
    stringBuffer.append(TEXT_48);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__GOOGLE_CLUSTER_ID__"));
    stringBuffer.append(TEXT_49);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__GOOGLE_REGION__"));
    stringBuffer.append(TEXT_50);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__GOOGLE_PROJECT_ID__"));
    stringBuffer.append(TEXT_51);
    
                        if(ElementParameterParser.getBooleanValue(mrconn, "__DEFINE_PATH_TO_GOOGLE_CREDENTIALS__")) {

    stringBuffer.append(TEXT_52);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__PATH_TO_GOOGLE_CREDENTIALS__"));
    stringBuffer.append(TEXT_53);
                  
                        }

    stringBuffer.append(TEXT_54);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__GOOGLE_JARS_BUCKET__"));
    stringBuffer.append(TEXT_55);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_56);
    stringBuffer.append(JavaResourcesHelper.getJobFolderName(className, process.getVersion()));
    stringBuffer.append(TEXT_57);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_58);
    
                boolean customLogLevel  = "true".equals(ElementParameterParser.getValue(process, "__LOG4J_RUN_ACTIVATE__"));
                if(customLogLevel){
                String runLevel = ElementParameterParser.getValue(process, "__LOG4J_RUN_LEVEL__").toUpperCase();
                
    stringBuffer.append(TEXT_59);
    stringBuffer.append(runLevel);
    stringBuffer.append(TEXT_60);
    }
    stringBuffer.append(TEXT_61);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_62);
    }
    stringBuffer.append(TEXT_63);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_64);
    }
    stringBuffer.append(TEXT_65);
    
    }

    stringBuffer.append(TEXT_66);
     if (!preloadAuthentication && hcmNodes.size() != 1) {
    stringBuffer.append(TEXT_67);
     } 
    stringBuffer.append(TEXT_68);
    
		for(IContextParameter ctxParam :params) {
        	if ("id_Password".equals(ctxParam.getType())) {
			
    stringBuffer.append(TEXT_69);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_70);
    
        	}
        }
		
    stringBuffer.append(TEXT_71);
    stringBuffer.append(mrTempFolder);
    stringBuffer.append(TEXT_72);
    
        for(INode rootNode : rootNodes){
            String componentName = rootNode.getComponent().getName();
            if(!"tHadoopConfManager".equals(componentName)) {
                String uniqueName = rootNode.getUniqueName();

    stringBuffer.append(TEXT_73);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(rootNode.getUniqueName());
    stringBuffer.append(TEXT_75);
    
            }
    	}

			if(mrClearTempFolder){

    stringBuffer.append(TEXT_76);
    
			}

    stringBuffer.append(TEXT_77);
    stringBuffer.append(mrServerPathSeparator);
    stringBuffer.append(TEXT_78);
    
		if(!isCloudDistribution) {

    stringBuffer.append(TEXT_79);
    stringBuffer.append(mrNameNode);
    stringBuffer.append(TEXT_80);
    
		}

		if (mrCompressMapIO) {
		    // Other possible values to be set when compressing intermediate map ouput.
		    // "mapred.compress.map.output"
	        // "mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec"
	        // "mapreduce.map.output.compress"
	        // "mapreduce.map.output.compress.codec"

    stringBuffer.append(TEXT_81);
    
		}

        if(useHDFSEnc){

    stringBuffer.append(TEXT_82);
    stringBuffer.append(hdfsKMS);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(hdfsKMS);
    stringBuffer.append(TEXT_84);
    
        }

		// TODO use better variables
		if(!"CUSTOM".equals(mrDistribution) && ("MAPR401".equals(mrVersion) || "MAPR410".equals(mrVersion))) {//set the default properties
		    
    stringBuffer.append(TEXT_85);
    
		}

		if(((isCustom && useYarn) || (!isCustom && mrDistrib.isHadoop2()))) {
    
			if(!isCloudDistribution) {

    stringBuffer.append(TEXT_86);
    stringBuffer.append(resourceManager);
    stringBuffer.append(TEXT_87);
    
				boolean setJobHistoryAddress = "true".equals(ElementParameterParser.getValue(mrconn, "__SET_JOBHISTORY_ADDRESS__"));
				if(setJobHistoryAddress) {
					String jobHistoryAddress = ElementParameterParser.getValue(mrconn,"__JOBHISTORY_ADDRESS__");
	    			
    stringBuffer.append(TEXT_88);
    stringBuffer.append(jobHistoryAddress);
    stringBuffer.append(TEXT_89);
    
				}
				boolean setSchedulerAddress = "true".equals(ElementParameterParser.getValue(mrconn, "__SET_SCHEDULER_ADDRESS__"));
				if(setSchedulerAddress) {
					String schedulerAddress = ElementParameterParser.getValue(mrconn,"__RESOURCEMANAGER_SCHEDULER_ADDRESS__");
				
    stringBuffer.append(TEXT_90);
    stringBuffer.append(schedulerAddress);
    stringBuffer.append(TEXT_91);
    
				}

				boolean crossPlatformSubmission = "true".equals(ElementParameterParser.getValue(mrconn, "__CROSS_PLATFORM_SUBMISSION__"));
				if((!isCustom && mrDistrib.doSupportCrossPlatformSubmission()) || (isCustom && useYarn && crossPlatformSubmission)) {
				
    stringBuffer.append(TEXT_92);
    
				}

				if(mrDistrib.doSupportCustomMRApplicationCP()){

    stringBuffer.append(TEXT_93);
    stringBuffer.append(mrDistrib.getCustomMRApplicationCP());
    stringBuffer.append(TEXT_94);
    
				}

				
    stringBuffer.append(TEXT_95);
    stringBuffer.append(mrDistrib.getYarnApplicationClasspath());
    stringBuffer.append(TEXT_96);
    
				boolean setStagingDirectory = "true".equals(ElementParameterParser.getValue(mrconn, "__SET_STAGING_DIRECTORY__"));
				if(setStagingDirectory) {
					String stagingDirectory = ElementParameterParser.getValue(mrconn, "__STAGING_DIRECTORY__");
				
    stringBuffer.append(TEXT_97);
    stringBuffer.append(stagingDirectory);
    stringBuffer.append(TEXT_98);
    
				}
			}

			boolean setMemory = "true".equals(ElementParameterParser.getValue(mrconn, "__SET_MEMORY__"));
			if(setMemory) {
				String mapMemory = ElementParameterParser.getValue(mrconn,"__MAPREDUCE_MAP_MEMORY_MB__");
				String reduceMemory = ElementParameterParser.getValue(mrconn,"__MAPREDUCE_REDUCE_MEMORY_MB__");
				String amMemory = ElementParameterParser.getValue(mrconn,"__YARN_APP_MAPREDUCE_AM_RESOURCE_MB__");
				
    stringBuffer.append(TEXT_99);
    stringBuffer.append(mapMemory);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(reduceMemory);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(amMemory);
    stringBuffer.append(TEXT_102);
    
			}
		}else{
		
    stringBuffer.append(TEXT_103);
    stringBuffer.append(mrJobTracker);
    stringBuffer.append(TEXT_104);
    
		}
		if(mrUseKrb){
            if(((isCustom || mrDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                
    stringBuffer.append(TEXT_105);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_106);
    stringBuffer.append(setMapRHadoopLogin ? mapRHadoopLogin : "\"kerberos\"");
    stringBuffer.append(TEXT_107);
    
            }

            if(useKeytab) {
                
    stringBuffer.append(TEXT_108);
    stringBuffer.append(userPrincipal);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(keytabPath);
    stringBuffer.append(TEXT_110);
    
			}
		
    stringBuffer.append(TEXT_111);
    stringBuffer.append(mrNNPrincipal);
    stringBuffer.append(TEXT_112);
    
			if((!isCustom && mrDistrib.doSupportKerberos() && mrDistrib.isHadoop1()) || (isCustom && !useYarn)) {
		
    stringBuffer.append(TEXT_113);
    stringBuffer.append(mrJTPrincipal);
    stringBuffer.append(TEXT_114);
    
			}

			if((!isCustom && mrDistrib.doSupportKerberos() && mrDistrib.isHadoop2()) || (isCustom && useYarn)) {
		
    stringBuffer.append(TEXT_115);
    stringBuffer.append(mrRMPrincipal);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(mrJHPrincipal);
    stringBuffer.append(TEXT_117);
    
			}
            if(((isCustom || mrDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                
    stringBuffer.append(TEXT_118);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_120);
    
            }
		} else {
		    // Mapr ticket
            if(((isCustom || mrDistrib.doSupportMapRTicket()) && useMapRTicket)) {
                passwordFieldName = "__MAPRTICKET_PASSWORD__";
                
    stringBuffer.append(TEXT_121);
    stringBuffer.append(setMapRHomeDir ? mapRHomeDir : "\"/opt/mapr\"" );
    stringBuffer.append(TEXT_122);
    
                if (setMapRHadoopLogin) {
                    
    stringBuffer.append(TEXT_123);
    stringBuffer.append(mapRHadoopLogin);
    stringBuffer.append(TEXT_124);
    
                } else {
                    
    stringBuffer.append(TEXT_125);
    
                }
                String cid = "mr";
                INode node = mrconn;
                
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_126);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_128);
    } else {
    stringBuffer.append(TEXT_129);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_130);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_131);
    }
    stringBuffer.append(TEXT_132);
    
                if(mrDistrib.doSupportMaprTicketV52API()){
                    
    stringBuffer.append(TEXT_133);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_137);
    
                } else {
                    
    stringBuffer.append(TEXT_138);
    stringBuffer.append(mapRTicketCluster);
    stringBuffer.append(TEXT_139);
    stringBuffer.append(mapRTicketUsername);
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(mapRTicketDuration);
    stringBuffer.append(TEXT_142);
    
                }
            }
		}
		if (mrDistrib.doSupportUseDatanodeHostname() && mrUseDatanodeHostname) {
		    
    stringBuffer.append(TEXT_143);
    
		}
		
    stringBuffer.append(TEXT_144);
    //For TDI-27581 add map and reduce memory
    	if(!"CUSTOM".equals(mrDistribution) && ("HDP_1_2".equals(mrVersion) || "HDP_1_3".equals(mrVersion))) {
        	String mapMemory = ElementParameterParser.getValue(mrconn,"__MAPRED_JOB_MAP_MEMORY_MB__");
        	String reduceMemory = ElementParameterParser.getValue(mrconn,"__MAPRED_JOB_REDUCE_MEMORY_MB__");
			
    stringBuffer.append(TEXT_145);
    stringBuffer.append(mapMemory);
    stringBuffer.append(TEXT_146);
    stringBuffer.append(reduceMemory);
    stringBuffer.append(TEXT_147);
    
    	}
		
    stringBuffer.append(TEXT_148);
    
		if(mrCustomProps!=null && mrCustomProps.size()>0){
			for(int i = 0; i < mrCustomProps.size(); i++){
				Map<String, String> mrCustomProp = mrCustomProps.get(i);
				
    stringBuffer.append(TEXT_149);
    stringBuffer.append(mrCustomProp.get("PROPERTY"));
    stringBuffer.append(TEXT_150);
    stringBuffer.append(mrCustomProp.get("VALUE"));
    stringBuffer.append(TEXT_151);
    
			}
		}
		
    stringBuffer.append(TEXT_152);
    if(mrNeedUserName){
    stringBuffer.append(TEXT_153);
    stringBuffer.append(mrUserName);
    stringBuffer.append(TEXT_154);
    }
    stringBuffer.append(TEXT_155);
    
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
            
    stringBuffer.append(TEXT_156);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(configurationNode, "__CLOUDERA_NAVIGATOR_PASSWORD__"));
    stringBuffer.append(TEXT_157);
    
        } else {
            
    stringBuffer.append(TEXT_158);
    stringBuffer.append( ElementParameterParser.getValue(configurationNode, "__CLOUDERA_NAVIGATOR_PASSWORD__"));
    stringBuffer.append(TEXT_159);
    
        }
        
    stringBuffer.append(TEXT_160);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(clientUrlCN);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(urlCN);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(urlMetadataCN);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(usernameCN);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(useAutocommit);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(disableSslValidation);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(apiVersion);
    stringBuffer.append(TEXT_174);
    
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
            
    stringBuffer.append(TEXT_175);
    stringBuffer.append(atlasApplicationPropertiesFilePath);
    stringBuffer.append(TEXT_176);
    
        }
        String atlasURL = ElementParameterParser.getValue(configurationNode,"__ATLAS_URL__");
        if(doRequireBasicAtlasAuthentification(configurationNode)){
            String atlasUsername = ElementParameterParser.getValue(configurationNode,"__ATLAS_USERNAME__");
            String passwordFieldName = "__ATLAS_PASSWORD__";
            if (ElementParameterParser.canEncrypt(configurationNode, passwordFieldName)) {
                
    stringBuffer.append(TEXT_177);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(configurationNode, passwordFieldName));
    stringBuffer.append(TEXT_178);
    
            } else {
                
    stringBuffer.append(TEXT_179);
    stringBuffer.append( ElementParameterParser.getValue(configurationNode, passwordFieldName));
    stringBuffer.append(TEXT_180);
    
            }
            
    stringBuffer.append(TEXT_181);
    stringBuffer.append(atlasURL);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(atlasUsername);
    stringBuffer.append(TEXT_183);
    
        } else {
            
    stringBuffer.append(TEXT_184);
    stringBuffer.append(atlasURL);
    stringBuffer.append(TEXT_185);
    
        }
        
    stringBuffer.append(TEXT_186);
    
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
        
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    
        for (IMetadataColumn column: metadataTable.getListColumns()) {
            
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_191);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_192);
    
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
        
    stringBuffer.append(TEXT_193);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_194);
    
        if ((node.getOutgoingConnections() != null)
            && (node.getOutgoingConnections().size() > 0)) {
            for (IConnection connection: node.getOutgoingConnections()) {
                if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                    for (IMetadataColumn column: connection.getMetadataTable().getListColumns()) {
                        
    stringBuffer.append(TEXT_195);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_197);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_198);
    
                    }
                }
            }
        } else  {
            for (IMetadataTable metadataTable: node.getMetadataList()) {
                for (IMetadataColumn column: metadataTable.getListColumns()) {
                    
    stringBuffer.append(TEXT_199);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_201);
    stringBuffer.append(org.talend.core.model.metadata.types.JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()));
    stringBuffer.append(TEXT_202);
    
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
        
    stringBuffer.append(TEXT_203);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(componentName);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(folderPath);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(folderType);
    stringBuffer.append(TEXT_207);
    
    }

    /**
     * Generate list of input nodes for a given component.
     * 
     * @param node the currentNode
     */
    public void generateInputNodeList(INode node) {
        
    stringBuffer.append(TEXT_208);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_209);
    
        java.util.List<IConnection> inputs = (java.util.List<IConnection>)node.getIncomingConnections();
        for (IConnection connection: inputs) {
            if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                
    stringBuffer.append(TEXT_210);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_211);
    stringBuffer.append(connection.getSource().getUniqueName());
    stringBuffer.append(TEXT_212);
    
            }
        }
    }

    /**
     * Generate list of output nodes for a given component.
     * 
     * @param node the currentNode
     */
    public void generateOutputNodeList(INode node) {
        
    stringBuffer.append(TEXT_213);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_214);
    
        java.util.List<IConnection> outputs = (java.util.List<IConnection>)node.getOutgoingConnections();
        for (IConnection connection: outputs) {
            if (connection.getLineStyle().hasConnectionCategory(org.talend.core.model.process.IConnectionCategory.DATA)) {
                
    stringBuffer.append(TEXT_215);
    stringBuffer.append(node.getUniqueName());
    stringBuffer.append(TEXT_216);
    stringBuffer.append(connection.getTarget().getUniqueName());
    stringBuffer.append(TEXT_217);
    
            }
        }
    }

    /**
     * Generate the code to call the method addNodeToLineage of rg.talend.cloudera.navigator.api.LineageCreator.
     * 
     * @param cid the cid of the current component
     */
    public void addNodeToLineage(String cid) {
        
    stringBuffer.append(TEXT_218);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_222);
    
    }

}

    
    TalendLineageAPI talendLineageAPI = new TalendLineageAPI();
    if (talendLineageAPI.doRequireLineageSupport(mrconn)) {
        
    stringBuffer.append(TEXT_223);
    
    }
    
    stringBuffer.append(TEXT_224);
    
        if (talendLineageAPI.doRequireLineageSupport(mrconn)) {
            List<? extends INode> graphicalNodes = process.getGraphicalNodes();
            for (INode gNode: graphicalNodes) {
                if (gNode.isActivate()) {
                    talendLineageAPI.generateColumnListFromOutputLink(gNode, gNode.getUniqueName());
                    talendLineageAPI.generateInputNodeList(gNode);
                    talendLineageAPI.generateOutputNodeList(gNode);
                    talendLineageAPI.addNodeToLineage(gNode.getUniqueName());
                }
            }
            Boolean dieOnError = talendLineageAPI.getDieOnError(mrconn);
            
    stringBuffer.append(TEXT_225);
    stringBuffer.append(dieOnError);
    stringBuffer.append(TEXT_226);
    
        }
        
    stringBuffer.append(TEXT_227);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_228);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_229);
    stringBuffer.append(jobFolderName);
    stringBuffer.append(TEXT_230);
    
                for(IContextParameter ctxParam :params){
                    String typeToGenerate = "String";
                    if(ctxParam.getType().equals("id_List Of Value") || ctxParam.getType().equals("id_File") || ctxParam.getType().equals("id_Directory")){
                        typeToGenerate = "String";
                    }else{
                        typeToGenerate = JavaTypesManager.getTypeToGenerate(ctxParam.getType(),true);
                    }
                    
    stringBuffer.append(TEXT_231);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_232);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_233);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_234);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_235);
    
                }
                
    stringBuffer.append(TEXT_236);
    
        if (talendLineageAPI.doRequireLineageSupport(mrconn)) {
            if(talendLineageAPI.doRequireClouderaNavigatorSupport(mrconn)){
                talendLineageAPI.generateClouderaNavigatorLinageCreator(mrconn);
            } else if(talendLineageAPI.doRequireAtlasSupport(mrconn)){
                talendLineageAPI.generateAtlasLinageCreator(mrconn);
            }
        }
        
    stringBuffer.append(TEXT_237);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_238);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_239);
    stringBuffer.append(jobFolderName);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(process.getName());
    stringBuffer.append(TEXT_241);
    stringBuffer.append(codeGenArgument.getCurrentProjectName().toLowerCase());
    stringBuffer.append(TEXT_242);
    stringBuffer.append(jobFolderName);
    stringBuffer.append(TEXT_243);
    
                for(IContextParameter ctxParam : params){
                
    stringBuffer.append(TEXT_244);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_245);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_246);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_247);
    stringBuffer.append(ctxParam.getName());
    stringBuffer.append(TEXT_248);
    
                }
                
    stringBuffer.append(TEXT_249);
    
            if(mrNeedUserName){
            
    stringBuffer.append(TEXT_250);
    stringBuffer.append(mrUserName);
    stringBuffer.append(TEXT_251);
    
            }
            
    stringBuffer.append(TEXT_252);
    
            if(mrNeedUserName){
            
    stringBuffer.append(TEXT_253);
    
            }
            
    stringBuffer.append(TEXT_254);
    stringBuffer.append(mrTempFolder);
    stringBuffer.append(TEXT_255);
    
                    if(isExecutedThroughWebHCat) {
                
    stringBuffer.append(TEXT_256);
        
                    }
                    else 
                    if(isGoogleDataprocDistribution) {
                
    stringBuffer.append(TEXT_257);
    
                    }
                
    stringBuffer.append(TEXT_258);
    
                            if(isCloudDistribution) {
                        
    stringBuffer.append(TEXT_259);
    
                            } else {
                                // Used to add the scheme prefix.
                        
    stringBuffer.append(TEXT_260);
    
                            }
                        
    stringBuffer.append(TEXT_261);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_262);
    }
    stringBuffer.append(TEXT_263);
    
                if(isCloudDistribution) {
                
    stringBuffer.append(TEXT_264);
    
                        // We don't need libjars With Google Dataproc 
                        if(isGoogleDataprocDistribution){
                        
    stringBuffer.append(TEXT_265);
    
                        }

    stringBuffer.append(TEXT_266);
     } else { 
    stringBuffer.append(TEXT_267);
     
                } 
    stringBuffer.append(TEXT_268);
    
				String[] commandLine = new String[] {};
				try {
					commandLine = ProcessorUtilities.getCommandLine("win32",true, process.getId(), "",org.talend.designer.runprocess.IProcessor.NO_STATISTICS,org.talend.designer.runprocess.IProcessor.NO_TRACES, new String[]{});
				} catch (ProcessorException e) {
					e.printStackTrace();
				}
				int indexlibjars = java.util.Arrays.asList(commandLine).indexOf("-libjars");
				if (indexlibjars != -1 && indexlibjars + 1 < commandLine.length) {

    stringBuffer.append(TEXT_269);
    
					for (String lib : commandLine[indexlibjars + 1].split(",")) {

    stringBuffer.append(TEXT_270);
    stringBuffer.append(lib.substring(lib.lastIndexOf('/') + 1));
    stringBuffer.append(TEXT_271);
    
					}

    stringBuffer.append(TEXT_272);
    
				}

    stringBuffer.append(TEXT_273);
    
	if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_274);
    
	}
    else 
    if(isGoogleDataprocDistribution) {

    stringBuffer.append(TEXT_275);
    
    }



    stringBuffer.append(TEXT_276);
     if(isCloudDistribution) { 
    stringBuffer.append(TEXT_277);
     } 
    stringBuffer.append(TEXT_278);
     if(isCloudDistribution) { 
    stringBuffer.append(TEXT_279);
     } 
    stringBuffer.append(TEXT_280);
    stringBuffer.append(TEXT_281);
    return stringBuffer.toString();
  }
}
