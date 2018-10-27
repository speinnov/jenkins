package org.talend.designer.codegen.translators.orchestration;

import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.ElementParameterParser;
import java.util.List;

public class TParallelizeBeginJava
{
  protected static String nl;
  public static synchronized TParallelizeBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TParallelizeBeginJava result = new TParallelizeBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "// call parallelized subjobs";
  protected final String TEXT_3 = NL + "\tjava.util.Map<String, Thread> mt_";
  protected final String TEXT_4 = " = new java.util.HashMap<String, Thread>();" + NL + "\t" + NL + "// clear the temporary values in the globalMap";
  protected final String TEXT_5 = NL + "\tglobalMap.remove(\"";
  protected final String TEXT_6 = "_SUBPROCESS_STATE\");";
  protected final String TEXT_7 = NL;
  protected final String TEXT_8 = NL + "\t\t\t\tjava.util.Map concurrentMap_temp_";
  protected final String TEXT_9 = ";" + NL + "\t\t\t\tif(globalMap instanceof java.util.HashMap){" + NL + "\t\t\t\t\tconcurrentMap_temp_";
  protected final String TEXT_10 = " = java.util.Collections.synchronizedMap(globalMap);" + NL + "\t\t\t\t}else{" + NL + "\t\t\t\t\tconcurrentMap_temp_";
  protected final String TEXT_11 = " = globalMap;" + NL + "\t\t\t\t}" + NL + "\t\t\t\tfinal java.util.Map concurrentMap_";
  protected final String TEXT_12 = " = concurrentMap_temp_";
  protected final String TEXT_13 = ";" + NL + "\t\t\t";
  protected final String TEXT_14 = NL + "\t\trunningThreadCount.add(1);" + NL + "\t\tString name_";
  protected final String TEXT_15 = " = \"";
  protected final String TEXT_16 = "_\" + java.util.UUID.randomUUID().toString();" + NL + "\t\tThread thread_";
  protected final String TEXT_17 = " = new Thread(new ThreadGroup(name_";
  protected final String TEXT_18 = "), name_";
  protected final String TEXT_19 = "){" + NL + "\t    \tpublic void run() {\t    \t" + NL + "\t\t\t\tjava.util.Map threadRunResultMap = new java.util.HashMap();" + NL + "\t\t\t\tthreadRunResultMap.put(\"errorCode\", null);" + NL + "\t\t\t\tthreadRunResultMap.put(\"status\", \"\");" + NL + "\t\t\t\tthreadLocal.set(threadRunResultMap);" + NL + "\t\t\t\t\t    \t" + NL + "                try {" + NL + "\t\t\t\t\t";
  protected final String TEXT_20 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_21 = " - The subjob starting with the component '";
  protected final String TEXT_22 = "' starts.\");" + NL + "\t\t\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\t\t";
  protected final String TEXT_24 = "Process(concurrentMap_";
  protected final String TEXT_25 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_26 = NL + "\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_27 = " - The subjob starting with the component '";
  protected final String TEXT_28 = "' is done.\");" + NL + "\t\t\t\t\t";
  protected final String TEXT_29 = NL + "\t\t\t\t} catch (TalendException e) {" + NL + "\t\t\t\t\t";
  protected final String TEXT_30 = NL + "\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_31 = " - \" + e.getMessage());" + NL + "\t\t\t\t\t";
  protected final String TEXT_32 = NL + "\t\t\t\t\tconcurrentMap_";
  protected final String TEXT_33 = ".put(\"";
  protected final String TEXT_34 = "_SUBPROCESS_STATE\", -1);" + NL + "\t\t\t\t\te.printStackTrace();" + NL + "\t\t\t\t} catch (java.lang.Error error) {" + NL + "\t\t\t\t\t";
  protected final String TEXT_35 = NL + "\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_36 = " - \" + error.getMessage());" + NL + "\t\t\t\t\t";
  protected final String TEXT_37 = NL + "\t\t\t\t\tconcurrentMap_";
  protected final String TEXT_38 = ".put(\"";
  protected final String TEXT_39 = "_SUBPROCESS_STATE\", -1);" + NL + "\t\t\t\t\terror.printStackTrace();" + NL + "\t\t\t\t}finally {" + NL + "                \trunningThreadCount.add(-1);" + NL + "                }\t        \t" + NL + "\t    \t}" + NL + "\t\t};" + NL + "\t\tthread_";
  protected final String TEXT_40 = ".start();" + NL + "\t\tmt_";
  protected final String TEXT_41 = ".put(\"";
  protected final String TEXT_42 = "\",thread_";
  protected final String TEXT_43 = ");";
  protected final String TEXT_44 = NL + "\twhile(" + NL + "(";
  protected final String TEXT_45 = NL + "\t\t((globalMap.get(\"";
  protected final String TEXT_46 = "_SUBPROCESS_STATE\")==null)?true:((Integer)globalMap.get(\"";
  protected final String TEXT_47 = "_SUBPROCESS_STATE\")";
  protected final String TEXT_48 = "0))";
  protected final String TEXT_49 = " ";
  protected final String TEXT_50 = ") ";
  protected final String TEXT_51 = NL + "&&  (";
  protected final String TEXT_52 = NL + "\t\t((globalMap.get(\"";
  protected final String TEXT_53 = "_SUBPROCESS_STATE\")==null)?true:((Integer)globalMap.get(\"";
  protected final String TEXT_54 = "_SUBPROCESS_STATE\") != -1)) &&";
  protected final String TEXT_55 = " true)";
  protected final String TEXT_56 = " ) {" + NL + "\tThread.sleep(";
  protected final String TEXT_57 = ");" + NL + "}";
  protected final String TEXT_58 = NL + "// Die on error" + NL + "// if one subjob fails,all other subjob will be shut down." + NL + "if(";
  protected final String TEXT_59 = NL + "\t\t((globalMap.get(\"";
  protected final String TEXT_60 = "_SUBPROCESS_STATE\")==null)?false:((Integer)globalMap.get(\"";
  protected final String TEXT_61 = "_SUBPROCESS_STATE\")";
  protected final String TEXT_62 = "-1)) || ";
  protected final String TEXT_63 = NL + "false){" + NL + "    for(Thread thread_";
  protected final String TEXT_64 = " : mt_";
  protected final String TEXT_65 = ".values()){" + NL + "\t\tthread_";
  protected final String TEXT_66 = ".stop();" + NL + "\t}" + NL + "\tthrow new Exception(\"At least one of the subjobs in tParallelize fails\");" + NL + "}";
  protected final String TEXT_67 = NL + "// wait for all" + NL + "// the following code will break only when all subjobs are done." + NL + "\twhile(";
  protected final String TEXT_68 = NL + "\t\t((globalMap.get(\"";
  protected final String TEXT_69 = "_SUBPROCESS_STATE\")==null)?true:((Integer)globalMap.get(\"";
  protected final String TEXT_70 = "_SUBPROCESS_STATE\")";
  protected final String TEXT_71 = "0))";
  protected final String TEXT_72 = NL;
  protected final String TEXT_73 = ") {" + NL + "\tThread.sleep(";
  protected final String TEXT_74 = ");" + NL + "}";
  protected final String TEXT_75 = NL + NL + NL + "// call next subprocesses";
  protected final String TEXT_76 = NL + "\tlog.debug(\"";
  protected final String TEXT_77 = " - The subjob starting with the component '";
  protected final String TEXT_78 = "' starts.\");";
  protected final String TEXT_79 = NL + "\t";
  protected final String TEXT_80 = "Process(globalMap);";
  protected final String TEXT_81 = NL + "\tlog.debug(\"";
  protected final String TEXT_82 = " - The subjob starting with the component '";
  protected final String TEXT_83 = "' is done.\");";
  protected final String TEXT_84 = NL;
  protected final String TEXT_85 = NL;
  protected final String TEXT_86 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
     
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	String waitFor = ElementParameterParser.getValue(node,"__WAIT_FOR__");
	String sleepTime = ElementParameterParser.getValue(node,"__SLEEPTIME__");
	
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
	/* Build the Log4J message for the different parameters */

	
	/*fixbug 18455,whether we choose "end of all subjobs" or "end of first subjob",when we choose "dieOnError",
	 *all subjobs will be shutdown when a job throws exception.So only when we choose "end of all subjobs" and do not choose "dieOnError"
	 *otherjobs can be executed after all subjobs finish.
	*/
	String operator="";
	String endOperator="";
	String internalOperator="==";
	String internalFlag="";
	if (waitFor.compareTo("All")==0) {
		operator="||";
		endOperator="false";
	} else {
		operator="&&";
		endOperator="true";
	}
	
	String dieOnErrorStr = ElementParameterParser.getValue(node, "__DIE_ON_ERROR__");
	boolean dieOnError = (dieOnErrorStr!=null&&!("").equals(dieOnErrorStr))?("true").equals(dieOnErrorStr):false; 

    stringBuffer.append(TEXT_2);
    
	List< ? extends IConnection> conns = node.getOutgoingConnections();
	int parallelizeCount = 0;

    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
     
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {

    stringBuffer.append(TEXT_5);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_6);
    
		}
	}

    stringBuffer.append(TEXT_7);
    
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {
			parallelizeCount ++;
			
			if(parallelizeCount == 1){
			
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    
			}
			
    stringBuffer.append(TEXT_14);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_19);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_22);
    }
    stringBuffer.append(TEXT_23);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_28);
    }
    stringBuffer.append(TEXT_29);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    }
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_34);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    }
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(conn.getTarget().getUniqueName());
    stringBuffer.append(TEXT_42);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_43);
    
		}
	}
	
	if(parallelizeCount > 0) { // TDI-17929_START: we generate this part of code if and only if there is at least 1 parallelize output link

    stringBuffer.append(TEXT_44);
     
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {

    stringBuffer.append(TEXT_45);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_46);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(internalOperator );
    stringBuffer.append(TEXT_48);
    stringBuffer.append(operator );
    
		}
	}

    stringBuffer.append(TEXT_49);
    stringBuffer.append(endOperator );
    stringBuffer.append(TEXT_50);
    
if (dieOnError) {

    stringBuffer.append(TEXT_51);
    
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {

    stringBuffer.append(TEXT_52);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_53);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_54);
    
		}
	}

    stringBuffer.append(TEXT_55);
    
}

    stringBuffer.append(TEXT_56);
    stringBuffer.append(sleepTime );
    stringBuffer.append(TEXT_57);
    
if (dieOnError) {

    stringBuffer.append(TEXT_58);
    
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {

    stringBuffer.append(TEXT_59);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_60);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_61);
    stringBuffer.append(internalOperator );
    stringBuffer.append(TEXT_62);
    
		}
	}

    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    
}

    
if (dieOnError && waitFor.compareTo("All")==0) {
		operator="||";
		endOperator="false";
		internalOperator="==";

    stringBuffer.append(TEXT_67);
     
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.PARALLELIZE)) {

    stringBuffer.append(TEXT_68);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_69);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_70);
    stringBuffer.append(internalOperator );
    stringBuffer.append(TEXT_71);
    stringBuffer.append(operator );
    
		}
	}

    stringBuffer.append(TEXT_72);
    stringBuffer.append(endOperator );
    stringBuffer.append(TEXT_73);
    stringBuffer.append(sleepTime );
    stringBuffer.append(TEXT_74);
    
}
} // TDI-17929_CLOSE: we generate this part of code if and only if there is at least 1 parallelize output link

    stringBuffer.append(TEXT_75);
     
	for (IConnection conn : conns) {
		if (conn.getLineStyle().equals(org.talend.core.model.process.EConnectionType.SYNCHRONIZE)) {

    if(isLog4jEnabled){
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_78);
    }
    stringBuffer.append(TEXT_79);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_80);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(conn.getTarget().getUniqueName() );
    stringBuffer.append(TEXT_83);
    }
    stringBuffer.append(TEXT_84);
    
		}
	}

    stringBuffer.append(TEXT_85);
    stringBuffer.append(TEXT_86);
    return stringBuffer.toString();
  }
}
