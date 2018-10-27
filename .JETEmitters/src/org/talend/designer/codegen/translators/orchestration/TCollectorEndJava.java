package org.talend.designer.codegen.translators.orchestration;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.EConnectionType;
import java.util.List;

public class TCollectorEndJava
{
  protected static String nl;
  public static synchronized TCollectorEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCollectorEndJava result = new TCollectorEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "}//end if" + NL + "globalMap.put(\"";
  protected final String TEXT_2 = "_NB_LINE\",counter_";
  protected final String TEXT_3 = ");" + NL + "" + NL + "finished_";
  protected final String TEXT_4 = " = globalMap.get(\"FINISHED_";
  protected final String TEXT_5 = "\") == null ? true : (Boolean)globalMap.get(\"FINISHED_";
  protected final String TEXT_6 = "\");" + NL + "}//end loop" + NL + "globalMap.put(\"COMPLETED_";
  protected final String TEXT_7 = "\",Boolean.TRUE);" + NL + "} catch (Exception ex) {" + NL + "\tglobalMap.put(\"HAS_ERROR\",ex);";
  protected final String TEXT_8 = NL + "java.util.List<java.util.concurrent.Future<Object>> futures_";
  protected final String TEXT_9 = " = " + NL + "\t(java.util.List<java.util.concurrent.Future<Object>>)globalMap.get(\"FUTURES_LIST_";
  protected final String TEXT_10 = "\");" + NL + "int i_";
  protected final String TEXT_11 = " = ";
  protected final String TEXT_12 = ";" + NL + "java.util.concurrent.Future<Object> future_";
  protected final String TEXT_13 = " = null;" + NL + "while (i_";
  protected final String TEXT_14 = " < futures_";
  protected final String TEXT_15 = ".size()) {" + NL + "\tfuture_";
  protected final String TEXT_16 = " = futures_";
  protected final String TEXT_17 = ".get(i_";
  protected final String TEXT_18 = ");" + NL + "    if (!future_";
  protected final String TEXT_19 = ".isDone()) {" + NL + "        future_";
  protected final String TEXT_20 = ".cancel(true);" + NL + "    }" + NL + "    i_";
  protected final String TEXT_21 = "++;" + NL + "}";
  protected final String TEXT_22 = NL + "  " + NL + "  throw ex;" + NL + "}" + NL + "" + NL + "\t";
  protected final String TEXT_23 = NL + "\t\tlog.info(\"";
  protected final String TEXT_24 = "[\" + Thread.currentThread().getName() + \"] - Done.\");" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
	INode tCollector = node;
	String numPartitions = null;
	String tPartitioner_cid = null;
	List<? extends IConnection> inConns = tCollector.getIncomingConnections(EConnectionType.STARTS);
	if (inConns !=null && inConns.size() > 0 ) {
		tPartitioner_cid = inConns.get(0).getSource().getUniqueName();
		numPartitions = ElementParameterParser.getValue(inConns.get(0).getSource(), "__NUM_PARTITIONS__");
	}
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));	

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    
if (numPartitions != null) {

    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(tPartitioner_cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(numPartitions );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    
}

    stringBuffer.append(TEXT_22);
    if(isLog4jEnabled){
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    }
    return stringBuffer.toString();
  }
}
