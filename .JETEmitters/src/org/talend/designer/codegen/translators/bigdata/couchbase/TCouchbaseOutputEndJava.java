package org.talend.designer.codegen.translators.bigdata.couchbase;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;

public class TCouchbaseOutputEndJava
{
  protected static String nl;
  public static synchronized TCouchbaseOutputEndJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCouchbaseOutputEndJava result = new TCouchbaseOutputEndJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "    \tclient_";
  protected final String TEXT_3 = ".shutdown();";
  protected final String TEXT_4 = NL + "\tglobalMap.put(\"";
  protected final String TEXT_5 = "_NB_LINE\",nb_line_";
  protected final String TEXT_6 = ");" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_7 = "_NB_LINE_UPDATED\",nb_line_update_";
  protected final String TEXT_8 = ");" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_9 = "_NB_LINE_INSERTED\",nb_line_inserted_";
  protected final String TEXT_10 = ");" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_11 = "_NB_LINE_INSERTED_UPDATED\",nb_line_inserted_updated_";
  protected final String TEXT_12 = ");" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_13 = "_NB_LINE_DELETED\",nb_line_deleted_";
  protected final String TEXT_14 = ");" + NL + "\tglobalMap.put(\"";
  protected final String TEXT_15 = "_NB_LINE_REJECTED\",nb_line_rejected_";
  protected final String TEXT_16 = ");" + NL + "\tint nb_line_failed_";
  protected final String TEXT_17 = " = nb_line_";
  protected final String TEXT_18 = " - nb_line_update_";
  protected final String TEXT_19 = " - nb_line_inserted_";
  protected final String TEXT_20 = " " + NL + "\t\t- nb_line_inserted_updated_";
  protected final String TEXT_21 = " - nb_line_deleted_";
  protected final String TEXT_22 = ";" + NL + "\t";
  protected final String TEXT_23 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_24 = " - Has inserted \" + nb_line_inserted_";
  protected final String TEXT_25 = " + \" records.\");" + NL + "\t\t";
  protected final String TEXT_26 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_27 = " - Has updated \" + nb_line_update_";
  protected final String TEXT_28 = " + \" records.\");" + NL + "\t\t";
  protected final String TEXT_29 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_30 = " - Has upserted \" + nb_line_inserted_updated_";
  protected final String TEXT_31 = " + \" records.\");" + NL + "\t\t";
  protected final String TEXT_32 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_33 = " - Has deleted \" + nb_line_deleted_";
  protected final String TEXT_34 = " + \" records.\");" + NL + "\t\t";
  protected final String TEXT_35 = NL + "\t\tlog.info(\"";
  protected final String TEXT_36 = " - \" + nb_line_failed_";
  protected final String TEXT_37 = " + \" records failed.\");" + NL + "\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    String useExistingConn = ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__");
	String dataAction = ElementParameterParser.getValue(node,"__DATA_ACTION__");
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
    if(!("true").equals(useExistingConn)){
    
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
	}
	String destination = ElementParameterParser.getValue(node, "__DESTINATION__");
	String strJobCid="";
	if(destination !=null && !("").equals(destination.trim()))
	    strJobCid=destination;
	else{
	    strJobCid= cid;
	}
	
    stringBuffer.append(TEXT_4);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(strJobCid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    
	if(isLog4jEnabled){
		if("add".equals(dataAction)){
		
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    
		}else if("replace".equals(dataAction)){
		
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    
		}else if("set".equals(dataAction)){
		
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
		}else if("delete".equals(dataAction)){
		
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
		}
		
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_37);
    
	}
	
    return stringBuffer.toString();
  }
}
