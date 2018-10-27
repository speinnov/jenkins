package org.talend.designer.codegen.translators.processing.hadoop;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;

public class TPigDistinctMainJava
{
  protected static String nl;
  public static synchronized TPigDistinctMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TPigDistinctMainJava result = new TPigDistinctMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t";
  protected final String TEXT_3 = NL + "\t";
  protected final String TEXT_4 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_5 = " - Retrieving records from the datasource.\");" + NL + "\t\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_7 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_8 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_10 = " - Retrieved records count: \"+ globalMap.get(\"";
  protected final String TEXT_11 = "_NB_LINE\") + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_12 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_13 = " - Retrieved records count: \"+ nb_line_";
  protected final String TEXT_14 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_15 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_16 = " - Written records count: \" + nb_line_";
  protected final String TEXT_17 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_18 = NL + "\t\t\t\tfinal StringBuffer log4jSb_";
  protected final String TEXT_19 = " = new StringBuffer();" + NL + "\t\t\t";
  protected final String TEXT_20 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_21 = " - Retrieving the record \" + (nb_line_";
  protected final String TEXT_22 = ") + \".\");" + NL + "\t\t\t";
  protected final String TEXT_23 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_24 = " - Writing the record \" + nb_line_";
  protected final String TEXT_25 = " + \" to the file.\");" + NL + "\t\t\t";
  protected final String TEXT_26 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_27 = " - Processing the record \" + nb_line_";
  protected final String TEXT_28 = " + \".\");" + NL + "\t\t\t";
  protected final String TEXT_29 = NL + "\t\t\t\tlog.debug(\"";
  protected final String TEXT_30 = " - Processed records count: \" + nb_line_";
  protected final String TEXT_31 = " + \" .\");" + NL + "\t\t\t";
  protected final String TEXT_32 = "\t" + NL + "\t" + NL + "\troutines.system.PigHelper helper_";
  protected final String TEXT_33 = " = new routines.system.PigHelper();" + NL + "\t" + NL + "\tStringBuilder sb_";
  protected final String TEXT_34 = " = new StringBuilder();" + NL + "\tsb_";
  protected final String TEXT_35 = ".append(\"";
  protected final String TEXT_36 = "_";
  protected final String TEXT_37 = "_RESULT = DISTINCT ";
  protected final String TEXT_38 = "_";
  protected final String TEXT_39 = "_RESULT\");";
  protected final String TEXT_40 = NL + "\t\tsb_";
  protected final String TEXT_41 = ".append(\" PARALLEL ";
  protected final String TEXT_42 = "\");";
  protected final String TEXT_43 = NL + "\tsb_";
  protected final String TEXT_44 = ".append(\";\");" + NL + "\t" + NL + "\thelper_";
  protected final String TEXT_45 = ".add(\"query\",sb_";
  protected final String TEXT_46 = ".toString());" + NL + "\t" + NL + "\t";
  protected final String TEXT_47 = NL + "\tjava.util.List<String[]> pigLatins_";
  protected final String TEXT_48 = " = helper_";
  protected final String TEXT_49 = ".getPigLatins();";
  protected final String TEXT_50 = NL + "\t\tfor(String[] pigLatin_";
  protected final String TEXT_51 = " : pigLatins_";
  protected final String TEXT_52 = ") {" + NL + "\t\t\tString type_";
  protected final String TEXT_53 = " = pigLatin_";
  protected final String TEXT_54 = "[0];" + NL + "\t\t\t";
  protected final String TEXT_55 = NL + "\t\t\tlog.info(\"";
  protected final String TEXT_56 = " - register \" + type_";
  protected final String TEXT_57 = " + \" : \" + pigLatin_";
  protected final String TEXT_58 = "[1]);" + NL + "\t\t\t";
  protected final String TEXT_59 = NL + "\t\t\tif(\"query\".equals(type_";
  protected final String TEXT_60 = ")) {";
  protected final String TEXT_61 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_62 = ".write(pigLatin_";
  protected final String TEXT_63 = "[1]);";
  protected final String TEXT_64 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_65 = ".registerQuery(pigLatin_";
  protected final String TEXT_66 = "[1]);";
  protected final String TEXT_67 = NL + "\t\t\t} else if(\"jar\".equals(type_";
  protected final String TEXT_68 = ")) {";
  protected final String TEXT_69 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_70 = ".write(\"REGISTER \" + wasbPath_";
  protected final String TEXT_71 = " + new java.io.File(pigLatin_";
  protected final String TEXT_72 = "[1]).getName() + \";\");" + NL + "\t\t\t\t\tlibjars_";
  protected final String TEXT_73 = ".append(pigLatin_";
  protected final String TEXT_74 = "[1] + \",\");";
  protected final String TEXT_75 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_76 = ".registerJar(pigLatin_";
  protected final String TEXT_77 = "[1]);";
  protected final String TEXT_78 = NL + "\t\t\t} else if(\"script\".equals(type_";
  protected final String TEXT_79 = ")) {";
  protected final String TEXT_80 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_81 = ".write(pigLatin_";
  protected final String TEXT_82 = "[1]);";
  protected final String TEXT_83 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_84 = ".registerScript(new java.io.ByteArrayInputStream(pigLatin_";
  protected final String TEXT_85 = "[1].getBytes()));";
  protected final String TEXT_86 = NL + "\t\t\t} else if(\"function\".equals(type_";
  protected final String TEXT_87 = ")) {";
  protected final String TEXT_88 = NL + "\t\t\t\t\tbw_";
  protected final String TEXT_89 = ".write(\"DEFINE \" + pigLatin_";
  protected final String TEXT_90 = "[1] + \" \" + pigLatin_";
  protected final String TEXT_91 = "[2] + \";\");";
  protected final String TEXT_92 = NL + "\t\t\t\t\tpigServer_";
  protected final String TEXT_93 = ".registerFunction(pigLatin_";
  protected final String TEXT_94 = "[1], new org.apache.pig.FuncSpec(pigLatin_";
  protected final String TEXT_95 = "[2]));";
  protected final String TEXT_96 = NL + "\t\t\t}" + NL + "\t\t}";
  protected final String TEXT_97 = NL + "    \tpigScript_";
  protected final String TEXT_98 = ".addAll(pigLatins_";
  protected final String TEXT_99 = ");";
  protected final String TEXT_100 = NL + "\tpigLatins_";
  protected final String TEXT_101 = ".clear();" + NL;
  protected final String TEXT_102 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	String previous_node="";
	String start_node="";
	
	boolean inMain = true;
	boolean isExecutedThroughWebHCat = false;

	String previousOutputConnectionName = "";
	
	if(node.getIncomingConnections()!=null && node.getIncomingConnections().size()>0) {
		IConnection connection = node.getIncomingConnections().get(0);
		previous_node = connection.getSource().getUniqueName();
		INode loadNode = node.getDesignSubjobStartNode();
		start_node = loadNode.getUniqueName();

		boolean isLocal = "true".equals(ElementParameterParser.getValue(loadNode, "__LOCAL__"));

        if(!isLocal) {
            String loadDistribution = ElementParameterParser.getValue(loadNode, "__DISTRIBUTION__");
            String loadPigVersion = ElementParameterParser.getValue(loadNode, "__PIG_VERSION__");
            org.talend.hadoop.distribution.component.PigComponent pigDistrib = null;
            try {
                pigDistrib = (org.talend.hadoop.distribution.component.PigComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(loadDistribution, loadPigVersion);
            } catch (java.lang.Exception e) {
                e.printStackTrace();
                return "";
            }

            boolean isCustom = pigDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;
            isExecutedThroughWebHCat = !isCustom && pigDistrib != null && pigDistrib.isExecutedThroughWebHCat();
        }

		inMain = loadNode.isSubtreeStart();
		previousOutputConnectionName = connection.getName();
	}
	
	String outputConnectionName = "";
	List<IConnection> outputConnections = (List<IConnection>)node.getOutgoingConnections();
	if(outputConnections!=null && outputConnections.size()>0) {
		outputConnectionName = outputConnections.get(0).getName();
	}
	
	boolean useParallel = "true".equals(ElementParameterParser.getValue(node, "__INCREASE_PARALLELISM__"));
	String reduce_number = ElementParameterParser.getValue(node, "__REDUCE_NUMBER__");
	

    stringBuffer.append(TEXT_2);
    stringBuffer.append(TEXT_3);
    
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
			
    stringBuffer.append(TEXT_4);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_5);
    
			}
		}
		
		public void retrievedDataNumberInfo() {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_6);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
			}
		}
		
		public void retrievedDataNumberInfoFromGlobalMap(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_9);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    
			}
		}
		
		//for all tFileinput* components 
		public void retrievedDataNumberInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_12);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    
			}
		}
		
		public void writeDataFinishInfo(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_15);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    
			}
		}
		
 		//TODO delete it and remove all log4jSb parameter from components
		public void componentStartInfo(INode node) {
			if (isLog4jEnabled) {
			
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node,boolean hasIncreased) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_20);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(hasIncreased?"":"+1");
    stringBuffer.append(TEXT_22);
    
			}
		}
		
		//TODO rename or delete it
		public void debugRetriveData(INode node) {
			debugRetriveData(node,true);
		}
		
		//TODO rename or delete it
		public void debugWriteData(INode node) {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_23);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    
			}
		}
		
		public void logCurrentRowNumberInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_26);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    
			}
		}
		
		public void logDataCountInfo() {
			if(isLog4jEnabled){
			
    stringBuffer.append(TEXT_29);
    stringBuffer.append(label);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_31);
    
			}
		}
	}
	
	final DefaultLog4jFileUtil log4jFileUtil = new DefaultLog4jFileUtil((INode)(((org.talend.designer.codegen.config.CodeGeneratorArgument)argument).getArgument()));
	
    
	log4jFileUtil.componentStartInfo(node);
	final boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(outputConnectionName);
    stringBuffer.append(TEXT_37);
    stringBuffer.append(previous_node);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(previousOutputConnectionName);
    stringBuffer.append(TEXT_39);
    		
	if(useParallel) {

    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(reduce_number);
    stringBuffer.append(TEXT_42);
    
	}

    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    
	if(inMain) {

    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_58);
    }
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_61);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    					
				} else {

    stringBuffer.append(TEXT_64);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    
				}

    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_68);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_69);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_74);
    					
				} else {

    stringBuffer.append(TEXT_75);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_77);
    
				}

    stringBuffer.append(TEXT_78);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_79);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_80);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    					
				} else {

    stringBuffer.append(TEXT_83);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_85);
    
				}

    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_87);
    
				if(isExecutedThroughWebHCat) {

    stringBuffer.append(TEXT_88);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_89);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    					
				} else {

    stringBuffer.append(TEXT_92);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_94);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_95);
    
				}

    stringBuffer.append(TEXT_96);
    
	} else {

    stringBuffer.append(TEXT_97);
    stringBuffer.append(start_node);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_99);
    
	}

    stringBuffer.append(TEXT_100);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(TEXT_102);
    return stringBuffer.toString();
  }
}
