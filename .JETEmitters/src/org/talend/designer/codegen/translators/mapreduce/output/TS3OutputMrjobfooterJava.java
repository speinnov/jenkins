package org.talend.designer.codegen.translators.mapreduce.output;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TS3OutputMrjobfooterJava
{
  protected static String nl;
  public static synchronized TS3OutputMrjobfooterJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TS3OutputMrjobfooterJava result = new TS3OutputMrjobfooterJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = NL + "\t";
  protected final String TEXT_2 = NL + "\t\t\t\t" + NL + "    \t\t\tPath sourceDirPath_";
  protected final String TEXT_3 = " = new Path(outputPath_";
  protected final String TEXT_4 = ");" + NL + "    \t\t\tif(fs.exists(sourceDirPath_";
  protected final String TEXT_5 = ")) {" + NL + "    \t\t\t\tPath targetFilePath_";
  protected final String TEXT_6 = " = new Path(";
  protected final String TEXT_7 = ");" + NL + "    \t\t\t\t" + NL + "    \t\t\t\t";
  protected final String TEXT_8 = NL + "    \t\t\t\tif(fs.exists(targetFilePath_";
  protected final String TEXT_9 = ") && !fs.getFileStatus(targetFilePath_";
  protected final String TEXT_10 = ").isDir()) {" + NL + "    \t\t\t\t\tfs.delete(targetFilePath_";
  protected final String TEXT_11 = ",false);" + NL + "    \t\t\t\t}" + NL + "    \t\t\t\t";
  protected final String TEXT_12 = NL + "    \t\t\t\t\tint headerByteCount_";
  protected final String TEXT_13 = " = \"";
  protected final String TEXT_14 = "\".getBytes(";
  protected final String TEXT_15 = ").length + ";
  protected final String TEXT_16 = ".getBytes(";
  protected final String TEXT_17 = ").length;" + NL + "    \t\t\t\t\torg.talend.hadoop.fs.FileUtil.copyMerge(fs,sourceDirPath_";
  protected final String TEXT_18 = ",fs,targetFilePath_";
  protected final String TEXT_19 = ",";
  protected final String TEXT_20 = ",getConf(), null, headerByteCount_";
  protected final String TEXT_21 = ");" + NL + "    \t\t\t\t";
  protected final String TEXT_22 = NL + "    \t\t\t\t\torg.apache.hadoop.fs.FileUtil.copyMerge(fs,sourceDirPath_";
  protected final String TEXT_23 = ",fs,targetFilePath_";
  protected final String TEXT_24 = ",";
  protected final String TEXT_25 = ",getConf(), null);" + NL + "    \t\t\t\t";
  protected final String TEXT_26 = NL + "    \t\t\t}" + NL + "\t\t\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    
    List<IMetadataTable> metadatas = node.getMetadataList();
    if((metadatas != null) && (metadatas.size() > 0)){
        IMetadataTable metadata = metadatas.get(0);
        if(metadata != null){
        	boolean compress = "true".equals(ElementParameterParser.getValue(node, "__COMPRESS__"));	
			boolean merge = "true".equals(ElementParameterParser.getValue(node, "__MERGE_RESULT__"));
			String mergeTargetFilePath = ElementParameterParser.getValue(node, "__MERGE_PATH__");
			boolean removeMergeSourceDir = "true".equals(ElementParameterParser.getValue(node, "__REMOVE_FILE__"));
			boolean overwriteMergeTargetFile = "true".equals(ElementParameterParser.getValue(node, "__REPLACE_FILE__"));
			
			String typeFile = ElementParameterParser.getValue(node,"__TYPEFILE__");
			
		    List<? extends IConnection> inConns = node.getIncomingConnections(EConnectionType.FLOW_MAIN);
		    if(inConns != null && inConns.size() > 0){
		        
		    }else{
		        return "";
		    }
		    
			if(!compress && merge && "TEXT".equals(typeFile)) {
			
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(mergeTargetFilePath);
    stringBuffer.append(TEXT_7);
    if(overwriteMergeTargetFile){
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    }
    				
    				boolean includeHeader = "true".equals(ElementParameterParser.getValue(node, "__INCLUDEHEADER__"));
    				if(includeHeader){
    					String fieldSeparator = ElementParameterParser.getValue(node,"__FIELDSEPARATOR__");
    					String rowSeparator = ElementParameterParser.getValue(node,"__ROWSEPARATOR__");
    					boolean customEncoding = "true".equals(ElementParameterParser.getValue(node, "__CUSTOM_ENCODING__"));
        				String encoding = ElementParameterParser.getValue(node,"__ENCODING__");  
    					List<IMetadataColumn> columns = metadata.getListColumns();
    					String header = "";
    					int i = 1;
    					for(IMetadataColumn column : columns){
    						header += column.getLabel() + (i!=columns.size() ? fieldSeparator.substring(1,fieldSeparator.length()-1) : "");
    						i++;
    					}
    				
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(header);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(rowSeparator);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(removeMergeSourceDir);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    } else {
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(removeMergeSourceDir);
    stringBuffer.append(TEXT_25);
    }
    stringBuffer.append(TEXT_26);
    
			}
    	}
	}
	
    return stringBuffer.toString();
  }
}
