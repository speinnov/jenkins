package org.talend.designer.codegen.translators.mapreduce.output;

import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;

public class TParquetOutputMapperMrcodeJava
{
  protected static String nl;
  public static synchronized TParquetOutputMapperMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TParquetOutputMapperMrcodeJava result = new TParquetOutputMapperMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t" + NL + "\t\t\t\t\tpublic static class TalendParquetOutputMapper_";
  protected final String TEXT_2 = " extends MapReduceBase implements Mapper<NullWritable, ";
  protected final String TEXT_3 = "Struct, Void, Object> {" + NL + "" + NL + "\t\t\t\t\t\tprivate parquet.example.data.simple.SimpleGroupFactory factory;" + NL + "" + NL + "\t\t\t\t\t\tpublic void map(NullWritable key, ";
  protected final String TEXT_4 = "Struct value," + NL + "\t\t\t\t\t\t\t\torg.apache.hadoop.mapred.OutputCollector<Void, Object> output, org.apache.hadoop.mapred.Reporter reporter)" + NL + "\t\t\t\t\t\t\t\tthrows IOException {" + NL + "\t\t\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t\t\tparquet.example.data.Group group = factory.newGroup()";
  protected final String TEXT_5 = NL + "\t\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_6 = "\", ";
  protected final String TEXT_7 = "value.";
  protected final String TEXT_8 = " == null ? null : ";
  protected final String TEXT_9 = "new java.text.SimpleDateFormat(";
  protected final String TEXT_10 = ").format(value.";
  protected final String TEXT_11 = "))";
  protected final String TEXT_12 = NL + "\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_13 = "\", ";
  protected final String TEXT_14 = "value.";
  protected final String TEXT_15 = " == null ? null : ";
  protected final String TEXT_16 = "parquet.io.api.Binary.fromByteArray(value.";
  protected final String TEXT_17 = "))";
  protected final String TEXT_18 = NL + "\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_19 = "\", ";
  protected final String TEXT_20 = "value.";
  protected final String TEXT_21 = " == null ? null : ";
  protected final String TEXT_22 = "parquet.io.api.Binary.fromByteArray(BigDataParserUtils.parseTo_String(value.";
  protected final String TEXT_23 = ").getBytes()))";
  protected final String TEXT_24 = NL + "\t\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_25 = "\", value.";
  protected final String TEXT_26 = " == null ? null : value.";
  protected final String TEXT_27 = ".intValue())";
  protected final String TEXT_28 = NL + "\t\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_29 = "\", BigDataParserUtils.parseTo_int(value.";
  protected final String TEXT_30 = "))";
  protected final String TEXT_31 = NL + "\t\t\t\t\t\t\t\t\t\t.append(\"";
  protected final String TEXT_32 = "\", value.";
  protected final String TEXT_33 = ")";
  protected final String TEXT_34 = NL + "\t\t\t\t\t\t\t\t;" + NL + "\t\t" + NL + "\t\t\t\t\t\t\t\toutput.collect(null, group);" + NL + "\t\t\t\t\t\t}" + NL + "" + NL + "\t\t\t\t\t\tpublic void configure(JobConf conf) {" + NL + "\t\t\t\t\t\t\tfactory = new parquet.example.data.simple.SimpleGroupFactory(parquet.hadoop.example.GroupWriteSupport.getSchema(conf));" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\t\t\tpublic void close() throws IOException {\t\t\t\t\t" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\t\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;

INode node = (INode)codeGenArgument.getArgument();

String cid = node.getUniqueName();

List<IMetadataTable> metadatas = node.getMetadataList();

if ((metadatas!=null) && (metadatas.size() > 0)) {
    IMetadataTable metadata = metadatas.get(0);
    if (metadata != null) {
    	String folder = ElementParameterParser.getValue(node,"__FILENAME__");
		
		List< ? extends IConnection> conns = node.getIncomingConnections();
		if (conns != null){
		
			if (conns.size()>0){
		
				IConnection conn =conns.get(0);
				String connName = conn.getName();
		
				if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
					List<IMetadataColumn> columns = metadata.getListColumns();
					int nbColumns = columns.size();
					
					
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_4);
    
								for(int i=0; i<nbColumns; i++) {
									IMetadataColumn column = columns.get(i);
									String columnName = columns.get(i).getLabel();
									String outputType = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
									JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
									if(javaType == JavaTypesManager.DATE) {

    stringBuffer.append(TEXT_5);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_6);
     if(column.isNullable()) { 
    stringBuffer.append(TEXT_7);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_8);
     } 
    stringBuffer.append(TEXT_9);
    stringBuffer.append(column.getPattern());
    stringBuffer.append(TEXT_10);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_11);
    
									} else if(javaType == JavaTypesManager.BYTE_ARRAY) {

    stringBuffer.append(TEXT_12);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_13);
     if(column.isNullable()) { 
    stringBuffer.append(TEXT_14);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_15);
     } 
    stringBuffer.append(TEXT_16);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_17);
    
									} else if(javaType == JavaTypesManager.CHARACTER || javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.BIGDECIMAL) {

    stringBuffer.append(TEXT_18);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_19);
     if(column.isNullable()) { 
    stringBuffer.append(TEXT_20);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_21);
     } 
    stringBuffer.append(TEXT_22);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_23);
    										
									} else if(javaType == JavaTypesManager.SHORT || javaType == JavaTypesManager.BYTE) {
										if(column.isNullable()) {

    stringBuffer.append(TEXT_24);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_27);
    										
										} else {

    stringBuffer.append(TEXT_28);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_30);
    										
										}
									} else {

    stringBuffer.append(TEXT_31);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_33);
    
									}
								}

    stringBuffer.append(TEXT_34);
    
				}
			}
		}
	}		
}

    return stringBuffer.toString();
  }
}
