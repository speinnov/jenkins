package org.talend.designer.codegen.translators.mapreduce.output;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TParquetOutputMapperMrconfigJava
{
  protected static String nl;
  public static synchronized TParquetOutputMapperMrconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TParquetOutputMapperMrconfigJava result = new TParquetOutputMapperMrconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t\t\t\tChainReducer.addMapper(job, (Class<? extends Mapper<NullWritable, ";
  protected final String TEXT_2 = "Struct, Void, Object>>) TalendParquetOutputMapper_";
  protected final String TEXT_3 = ".class,\tNullWritable.class, ";
  protected final String TEXT_4 = "Struct.class, Void.class, Text.class, true, new JobConf(false));";
  protected final String TEXT_5 = NL + "\t\t\t\t\t\tchainMapper.addMapper(job,\t(Class<? extends Mapper<NullWritable, ";
  protected final String TEXT_6 = "Struct, Void, Object>>) TalendParquetOutputMapper_";
  protected final String TEXT_7 = ".class,\tNullWritable.class, ";
  protected final String TEXT_8 = "Struct.class, Void.class, Text.class, true, new JobConf(false));";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;

INode node = (INode)codeGenArgument.getArgument();

String cid = node.getUniqueName();

List<IMetadataTable> metadatas = node.getMetadataList();

if(metadatas!=null && metadatas.size() > 0) {
    IMetadataTable metadata = metadatas.get(0);
    if(metadata != null){
    
    	String folder = ElementParameterParser.getValue(node,"__FILENAME__");
    	String fileAction = ElementParameterParser.getValue(node, "__FILE_ACTION__");
   	
		List< ? extends IConnection> conns = node.getIncomingConnections();
		if(conns != null){
		
			if(conns.size()>0){
		
				IConnection conn =conns.get(0);
				String connName = conn.getName();

				if(conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){
					List<IMetadataColumn> columns = metadata.getListColumns();
					if(((org.talend.core.model.process.AbstractNode)node).isMapOnlyAfterReduce()){

    stringBuffer.append(TEXT_1);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_4);
    
					} else {

    stringBuffer.append(TEXT_5);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_8);
    
					}
				}
			}					
		}
	}		
}

    return stringBuffer.toString();
  }
}
