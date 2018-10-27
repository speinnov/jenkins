package org.talend.designer.codegen.translators.databases.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IBigDataNode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.core.model.utils.NodeUtil;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TCassandraInputSparkconfigJava
{
  protected static String nl;
  public static synchronized TCassandraInputSparkconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCassandraInputSparkconfigJava result = new TCassandraInputSparkconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\tif(true) {" + NL + "\t\tthrow new RuntimeException(\"";
  protected final String TEXT_2 = " component needs a tCassandraConfiguration to be defined within the job.\");\t" + NL + "\t}" + NL + "\torg.apache.spark.api.java.JavaRDD<";
  protected final String TEXT_3 = "> rdd_";
  protected final String TEXT_4 = " = null;\t";
  protected final String TEXT_5 = NL + "\torg.apache.spark.api.java.JavaRDD<";
  protected final String TEXT_6 = ">" + NL + "\t";
  protected final String TEXT_7 = NL + "\torg.apache.spark.api.java.JavaPairRDD<";
  protected final String TEXT_8 = ", Iterable<";
  protected final String TEXT_9 = ">>" + NL + "\t";
  protected final String TEXT_10 = NL + "\t    rdd_";
  protected final String TEXT_11 = " = com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions(ctx)" + NL + "\t    .cassandraTable(";
  protected final String TEXT_12 = ", ";
  protected final String TEXT_13 = ", com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo(";
  protected final String TEXT_14 = ".class))" + NL + "\t    .select(" + NL + "\t        ";
  protected final String TEXT_15 = NL + "\t                com.datastax.spark.connector.japi.CassandraJavaUtil.column(\"";
  protected final String TEXT_16 = "\")" + NL + "\t            ";
  protected final String TEXT_17 = NL + "\t                com.datastax.spark.connector.japi.CassandraJavaUtil.ttl(\"";
  protected final String TEXT_18 = "\")" + NL + "\t            ";
  protected final String TEXT_19 = NL + "\t                com.datastax.spark.connector.japi.CassandraJavaUtil.writeTime(\"";
  protected final String TEXT_20 = "\")" + NL + "\t            ";
  protected final String TEXT_21 = NL + "\t            ";
  protected final String TEXT_22 = ".as(\"";
  protected final String TEXT_23 = "\")";
  protected final String TEXT_24 = ",";
  protected final String TEXT_25 = NL + "\t        ";
  protected final String TEXT_26 = NL + "\t    )" + NL + "\t    ";
  protected final String TEXT_27 = NL + "\t        .where(";
  protected final String TEXT_28 = " + \" ";
  protected final String TEXT_29 = " ?\", ";
  protected final String TEXT_30 = ")" + NL + "\t    ";
  protected final String TEXT_31 = NL + "\t    ";
  protected final String TEXT_32 = NL + "\t        .limit(";
  protected final String TEXT_33 = ")" + NL + "\t    ";
  protected final String TEXT_34 = NL + "\t    ";
  protected final String TEXT_35 = NL + "\t        .withAscOrder()" + NL + "\t    ";
  protected final String TEXT_36 = NL + "\t        .withDescOrder()" + NL + "\t    ";
  protected final String TEXT_37 = NL + "\t    ";
  protected final String TEXT_38 = NL + "\t        .spanBy(" + NL + "\t            new org.apache.spark.api.java.function.Function<";
  protected final String TEXT_39 = ", ";
  protected final String TEXT_40 = ">(){" + NL + "\t                public ";
  protected final String TEXT_41 = " call(";
  protected final String TEXT_42 = " row) throws Exception {" + NL + "\t                    ";
  protected final String TEXT_43 = " result = new ";
  protected final String TEXT_44 = "();" + NL + "\t                    ";
  protected final String TEXT_45 = NL + "\t                        result.set";
  protected final String TEXT_46 = "(row.get";
  protected final String TEXT_47 = "());" + NL + "\t                    ";
  protected final String TEXT_48 = NL + "\t                    return result;" + NL + "\t                }" + NL + "\t            },";
  protected final String TEXT_49 = ".class" + NL + "\t        )" + NL + "\t    ";
  protected final String TEXT_50 = NL + "\t    ;";
  protected final String TEXT_51 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
// Parse the inputs to this javajet generator.
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode) codeGenArgument.getArgument();
final String cid = node.getUniqueName();

    
IMetadataTable metadata = null;
IConnection conn = null;
List<IMetadataColumn> columns = null;
List<IMetadataTable> metadatas = node.getMetadataList();
if((metadatas != null) && (metadatas.size() > 0)){
    metadata = metadatas.get(0);
    if(metadata != null){
        columns = metadata.getListColumns();
    }
}

List<? extends IConnection> conns = node.getOutgoingSortedConnections();
if(conns != null && conns.size() > 0 && conns.get(0).getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){
    conn = conns.get(0);
}

if(columns == null || columns.isEmpty() || conn == null){
    return "";
}
String outRowStruct = codeGenArgument.getRecordStructName(conn);
String connName = conn.getName();

java.util.List<? extends INode> configurationNodes = node.getProcess().getNodesOfType("tCassandraConfiguration");
INode configurationNode = null;
if (configurationNodes != null && configurationNodes.size() > 0) {
    configurationNode = configurationNodes.get(0);
} else {

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_4);
    
}

if(configurationNode != null) {
	String keyspace = ElementParameterParser.getValue(node,"__KEY_SPACE__");
	String table = ElementParameterParser.getValue(node,"__COLUMN_FAMILY__");
	
	List<Map<String, String>> selectedColFuntions = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node, "__SELECTED_COLUMN_FUNCTION__");
	Map<String, String> selectedColFuntionMap = new HashMap<String, String>();
	for(Map<String, String> selectedColFuntion : selectedColFuntions){
	    selectedColFuntionMap.put(selectedColFuntion.get("COLUMN"), selectedColFuntion.get("FUNCTION"));
	}
	List<Map<String, String>> group = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node, "__GROUP__");
	org.talend.core.model.metadata.MetadataTable groupMetadata = new org.talend.core.model.metadata.MetadataTable();
	List<IMetadataColumn> groupColumnList = new ArrayList<IMetadataColumn>();
	groupMetadata.setListColumns(groupColumnList);
	for(Map<String, String> groupCol : group){
	    for(IMetadataColumn col : columns){
	        if(col.getLabel().equals(groupCol.get("COLUMN"))){
	            groupColumnList.add(col);
	        }
	    }
	}
	boolean hasGroup = group.size() > 0;
	String groupStructName = "Group_" + connName + "Struct";
	if(hasGroup){
	    codeGenArgument.getRecordStructGenerator().generateRecordStruct(groupStructName, groupMetadata);
	}
	if(!hasGroup){
	
    stringBuffer.append(TEXT_5);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_6);
    }else{
    stringBuffer.append(TEXT_7);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_9);
    }
    stringBuffer.append(TEXT_10);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(keyspace);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(table);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_14);
    
	        int count = 0;
	        for(IMetadataColumn column : columns){
	            String dbName = column.getOriginalDbColumnName();
	            String colName = column.getLabel();
	            boolean diffColName = !dbName.equals(colName);
	            if(selectedColFuntionMap.get(colName) == null){
	            
    stringBuffer.append(TEXT_15);
    stringBuffer.append(dbName);
    stringBuffer.append(TEXT_16);
    
	            }else if("ttl".equals(selectedColFuntionMap.get(colName))){
	            
    stringBuffer.append(TEXT_17);
    stringBuffer.append(dbName);
    stringBuffer.append(TEXT_18);
    
	            }else if("writeTime".equals(selectedColFuntionMap.get(colName))){
	            
    stringBuffer.append(TEXT_19);
    stringBuffer.append(dbName);
    stringBuffer.append(TEXT_20);
    
	            }
	            
    stringBuffer.append(TEXT_21);
    if(diffColName){
    stringBuffer.append(TEXT_22);
    stringBuffer.append(colName);
    stringBuffer.append(TEXT_23);
    }
    if(++count < columns.size()){
    stringBuffer.append(TEXT_24);
    }
    stringBuffer.append(TEXT_25);
    
	        }
	        
    stringBuffer.append(TEXT_26);
    
	    List<Map<String, String>> filterConditions = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node, "__FILTER_CONDITION__");
	    for(Map<String, String> filterCondition : filterConditions){
	    
    stringBuffer.append(TEXT_27);
    stringBuffer.append(filterCondition.get("COLUMN"));
    stringBuffer.append(TEXT_28);
    stringBuffer.append(filterCondition.get("FUNCTION"));
    stringBuffer.append(TEXT_29);
    stringBuffer.append(filterCondition.get("VALUE"));
    stringBuffer.append(TEXT_30);
    
	    }
	    
    stringBuffer.append(TEXT_31);
    
	    boolean useLimit = "true".equals(ElementParameterParser.getValue(node,"__USE_LIMIT__"));
	    String limit = ElementParameterParser.getValue(node,"__LIMIT__");
	    if(useLimit){
	    
    stringBuffer.append(TEXT_32);
    stringBuffer.append(limit);
    stringBuffer.append(TEXT_33);
    
	    }
	    
    stringBuffer.append(TEXT_34);
    
	    String order = ElementParameterParser.getValue(node,"__ORDER__");
	    if("NONE".equals(order)){
	    }else if("ASC".equals(order)){
	    
    stringBuffer.append(TEXT_35);
    
	    }else if("DESC".equals(order)){
	    
    stringBuffer.append(TEXT_36);
    
	    }
	    
    stringBuffer.append(TEXT_37);
    
	    if(hasGroup){
	    //TODO: when support group, should move the anonymous class to sparkcode.javajet 
	    
    stringBuffer.append(TEXT_38);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_39);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(outRowStruct);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_44);
    
	                    for(IMetadataColumn groupColumn : groupColumnList){
	                        String propertyName = groupColumn.getLabel().substring(0,1).toUpperCase() + groupColumn.getLabel().substring(1);
	                    
    stringBuffer.append(TEXT_45);
    stringBuffer.append(propertyName);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(propertyName);
    stringBuffer.append(TEXT_47);
    
	                    }
	                    
    stringBuffer.append(TEXT_48);
    stringBuffer.append(groupStructName);
    stringBuffer.append(TEXT_49);
    
	    }
	    
    stringBuffer.append(TEXT_50);
    
} // end if(configurationNode != null)

    stringBuffer.append(TEXT_51);
    return stringBuffer.toString();
  }
}
