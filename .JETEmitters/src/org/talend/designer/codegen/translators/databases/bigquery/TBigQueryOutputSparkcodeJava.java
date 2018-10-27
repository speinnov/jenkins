package org.talend.designer.codegen.translators.databases.bigquery;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.designer.common.tsqlrow.TSqlRowUtil;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;

public class TBigQueryOutputSparkcodeJava
{
  protected static String nl;
  public static synchronized TBigQueryOutputSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TBigQueryOutputSparkcodeJava result = new TBigQueryOutputSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        public static class ";
  protected final String TEXT_2 = "_From";
  protected final String TEXT_3 = "ToGson implements org.apache.spark.api.java.function.PairFunction<";
  protected final String TEXT_4 = ", Object, com.google.gson.JsonObject>{" + NL + "            private ContextProperties context = null;" + NL + "" + NL + "            public ";
  protected final String TEXT_5 = "_From";
  protected final String TEXT_6 = "ToGson(JobConf job){" + NL + "                this.context = new ContextProperties(job);" + NL + "            }" + NL + "" + NL + "            public scala.Tuple2<Object, com.google.gson.JsonObject> call(";
  protected final String TEXT_7 = " row) {" + NL + "            \tcom.google.gson.JsonObject jsonObject = new com.google.gson.JsonObject();" + NL + "            \tcom.google.gson.Gson gson = new com.google.gson.Gson();";
  protected final String TEXT_8 = NL + "                        if (row.";
  protected final String TEXT_9 = " != null) {";
  protected final String TEXT_10 = NL + "                        \t\tjsonObject.add(\"";
  protected final String TEXT_11 = "\", gson.fromJson(row.get(\"";
  protected final String TEXT_12 = "\"), com.google.gson.JsonElement.class));" + NL + "                        \t";
  protected final String TEXT_13 = NL + "                        \t\t\tcom.google.gson.JsonArray array = new com.google.gson.JsonArray();" + NL + "                        \t\t\tfor(Object e : (java.util.List)row.get(\"";
  protected final String TEXT_14 = "\")){" + NL + "                        \t\t\t\tarray.add(gson.fromJson(e.toString(), com.google.gson.JsonElement.class));\t" + NL + "                        \t\t\t}" + NL + "                        \t\t\tjsonObject.add(\"";
  protected final String TEXT_15 = "\", array);" + NL + "                        \t\t";
  protected final String TEXT_16 = NL + "                        \t\t\tjsonObject.add(\"";
  protected final String TEXT_17 = "\", gson.fromJson(row.get(\"";
  protected final String TEXT_18 = "\"), com.google.gson.JsonElement.class));" + NL + "                        \t\t";
  protected final String TEXT_19 = NL + "                        \t\tjsonObject.addProperty(\"";
  protected final String TEXT_20 = "\", String.valueOf(row.get(\"";
  protected final String TEXT_21 = "\")));" + NL + "                        \t";
  protected final String TEXT_22 = NL + "                        \t\tjsonObject.addProperty(\"";
  protected final String TEXT_23 = "\", (Number)row.get(\"";
  protected final String TEXT_24 = "\"));" + NL + "                        \t";
  protected final String TEXT_25 = NL + "                        \t\tjsonObject.addProperty(\"";
  protected final String TEXT_26 = "\", Boolean.valueOf(String.valueOf(row.get(\"";
  protected final String TEXT_27 = "\"))));" + NL + "                        \t";
  protected final String TEXT_28 = NL + "                        \t\tjsonObject.addProperty(\"";
  protected final String TEXT_29 = "\", String.valueOf(row.get(\"";
  protected final String TEXT_30 = "\")));" + NL + "                        \t";
  protected final String TEXT_31 = NL + "                        }" + NL + "            \t\t";
  protected final String TEXT_32 = NL + "                return new scala.Tuple2<Object, com.google.gson.JsonObject>(null, jsonObject);" + NL + "            }" + NL + "        }";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument)argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();
TSqlRowUtil tSqlRowUtil = new TSqlRowUtil(node);
String validateError = tSqlRowUtil.validate(true, false);
if (validateError != null) {
    // Cause the job compilation to explicitly fail if there is a problem.
    return "throw new JobConfigurationError(\"" + validateError +"\");";
}

java.util.List<IMetadataTable> metadatas = node.getMetadataList();

if(metadatas != null && metadatas.size() > 0){
    IMetadataTable metadata = metadatas.get(0);
    if(metadata != null){
        IConnection componentIncomingConnection = tSqlRowUtil.getIncomingConnections().get(0);
        String inStructName = codeGenArgument.getRecordStructName(componentIncomingConnection);
        String inRddName = "rdd_"+componentIncomingConnection.getName();
        java.util.List<IMetadataColumn> columnList = tSqlRowUtil.getColumns(componentIncomingConnection);
        int sizeColumns = columnList.size();
        
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(inStructName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(inStructName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(inStructName);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(inStructName);
    stringBuffer.append(TEXT_7);
    
                // Build the GSON document
                for (int i = 0; i < sizeColumns; i++) {
                    IMetadataColumn column = columnList.get(i);
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String dbType = column.getType();
                    boolean isPrimitive = JavaTypesManager.isJavaPrimitiveType(javaType, column.isNullable());
                	if(!isPrimitive) {
                	
    stringBuffer.append(TEXT_8);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_9);
    
                    }
                        	if("RECORD".equals(dbType)){
                        	
    stringBuffer.append(TEXT_10);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_11);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_12);
    
                        	}else if("ARRAY".equals(dbType)){
                        		if(javaType == JavaTypesManager.LIST){
                        		
    stringBuffer.append(TEXT_13);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_14);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_15);
    
                        		}else{//STRING
                        		
    stringBuffer.append(TEXT_16);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_17);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_18);
    
                        		}
                        	}else if("BYTES".equals(dbType)){
                        	
    stringBuffer.append(TEXT_19);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_20);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_21);
    
                        	}else if("INTEGER".equals(dbType) || "FLOAT".equals(dbType)){
                        	
    stringBuffer.append(TEXT_22);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_23);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_24);
    
                        	}else if("BOOLEAN".equals(dbType)){
                        	
    stringBuffer.append(TEXT_25);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_26);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_27);
    
                        	//}else if("DATETIME".equals(dbType)){
                        	//}else if("DATE".equals(dbType)){
                        	//}else if("TIME".equals(dbType)){
                        	//}else if("TIMESTAMP".equals(dbType)){
                        	//}else if("STRING".equals(dbType)){
                        	}else{
                        	
    stringBuffer.append(TEXT_28);
    stringBuffer.append(column.getOriginalDbColumnName());
    stringBuffer.append(TEXT_29);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_30);
     
                        	}
                    if(!isPrimitive) {
                    
    stringBuffer.append(TEXT_31);
    
            		}
                }
                
    stringBuffer.append(TEXT_32);
    
    }
}
// No schema
else{

}

    return stringBuffer.toString();
  }
}
