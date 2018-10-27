package org.talend.designer.codegen.translators.databases.mongodb;

import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.designer.common.tsqlrow.TSqlRowUtil;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.ElementParameterParser;

public class TMongoDBInputSparkcodeJava
{
  protected static String nl;
  public static synchronized TMongoDBInputSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMongoDBInputSparkcodeJava result = new TMongoDBInputSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "        public static class ";
  protected final String TEXT_2 = "_FromBsonTo";
  protected final String TEXT_3 = " implements org.apache.spark.api.java.function.Function<org.bson.BSONObject, ";
  protected final String TEXT_4 = "> {" + NL + "" + NL + "            private java.util.Map<String, String> pathMap_";
  protected final String TEXT_5 = ";" + NL + "            private ContextProperties context = null;" + NL + "" + NL + "            public ";
  protected final String TEXT_6 = "_FromBsonTo";
  protected final String TEXT_7 = "(JobConf job) {" + NL + "                this.context = new ContextProperties(job);" + NL + "                pathMap_";
  protected final String TEXT_8 = " = new java.util.HashMap<String, String>();";
  protected final String TEXT_9 = NL + "                    pathMap_";
  protected final String TEXT_10 = ".put(\"";
  protected final String TEXT_11 = "\", ";
  protected final String TEXT_12 = ");";
  protected final String TEXT_13 = NL + "            }" + NL + "" + NL + "            public ";
  protected final String TEXT_14 = " call(org.bson.BSONObject bson) throws java.io.UnsupportedEncodingException {";
  protected final String TEXT_15 = NL + "                ";
  protected final String TEXT_16 = " result = new ";
  protected final String TEXT_17 = "();" + NL + "                Object valueObj_";
  protected final String TEXT_18 = " = null;";
  protected final String TEXT_19 = NL + "                        valueObj_";
  protected final String TEXT_20 = " = getValue(pathMap_";
  protected final String TEXT_21 = ".get(\"";
  protected final String TEXT_22 = "\"),\"";
  protected final String TEXT_23 = "\", bson);";
  protected final String TEXT_24 = NL + "                        result.";
  protected final String TEXT_25 = " = valueObj_";
  protected final String TEXT_26 = ";";
  protected final String TEXT_27 = NL + "                        if(valueObj_";
  protected final String TEXT_28 = " != null && valueObj_";
  protected final String TEXT_29 = ".toString().length() > 0) {";
  protected final String TEXT_30 = NL + "                                result.";
  protected final String TEXT_31 = " = BigDataParserUtils.parseTo_Date((java.util.Date) valueObj_";
  protected final String TEXT_32 = ", ";
  protected final String TEXT_33 = ");";
  protected final String TEXT_34 = NL + "                                result.";
  protected final String TEXT_35 = " = java.nio.ByteBuffer.wrap(java.nio.charset.Charset.defaultCharset().decode(java.nio.ByteBuffer.wrap((byte[])valueObj_";
  protected final String TEXT_36 = ")).toString().getBytes(utf8Charset));";
  protected final String TEXT_37 = NL + "                                result.";
  protected final String TEXT_38 = " = valueObj_";
  protected final String TEXT_39 = ".toString();";
  protected final String TEXT_40 = NL + "                                    if (valueObj_";
  protected final String TEXT_41 = ".getClass().equals(Double.class)) {" + NL + "                                        result.";
  protected final String TEXT_42 = " = BigDataParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_43 = ".toString()).intValue();" + NL + "                                    } else {" + NL + "                                        result.";
  protected final String TEXT_44 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_45 = "(valueObj_";
  protected final String TEXT_46 = ".toString());" + NL + "                                    }";
  protected final String TEXT_47 = NL + "                                    if (valueObj_";
  protected final String TEXT_48 = ".getClass().equals(Double.class)) {" + NL + "                                        result.";
  protected final String TEXT_49 = " = BigDataParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_50 = ".toString()).longValue();" + NL + "                                    } else {" + NL + "                                        result.";
  protected final String TEXT_51 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_52 = "(valueObj_";
  protected final String TEXT_53 = ".toString());" + NL + "                                    }";
  protected final String TEXT_54 = NL + "                                    if (valueObj_";
  protected final String TEXT_55 = ".getClass().equals(Double.class)) {" + NL + "                                        result.";
  protected final String TEXT_56 = " = BigDataParserUtils.parseTo_Double(valueObj_";
  protected final String TEXT_57 = ".toString()).shortValue();" + NL + "                                    } else {" + NL + "                                        result.";
  protected final String TEXT_58 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_59 = "(valueObj_";
  protected final String TEXT_60 = ".toString());" + NL + "                                    }";
  protected final String TEXT_61 = NL + "                                result.";
  protected final String TEXT_62 = " = BigDataParserUtils.parseTo_";
  protected final String TEXT_63 = "(valueObj_";
  protected final String TEXT_64 = ".toString());";
  protected final String TEXT_65 = NL + "                        } else {" + NL + "                            result.";
  protected final String TEXT_66 = " = ";
  protected final String TEXT_67 = ";" + NL + "                        }";
  protected final String TEXT_68 = NL + "                return result;" + NL + "            }" + NL + "" + NL + "            // Get Value from BSON document" + NL + "            public Object getValue(String parentNode, String dbColumnName, org.bson.BSONObject bson) {" + NL + "                Object value = null;" + NL + "                if(bson == null){" + NL + "                    return null;" + NL + "                }" + NL + "                if (parentNode == null || \"\".equals(parentNode)) {" + NL + "                    if (\"*\".equals(dbColumnName)) {" + NL + "                        value = bson;" + NL + "                    } else if (bson.get(dbColumnName)!= null){" + NL + "                        value = bson.get(dbColumnName);" + NL + "                    }" + NL + "                } else {" + NL + "                    String objNames[] = parentNode.split(\"\\\\.\");" + NL + "                    org.bson.BSONObject currentObj = bson;" + NL + "                    for(int i=0; i<objNames.length; i++){" + NL + "                        currentObj = (org.bson.BSONObject) currentObj.get(objNames[i]);" + NL + "                        if(currentObj == null){" + NL + "                            break;" + NL + "                        }" + NL + "                    }" + NL + "                    if (\"*\".equals(dbColumnName)) {" + NL + "                        value = currentObj;" + NL + "                    } else if(currentObj != null){" + NL + "                        value=currentObj.get(dbColumnName);" + NL + "                    }" + NL + "                }" + NL + "            return value;" + NL + "            }" + NL + "" + NL + "        }";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument)argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

TSqlRowUtil tSqlRowUtil = new TSqlRowUtil(node);
String validateError = tSqlRowUtil.validate(false, true);
if (validateError != null) {
    // Cause the job compilation to explicitly fail if there is a problem.
    return "throw new JobConfigurationError(\"" + validateError +"\");";
}

String outStructName = codeGenArgument.getRecordStructName(tSqlRowUtil.getOutgoingConnection());

java.util.List<IMetadataTable> metadatas = node.getMetadataList();
java.util.List<IMetadataColumn> columnList = null;

java.util.List<java.util.Map<String, String>> mapping = (java.util.List<java.util.Map<String,String>>)ElementParameterParser.getObjectValueXML(node, "__MAPPING__");

if(metadatas != null && metadatas.size() > 0){
    IMetadataTable metadata = metadatas.get(0);
    if(metadata != null){
        
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
                // Parent mapping
                for (int i=0;i<mapping.size();i++) {
                    String path = mapping.get(i).get("PARENT_NODE_PATH");
                    if(path==null || "".equals(path)){
                        path="\"\"";
                    }
                    String schemaColumn=mapping.get(i).get("SCHEMA_COLUMN");
                    
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(schemaColumn);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(path);
    stringBuffer.append(TEXT_12);
    
                }
                
    stringBuffer.append(TEXT_13);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    
                // Get input schema columns
                columnList = metadata.getListColumns();
                int sizeColumns = columnList.size();
                for (int i = 0; i < sizeColumns; i++) {
                    IMetadataColumn column = columnList.get(i);
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
                    
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_22);
    stringBuffer.append(column.getOriginalDbColumnName() );
    stringBuffer.append(TEXT_23);
    
                    // Types conversion
                    // If object then return the value
                    if(javaType == JavaTypesManager.OBJECT) {
                        
    stringBuffer.append(TEXT_24);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    
                    // Else other types conversion
                    } else {
                        
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    

                            // DATE
                            if(javaType == JavaTypesManager.DATE) {
                                
    stringBuffer.append(TEXT_30);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append( patternValue);
    stringBuffer.append(TEXT_33);
    
                            }

                            // BYTE ARRAY
                            else if (javaType == JavaTypesManager.BYTE_ARRAY){
                                
    stringBuffer.append(TEXT_34);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    
                            }

                            // STRING
                            else if(javaType == JavaTypesManager.STRING){
                                
    stringBuffer.append(TEXT_37);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_39);
    
                            }

                            // INTEGER with a returned DB type of double or Int
                            else if(javaType == JavaTypesManager.INTEGER){
                                
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_44);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    
                            }

                            // LONG with a returned DB type of double or long
                            else if (javaType == JavaTypesManager.LONG){
                                
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_51);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    
                            }

                            // SHORT with a returned DB type of double or short
                            else if(javaType == JavaTypesManager.SHORT){
                                
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_58);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    
                            }

                            // OTHER
                            else {
                                
    stringBuffer.append(TEXT_61);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_62);
    stringBuffer.append(typeToGenerate );
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    
                            }

                            
    stringBuffer.append(TEXT_65);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_66);
    stringBuffer.append(JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate));
    stringBuffer.append(TEXT_67);
    
                    }
                }
                
    stringBuffer.append(TEXT_68);
    
    }
// No Schema
} else {

}

    return stringBuffer.toString();
  }
}
