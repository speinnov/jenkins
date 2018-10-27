package org.talend.designer.codegen.translators.databases.hive;

import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.designer.common.tsqlrow.TSqlRowUtil;

public class THiveInputSparkcodeJava
{
  protected static String nl;
  public static synchronized THiveInputSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    THiveInputSparkcodeJava result = new THiveInputSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "public static class ";
  protected final String TEXT_2 = "_FromRowTo";
  protected final String TEXT_3 = " implements org.apache.spark.api.java.function.Function<org.apache.spark.sql.Row, ";
  protected final String TEXT_4 = "> {" + NL + "" + NL + "    public ";
  protected final String TEXT_5 = " call(org.apache.spark.sql.Row row) {";
  protected final String TEXT_6 = NL + "        ";
  protected final String TEXT_7 = " result = new ";
  protected final String TEXT_8 = "();" + NL + "        org.apache.spark.sql.types.StructField[] structFields = row.schema().fields();" + NL + "        for (int i = 0; i < structFields.length; i++) {" + NL + "            org.apache.avro.Schema.Field avroField = ";
  protected final String TEXT_9 = ".getClassSchema().getField(structFields[i].name());" + NL + "            if (avroField != null){" + NL + "                result.put(avroField.pos(), row.get(i));" + NL + "            }" + NL + "        }" + NL + "        return result;" + NL + "    }" + NL + "}";

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


    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(outStructName);
    stringBuffer.append(TEXT_9);
    

    return stringBuffer.toString();
  }
}
