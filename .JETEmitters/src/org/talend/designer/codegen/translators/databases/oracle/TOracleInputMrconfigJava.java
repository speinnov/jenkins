package org.talend.designer.codegen.translators.databases.oracle;

import java.util.List;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TOracleInputMrconfigJava
{
  protected static String nl;
  public static synchronized TOracleInputMrconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TOracleInputMrconfigJava result = new TOracleInputMrconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "                MultipleInputs.addInputPath(job,";
  protected final String TEXT_2 = NL + "                        ";
  protected final String TEXT_3 = "StructInputFormat.class," + NL + "                        ChainMapper.class, \"";
  protected final String TEXT_4 = "\");" + NL + "                chainMapper.setCid(\"";
  protected final String TEXT_5 = "\");";
  protected final String TEXT_6 = NL + "                MultipleInputs.addInputPath(job,";
  protected final String TEXT_7 = NL + "                        ";
  protected final String TEXT_8 = "StructInputFormat.class," + NL + "                        ChainMapper.class, \"";
  protected final String TEXT_9 = "\");" + NL + "                chainMapper.setCid(\"";
  protected final String TEXT_10 = "\");" + NL;
  protected final String TEXT_11 = NL + "                chainMapper.addMapper(job, ";
  protected final String TEXT_12 = "_InputMapper.class," + NL + "                    NullWritable.class, ";
  protected final String TEXT_13 = "TemporaryStruct.class," + NL + "                    NullWritable.class, ";
  protected final String TEXT_14 = "Struct.class," + NL + "                    true, new JobConf(false));" + NL + "" + NL + "                MultipleOutputs.setWorkDir(job," + NL + "                        genTempFolderForComponent(\"";
  protected final String TEXT_15 = "\"));" + NL + "                MultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_16 = "\"," + NL + "                        NullWritable.class, ";
  protected final String TEXT_17 = "Struct.class);";

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
         String dbtable = ElementParameterParser.getValue(node, "__TABLE__");
         String dbquery= ElementParameterParser.getValue(node, "__QUERY__");
        dbquery = dbquery.replaceAll("\n"," ");
        dbquery = dbquery.replaceAll("\r"," ");

        List< ? extends IConnection> mainConnections = node.getOutgoingConnections("FLOW");
        List< ? extends IConnection> rejectedConnections = node.getOutgoingConnections("REJECT");
        if ((rejectedConnections == null) || (rejectedConnections.size() == 0)) {
            // No reject flow => we parse directly the input.
            if ((mainConnections != null) && (mainConnections.size() == 1)) {
                IConnection mainConnection = mainConnections.get(0);
                String connName = mainConnection.getName();
                
    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
            }
        } else {
            // A reject flow, we need to make a multiples output.
            if ((mainConnections != null) && (mainConnections.size() == 1)
                    && (rejectedConnections != null) && (rejectedConnections.size() == 1)) {
                IConnection mainConnection = mainConnections.get(0);
                String mainConnName = mainConnection.getName();
                
    stringBuffer.append(TEXT_6);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(mainConnName);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    
                IConnection rejectedConnection = rejectedConnections.get(0);
                String rejectConnName = rejectedConnection.getName();
                
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(mainConnName);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(mainConnName);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(rejectedConnection.getName());
    stringBuffer.append(TEXT_16);
    stringBuffer.append(rejectedConnection.getName());
    stringBuffer.append(TEXT_17);
    
            }
        }
    }
}

    return stringBuffer.toString();
  }
}
