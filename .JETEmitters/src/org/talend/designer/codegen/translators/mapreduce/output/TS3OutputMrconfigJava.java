package org.talend.designer.codegen.translators.mapreduce.output;

import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TS3OutputMrconfigJava
{
  protected static String nl;
  public static synchronized TS3OutputMrconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TS3OutputMrconfigJava result = new TS3OutputMrconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "            String ";
  protected final String TEXT_2 = " = ";
  protected final String TEXT_3 = ";";
  protected final String TEXT_4 = NL + "                Path pathToDelete_";
  protected final String TEXT_5 = " = new Path(";
  protected final String TEXT_6 = ");" + NL + "                if (fs.exists(pathToDelete_";
  protected final String TEXT_7 = ")) {" + NL + "                    fs.delete(pathToDelete_";
  protected final String TEXT_8 = ", true);" + NL + "                }";
  protected final String TEXT_9 = NL + "            MultipleOutputs.setOutputFormat(job, \"";
  protected final String TEXT_10 = "\",";
  protected final String TEXT_11 = NL + "                    ";
  protected final String TEXT_12 = "StructOutputFormat.class, ";
  protected final String TEXT_13 = ");";
  protected final String TEXT_14 = NL + "            MultipleOutputs.setOutputFormat(job, \"";
  protected final String TEXT_15 = "\",";
  protected final String TEXT_16 = NL + "                    ";
  protected final String TEXT_17 = "StructOutputFormat.class);";
  protected final String TEXT_18 = NL + "        job.setOutputFormat(";
  protected final String TEXT_19 = "StructOutputFormat.class);";
  protected final String TEXT_20 = NL + "            ";
  protected final String TEXT_21 = "StructOutputFormat.setOutputPath(job," + NL + "                    new Path(";
  protected final String TEXT_22 = "));";
  protected final String TEXT_23 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_24 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_25 = ");";
  protected final String TEXT_26 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_27 = " = ";
  protected final String TEXT_28 = "; ";
  protected final String TEXT_29 = NL + "        {" + NL + "            org.apache.hadoop.fs.s3.S3FileSystem s3FileSystem = new org.apache.hadoop.fs.s3.S3FileSystem();" + NL + "            s3FileSystem.initialize(new java.net.URI(outputPath_";
  protected final String TEXT_30 = "), getConf());" + NL + "            s3FileSystem.delete(new Path(outputPath_";
  protected final String TEXT_31 = "),true);" + NL + "        }";
  protected final String TEXT_32 = NL + "        {" + NL + "            org.apache.hadoop.fs.s3native.NativeS3FileSystem s3nFileSystem = new org.apache.hadoop.fs.s3native.NativeS3FileSystem();" + NL + "            s3nFileSystem.initialize(new java.net.URI(outputPath_";
  protected final String TEXT_33 = "), getConf());" + NL + "            s3nFileSystem.delete(new Path(outputPath_";
  protected final String TEXT_34 = "),true);" + NL + "        }";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
// Parse the inputs to this javajet generator.
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode) codeGenArgument.getArgument();
final String cid = node.getUniqueName();

    

/**
 * This helper is used to generate code for output components (that is,
 * components that sit at the end of a Mapper or Reducer chain and persist rows
 * to the data store).
 */
class MrRowOutputUtil {

    
    
/**
 * This helper class provides utilities for generating code when there are multiple outgoing connections, specifically
 * those that provide a main connection and a reject connection. When both are present, this class helps determine where
 * to send or retrieve the data for a given connection.
 *
 * Normally, for efficiency reasons, the main output continues along a chain of Mapper/Reducer classes and the reject
 * output is sent to a MultipleOutputs to start a new chain.
 *
 * TODO: the file mr_multioutput_utils.javajet and MultipleOutputsComponentUtils.java need to be kept synchronized.
 */
class MultiOutputUtils {

    private static final String MAIN_FLOW_NAME = "FLOW"; //$NON-NLS-1$

    private static final String REJECT_FLOW_NAME = "REJECT"; //$NON-NLS-1$

    private static final String TAVROINPUT_COMPONENT_NAME = "tAvroInput"; //$NON-NLS-1$

    private static final String TEXTRACTABSTRACTJSONFIELDS_COMPONENT_NAME = "tExtractAbstractJSONFields"; //$NON-NLS-1$

    private static final String TEXTRACTDELIMITEDFIELDS_COMPONENT_NAME = "tExtractDelimitedFields"; //$NON-NLS-1$

    private static final String TEXTRACTFULLROW_COMPONENT_NAME = "tExtractFullRow"; //$NON-NLS-1$

    private static final String TEXTRACTJSONFIELDS_COMPONENT_NAME = "tExtractJSONFields"; //$NON-NLS-1$

    private static final String TEXTRACTPOSITIONALFIELDS_COMPONENT_NAME = "tExtractPositionalFields"; //$NON-NLS-1$

    private static final String TEXTRACTREGEXFIELDS_COMPONENT_NAME = "tExtractRegexFields"; //$NON-NLS-1$

    private static final String TEXTRACTXMLFIELD_COMPONENT_NAME = "tExtractXMLField"; //$NON-NLS-1$

    private static final String TFILEINPUTDELIMITED_COMPONENT_NAME = "tFileInputDelimited"; //$NON-NLS-1$

    private static final String TFILEINPUTFULLROW_COMPONENT_NAME = "tFileInputFullRow"; //$NON-NLS-1$

    private static final String TFILEINPUTJSON_COMPONENT_NAME = "tFileInputJSON"; //$NON-NLS-1$

    private static final String TFILEINPUTPOSITIONAL_COMPONENT_NAME = "tFileInputPositional"; //$NON-NLS-1$

    private static final String TFILEINPUTREGEX_COMPONENT_NAME = "tFileInputRegex"; //$NON-NLS-1$

    private static final String TFILEINPUTXML_COMPONENT_NAME = "tFileInputXML"; //$NON-NLS-1$

    private static final String THBASEINPUT_COMPONENT_NAME = "tHBaseInput"; //$NON-NLS-1$

    private static final String THDFSINPUT_COMPONENT_NAME = "tHDFSInput"; //$NON-NLS-1$

    private static final String TJDBCINPUT_COMPONENT_NAME = "tJDBCInput"; //$NON-NLS-1$

    private static final String TVIRTUALINPUTCOMPONENT_NAME = "tMRInput"; //$NON-NLS-1$

    /**
     * Check if the source of a connection may be a multipleOutput.
     *
     * @param connection current connection
     * @return true if the component may have a reject flow.
     */
    public boolean canBeAMultipleOutput(IConnection connection) {
        if ((connection == null) || (connection.getSource() == null) || (connection.getSource().getComponent() == null)) {
            return false;
        } else {
            return isAMultipleOutputComponent(connection.getSource().getComponent().getName());
        }
    }

    /**
     * Check if a the name of a component is in the list of the multipleoutputs component.
     *
     * These component may have a reject flow for processing error. But we need to handle them just like they don't have
     * one.
     *
     * @param name of the component
     * @return true if the component may have a reject flow.
     */
    public boolean isAMultipleOutputComponent(String name) {
        return name.equals(TAVROINPUT_COMPONENT_NAME) || name.equals(TEXTRACTABSTRACTJSONFIELDS_COMPONENT_NAME)
                || name.equals(TEXTRACTDELIMITEDFIELDS_COMPONENT_NAME) || name.equals(TEXTRACTFULLROW_COMPONENT_NAME)
                || name.equals(TEXTRACTJSONFIELDS_COMPONENT_NAME) || name.equals(TEXTRACTPOSITIONALFIELDS_COMPONENT_NAME)
                || name.equals(TEXTRACTREGEXFIELDS_COMPONENT_NAME) || name.equals(TEXTRACTXMLFIELD_COMPONENT_NAME)
                || name.equals(TFILEINPUTDELIMITED_COMPONENT_NAME) || name.equals(TFILEINPUTFULLROW_COMPONENT_NAME)
                || name.equals(TFILEINPUTJSON_COMPONENT_NAME) || name.equals(TFILEINPUTPOSITIONAL_COMPONENT_NAME)
                || name.equals(TFILEINPUTREGEX_COMPONENT_NAME) || name.equals(TFILEINPUTXML_COMPONENT_NAME)
                || name.equals(THBASEINPUT_COMPONENT_NAME) || name.equals(THDFSINPUT_COMPONENT_NAME)
                || name.equals(TJDBCINPUT_COMPONENT_NAME) || name.equals(TVIRTUALINPUTCOMPONENT_NAME);
    }

    /**
     * Check if the current connection is a reject flow of a multiple output component.
     *
     * @param connection the current connection
     * @return true if the current connection is a reject flow
     */
    public boolean isRejectFlowOnMultipleOutput(IConnection connection) {
        if ((connection == null) || (connection.getSource() == null) || (connection.getSource().getComponent() == null)) {
            return false;
        } else {
            return isRejectFlowOnMultipleOutput(connection.getSource().getComponent().getName(), connection.getConnectorName());
        }
    }

    /**
     * Check if the current connection is a reject flow of a multiple output component.
     *
     * @param name of the component
     * @param connectorName name of the link
     * @return true if the current connection is a reject flow
     */
    public boolean isRejectFlowOnMultipleOutput(String name, String connectorName) {
        return isAMultipleOutputComponent(name) && connectorName.equals(REJECT_FLOW_NAME);
    }

    /**
     * Check if the current connection is a main flow of a multiple output component.
     *
     * @param connection the current connection
     * @return true if the current connection is a main flow
     */
    public boolean isMainFlowOnMultipleOutput(IConnection connection) {
        if ((connection == null) || (connection.getSource() == null) || (connection.getSource().getComponent() == null)) {
            return false;
        } else {
            return isMainFlowOnMultipleOutput(connection.getSource().getComponent().getName(), connection.getConnectorName());
        }
    }

    /**
     * Check if the current connection is a main flow of a multiple output component.
     *
     * @param name of the component
     * @param connectorName name of the link
     * @return true id the current connection is a main flow
     */
    public boolean isMainFlowOnMultipleOutput(String name, String connectorName) {
        return isAMultipleOutputComponent(name) && connectorName.equals(MAIN_FLOW_NAME);
    }

}


    
    private final MultiOutputUtils moUtil = new MultiOutputUtils();

    /**
     *
     */
    public void generateOutputMrConfig(String cid, String folder,
            boolean deleteExisting, java.util.List<? extends IConnection> inConns) {

        if (inConns == null || inConns.size() == 0)
            return;
        IConnection inConn = inConns.get(0);

        // Generate a variable for the folder name if present.
        String codeVarOutputPath = null;
        if (null != folder) {
            codeVarOutputPath = "outputPath_" + cid;
            
    stringBuffer.append(TEXT_1);
    stringBuffer.append(codeVarOutputPath);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(folder);
    stringBuffer.append(TEXT_3);
    
            if (deleteExisting) {
                
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(codeVarOutputPath);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    
            }
        }

        // This is guaranteed to contain at least inConn.
        java.util.List<? extends IConnection> srcOutConns = inConn.getSource().getOutgoingConnections(EConnectionType.FLOW_MAIN);

        // The incoming data is on the normal mapper chain when it is the sole
        // output of the preceding component, OR when it is the main output of
        // a standard main/reject component.
        if (srcOutConns.size() == 1 || (moUtil.isMainFlowOnMultipleOutput(inConn))) {
            generateOutputOnChain(cid, codeVarOutputPath);
        } else {
            // In all other conditions, the incoming data comes through a
            // MultipleOutput object.
            generateOutputFromMultiple(inConn.getName(), cid, codeVarOutputPath);
        }
    }

    /**
     * Generates code that uses data from a MultipleOutputs object.
     */
    public void generateOutputFromMultiple(String moTag, String cid,
            String codeVarOutputPath) {
        if (codeVarOutputPath != null) {
            
    stringBuffer.append(TEXT_9);
    stringBuffer.append(moTag);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(codeVarOutputPath);
    stringBuffer.append(TEXT_13);
    
        } else {
            
    stringBuffer.append(TEXT_14);
    stringBuffer.append(moTag);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    
        }
    }

    /**
     * Generates code that uses data from the mapper chain.
     */
    public void generateOutputOnChain(String cid, String codeVarOutputPath) {
        
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    
        if (codeVarOutputPath != null) {
            
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(codeVarOutputPath);
    stringBuffer.append(TEXT_22);
    
        }
    }
}


    

boolean isS3Format = "true".equals(ElementParameterParser.getValue(node, "__S3_FORMAT__"));
String s3bucket = ElementParameterParser.getValue(node,"__S3_BUCKET__");
String s3username = ElementParameterParser.getValue(node, "__S3_USERNAME__");
String folder = (isS3Format ? "\"s3://\" + " : "\"s3n://\" + ")
    + s3username + " + \":\" + decryptedPassword_" + cid + " + \"@\" + " + s3bucket;

 // Get the password under the variable decryptedPassword
String passwordFieldName = "__S3_PASSWORD__";

    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_25);
    } else {
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_27);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_28);
    }
    

// Generate the standard output config with the custom folder, and no overwrite
// yet.
new MrRowOutputUtil().generateOutputMrConfig(cid, folder, false,
        node.getIncomingConnections(EConnectionType.FLOW_MAIN));

// Custom overwrite logic for S3.
if ("OVERWRITE".equals(ElementParameterParser.getValue(node, "__FILE_ACTION__"))) {
    if (isS3Format) {
        
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    
    } else { // use the normal S3n format
         
    stringBuffer.append(TEXT_32);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
    }
}

    return stringBuffer.toString();
  }
}
