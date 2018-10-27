package org.talend.designer.codegen.translators.processing.datamapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.talend.transform.components.spark.TDMExternalNodeProvider;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.MetadataTalendType;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IBigDataNode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.core.model.utils.NodeUtil;
import org.talend.core.model.utils.TalendTextUtils;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.transform.dataflow.common.THMapInputParms;
import org.talend.transform.components.spark.TDMExternalNodeProvider;
import org.talend.transform.components.spark.thmapinput.THMapInputComponent;
import org.talend.transform.components.spark.codegen.THMapUtil;
import org.talend.transform.components.spark.utils.TDMUtils;

public class THMapInputSparkconfigJava
{
  protected static String nl;
  public static synchronized THMapInputSparkconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    THMapInputSparkconfigJava result = new THMapInputSparkconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "    java.net.URI currentURI_";
  protected final String TEXT_2 = "_config = FileSystem.getDefaultUri(ctx.hadoopConfiguration());" + NL + "    FileSystem.setDefaultUri(ctx.hadoopConfiguration(), new java.net.URI(";
  protected final String TEXT_3 = "));" + NL + "    fs = FileSystem.get(ctx.hadoopConfiguration());";
  protected final String TEXT_4 = NL + "   ";
  protected final String TEXT_5 = "<NullWritable, ";
  protected final String TEXT_6 = "> rdd_";
  protected final String TEXT_7 = ";";
  protected final String TEXT_8 = NL + NL + "// Set up a Spark DataFlow." + NL + "org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlowContext ";
  protected final String TEXT_9 = "_sdfContext = new org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlowContext.Builder()" + NL + "        .withSparkContext(ctx).build();" + NL + "org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlow ";
  protected final String TEXT_10 = "_sdf = new org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlow(";
  protected final String TEXT_11 = NL + "        ";
  protected final String TEXT_12 = "_sdfContext);" + NL + "" + NL + "// Set up and run the component." + NL + "org.talend.transform.dataflow.thmapinput.THMapInput ";
  protected final String TEXT_13 = "_tHMapInput = new org.talend.transform.dataflow.thmapinput.THMapInput();" + NL + "org.talend.transform.dataflow.thmapinput.THMapInputSpecBuilder ";
  protected final String TEXT_14 = "_thmapSB = ";
  protected final String TEXT_15 = "_tHMapInput" + NL + "\t.createSpecBuilder()" + NL + "\t.setId(\"";
  protected final String TEXT_16 = "\")" + NL + "\t.setInput(";
  protected final String TEXT_17 = ")" + NL + "\t.setMap(\"";
  protected final String TEXT_18 = "\")" + NL + "\t.setProjectArchives(\"";
  protected final String TEXT_19 = "\")" + NL + "\t.setStorageLevel(\"";
  protected final String TEXT_20 = "\")" + NL + "\t.setParams(\"";
  protected final String TEXT_21 = "\");";
  protected final String TEXT_22 = NL + "\t";
  protected final String TEXT_23 = "_thmapSB.addConnectionId2ElementPath(\"";
  protected final String TEXT_24 = "\", \"";
  protected final String TEXT_25 = "\");" + NL + "\t";
  protected final String TEXT_26 = "_thmapSB.addConnectionId2Schema(\"";
  protected final String TEXT_27 = "\", ";
  protected final String TEXT_28 = ".getClassSchema().toString());";
  protected final String TEXT_29 = NL + NL + "// Set up context properties";
  protected final String TEXT_30 = NL;
  protected final String TEXT_31 = ".synchronizeContext();" + NL + "" + NL + "java.util.Enumeration<?> propertyNames_";
  protected final String TEXT_32 = " = ";
  protected final String TEXT_33 = ".propertyNames();" + NL + "while (propertyNames_";
  protected final String TEXT_34 = ".hasMoreElements()) {" + NL + "\tString key_";
  protected final String TEXT_35 = " = (String) propertyNames_";
  protected final String TEXT_36 = ".nextElement();" + NL + "\tObject value_";
  protected final String TEXT_37 = " = (Object) ";
  protected final String TEXT_38 = ".get(key_";
  protected final String TEXT_39 = ");" + NL + "\tif (value_";
  protected final String TEXT_40 = " instanceof String)" + NL + "\t\t";
  protected final String TEXT_41 = "_thmapSB.addContextProperty(\"context.\"+key_";
  protected final String TEXT_42 = ", (String) value_";
  protected final String TEXT_43 = ");" + NL + "}" + NL + "" + NL + "// Set up outgoing RDDs";
  protected final String TEXT_44 = NL;
  protected final String TEXT_45 = "_tHMapInput.createDataFlowBuilder(";
  protected final String TEXT_46 = "_thmapSB.build()).build(";
  protected final String TEXT_47 = "_sdf);";
  protected final String TEXT_48 = NL + "    \tFileSystem.setDefaultUri(ctx.hadoopConfiguration(), currentURI_";
  protected final String TEXT_49 = "_config);" + NL + "        fs = FileSystem.get(ctx.hadoopConfiguration());";
  protected final String TEXT_50 = NL + "\trdd_";
  protected final String TEXT_51 = " = ";
  protected final String TEXT_52 = "_sdf.";
  protected final String TEXT_53 = "(\"";
  protected final String TEXT_54 = "\");";
  protected final String TEXT_55 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
// Parse the inputs to this javajet generator.
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode) codeGenArgument.getArgument();
final String cid = node.getUniqueName();

String paramsStr = ElementParameterParser.getValue(node,"__PARAMS__");;
String maxEngineIdlePeriod = ElementParameterParser.getValue(node,"__MAXENGINEIDLEPERIOD__");;
String storageLevel = ElementParameterParser.getValue(node,"__STORAGE_LEVEL__");;
String map = TDMUtils.getMapProjectPath(ElementParameterParser.getValue(node,"__MAP__"));;
String input = ElementParameterParser.getValue(node,"__INPUT__");;
String output = ElementParameterParser.getValue(node,"__OUTPUT__");;
THMapInputComponent tHMapInputComponent = ((THMapInputComponent) ((TDMExternalNodeProvider) node.getExternalNode()).getCurrentTDMNode());
THMapInputParms params = tHMapInputComponent.getAndValidate(paramsStr);
String projectArchives = ((TDMExternalNodeProvider) node.getExternalNode()).createTDMArchives(map, false);
TDMExternalNodeProvider nodeProvider = ((TDMExternalNodeProvider) node.getExternalNode());
map = nodeProvider.fixStructurePath(map);

final THMapUtil tHMapUtil = new THMapUtil(node);
String sparkDatasetClassName = tHMapUtil.getSparkDatasetClassName();
String sparkGetDatasetMethodName = tHMapUtil.getSparkGetDatasetMethodName();

String uriPrefix = "";
// Used for Spark only for now.
boolean useConfigurationComponent = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_STORAGE_CONFIGURATION__"));
if(useConfigurationComponent) {
    uriPrefix = org.talend.designer.spark.generator.storage.SparkStorageUtils.getURIPrefix(node);
    input = uriPrefix + " + " + input;
    output = uriPrefix + " + " + output;
}
if(uriPrefix.length() > 0) {

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(uriPrefix);
    stringBuffer.append(TEXT_3);
    
}
// For every output connection, an output RDD needs to be created in this context.
	if(tHMapUtil.getOutgoingConnections() != null) {
	for (org.talend.core.model.process.IConnection connection : tHMapUtil.getOutgoingConnections()) {

    stringBuffer.append(TEXT_4);
    stringBuffer.append(sparkDatasetClassName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(codeGenArgument.getRecordStructName(connection.getName()));
    stringBuffer.append(TEXT_6);
    stringBuffer.append(connection.getName());
    stringBuffer.append(TEXT_7);
    
	} // end for
	} // end if

    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(input);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(map);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(projectArchives);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(storageLevel);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(((TDMExternalNodeProvider) node.getExternalNode()).escapeString(params.toString()));
    stringBuffer.append(TEXT_21);
    
	// For every output connection, initialize the output RDD.
	if(tHMapUtil.getOutgoingConnections() != null) {
	for (org.talend.core.model.process.IConnection connection : tHMapUtil.getOutgoingConnections()) {

    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(connection.getName());
    stringBuffer.append(TEXT_24);
    stringBuffer.append(params.getConnectionId2ElementPath().get(connection.getName()));
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(connection.getName());
    stringBuffer.append(TEXT_27);
    stringBuffer.append(codeGenArgument.getRecordStructName(connection.getName()));
    stringBuffer.append(TEXT_28);
    
	} // end for
	} // end if

    stringBuffer.append(TEXT_29);
    
    String localContext = "context";

    stringBuffer.append(TEXT_30);
    stringBuffer.append(localContext);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_32);
    stringBuffer.append(localContext);
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(localContext);
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_47);
    
    if(uriPrefix.length() > 0) {
    
    stringBuffer.append(TEXT_48);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_49);
    
    }
    
    
	// For every output connection, initialize the output RDD.
	if(tHMapUtil.getOutgoingConnections() != null) {
	for (org.talend.core.model.process.IConnection connection : tHMapUtil.getOutgoingConnections()) {

    stringBuffer.append(TEXT_50);
    stringBuffer.append(connection.getName());
    stringBuffer.append(TEXT_51);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(sparkGetDatasetMethodName);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(connection.getName());
    stringBuffer.append(TEXT_54);
    
	} // end for
	} // end if

    stringBuffer.append(TEXT_55);
    return stringBuffer.toString();
  }
}
