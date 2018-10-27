package org.talend.designer.codegen.translators.common;

import org.talend.designer.codegen.config.NodesSubTree;
import org.talend.core.model.process.INode;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Vector;
import org.talend.core.model.process.IProcess;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IHashableInputConnections;
import org.talend.core.model.process.IHashConfiguration;
import org.talend.core.model.process.IHashableColumn;
import org.talend.core.model.utils.NodeUtil;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.utils.TalendQuoteUtils;

public class Spark_subprocess_headerJava
{
  protected static String nl;
  public static synchronized Spark_subprocess_headerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Spark_subprocess_headerJava result = new Spark_subprocess_headerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "        public void ";
  protected final String TEXT_3 = "Process() throws java.lang.Exception {" + NL + "" + NL + "            try {";
  protected final String TEXT_4 = NL + "        public void ";
  protected final String TEXT_5 = "Process(final org.apache.spark.api.java.JavaSparkContext ctx, GlobalVar globalMap) throws java.lang.Exception {" + NL + "            FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());" + NL + "            final JobConf job = new JobConf(ctx.hadoopConfiguration());" + NL + "" + NL + "            try {";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    NodesSubTree subTree = (NodesSubTree)v.get(1);

    stringBuffer.append(TEXT_1);
    
    if(subTree.getName().startsWith("tHadoopConfManager")) {

    stringBuffer.append(TEXT_2);
    stringBuffer.append(subTree.getName());
    stringBuffer.append(TEXT_3);
        
    } else {

    stringBuffer.append(TEXT_4);
    stringBuffer.append(subTree.getName());
    stringBuffer.append(TEXT_5);
    
    }

    return stringBuffer.toString();
  }
}
