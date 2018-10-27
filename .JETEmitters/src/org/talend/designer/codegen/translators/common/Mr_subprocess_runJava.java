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
import org.talend.core.model.process.AbstractNode;

public class Mr_subprocess_runJava
{
  protected static String nl;
  public static synchronized Mr_subprocess_runJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Mr_subprocess_runJava result = new Mr_subprocess_runJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "\t\t\trunMRJob(job, ";
  protected final String TEXT_3 = ", ";
  protected final String TEXT_4 = ");" + NL + "\t\t";
  protected final String TEXT_5 = NL + "\t" + NL;
  protected final String TEXT_6 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    INode mrconn = (INode)v.get(0);
    NodesSubTree subTree = (NodesSubTree)v.get(1);
    AbstractNode abRootNode = (AbstractNode)subTree.getRootNode();

	INode firstNode = subTree.getNode(subTree.getName());
	List< ? extends IConnection> conns = firstNode.getOutgoingSortedConnections();
	String firstConnName = "";
	if(conns != null && conns.size()>0){
		IConnection conn = conns.get(0);
		if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			firstConnName = conn.getName();
		}
	}
	if(!"".equals(firstConnName)){
		if(subTree.getNodes().size() > 1){
		
    stringBuffer.append(TEXT_2);
    stringBuffer.append(abRootNode.getMRGroupId());
    stringBuffer.append(TEXT_3);
    stringBuffer.append(abRootNode.getMrJobIDInGroup());
    stringBuffer.append(TEXT_4);
    
		}
	}
	
    stringBuffer.append(TEXT_5);
    stringBuffer.append(TEXT_6);
    return stringBuffer.toString();
  }
}
