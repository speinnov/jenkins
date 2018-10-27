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

public class Mr_subprocess_footerJava
{
  protected static String nl;
  public static synchronized Mr_subprocess_footerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Mr_subprocess_footerJava result = new Mr_subprocess_footerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "                        return null;" + NL + "                    }" + NL + "                });";
  protected final String TEXT_2 = NL + "        } // end the resume" + NL;
  protected final String TEXT_3 = NL + "                        if (resumeEntryMethodName == null || globalResumeTicket) {" + NL + "                            resumeUtil.addLog(\"CHECKPOINT\", \"CONNECTION:";
  protected final String TEXT_4 = ":";
  protected final String TEXT_5 = ":";
  protected final String TEXT_6 = "\", \"\", Thread.currentThread().getId() + \"\", \"\", \"\", \"\", \"\", \"\");" + NL + "                        }";
  protected final String TEXT_7 = NL + "                    ";
  protected final String TEXT_8 = "Process(globalMap);";
  protected final String TEXT_9 = NL + NL + "    } catch(Exception e) {" + NL + "        throw e;" + NL + "    } catch(java.lang.Error error) {" + NL + "        throw error;" + NL + "    }" + NL + "}" + NL + NL;
  protected final String TEXT_10 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    INode mrconn = (INode)v.get(0);
    NodesSubTree subTree = (NodesSubTree)v.get(1);

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
                String username = ElementParameterParser.getValue(mrconn,"__USERNAME__");
                String mrDistribution = ElementParameterParser.getValue(mrconn,"__DISTRIBUTION__");
                String mrVersion = ElementParameterParser.getValue(mrconn,"__MR_VERSION__");

                org.talend.hadoop.distribution.component.MRComponent mrDistrib = null;
                try {
                    mrDistrib = (org.talend.hadoop.distribution.component.MRComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(mrDistribution, mrVersion);
                } catch (java.lang.Exception e) {
                    e.printStackTrace();
                    return "";
                }
                boolean isCustom = mrDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;

                boolean mrUseKrb = false;
                if(isCustom || mrDistrib.doSupportKerberos()){
                    mrUseKrb = "true".equals(ElementParameterParser.getValue(mrconn,"__USE_KRB__"));
                }
                if(!(username == null || "".equals(username) || "\"\"".equals(username) || mrUseKrb)){
                
    stringBuffer.append(TEXT_1);
    
                }
            }
        }

        
    stringBuffer.append(TEXT_2);
    
        {
            //////////////////////////////////////////////////////////////////////////////
            List<String> beforeSubProcesses = subTree.getBeforeSubProcesses();
            for(IConnection conn : firstNode.getOutgoingConnections()) {

                //boolean activeResume = "true".equals(ElementParameterParser.getValue(conn, "__RESUMING_CHECKPOINT__"));
                boolean activeResume = true;

                //System.out.println("Test:" + ElementParameterParser.getValue(conn, "__RESUMING_CHECKPOINT__"));

                String uniqueNameTargetNode = conn.getTarget().getUniqueName();
                EConnectionType lineStyle = conn.getLineStyle();

                if (beforeSubProcesses.indexOf(uniqueNameTargetNode) != -1) {
                    if (activeResume){
                    
    stringBuffer.append(TEXT_3);
    stringBuffer.append(lineStyle.getName() );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(subTree.getName() );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(conn.getOutputId() > 0 ? conn.getOutputId() : "" );
    stringBuffer.append(TEXT_6);
    
                    }
                    
    stringBuffer.append(TEXT_7);
    stringBuffer.append( uniqueNameTargetNode );
    stringBuffer.append(TEXT_8);
    
                }
            }
        }
        //////////////////////////////////////////////////////////////////////////////
        
    stringBuffer.append(TEXT_9);
    stringBuffer.append(TEXT_10);
    return stringBuffer.toString();
  }
}
