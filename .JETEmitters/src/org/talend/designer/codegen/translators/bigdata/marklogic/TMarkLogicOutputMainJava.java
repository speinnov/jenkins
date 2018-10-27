package org.talend.designer.codegen.translators.bigdata.marklogic;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.IMetadataColumn;
import java.util.List;

public class TMarkLogicOutputMainJava
{
  protected static String nl;
  public static synchronized TMarkLogicOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMarkLogicOutputMainJava result = new TMarkLogicOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "           ";
  protected final String TEXT_3 = " = null;";
  protected final String TEXT_4 = NL + "                   ";
  protected final String TEXT_5 = " = null;";
  protected final String TEXT_6 = NL + "                    nb_line_";
  protected final String TEXT_7 = " ++;" + NL + "                    String docId_";
  protected final String TEXT_8 = " = null;" + NL + "                    Object docContent_";
  protected final String TEXT_9 = " = null;" + NL + "                    try{";
  protected final String TEXT_10 = NL + "                       docId_";
  protected final String TEXT_11 = " = ";
  protected final String TEXT_12 = ".docId;" + NL + "                       docMgr_";
  protected final String TEXT_13 = ".delete(docId_";
  protected final String TEXT_14 = ");" + NL + "                       nb_line_deleted_";
  protected final String TEXT_15 = " ++;";
  protected final String TEXT_16 = NL + "                        docContent_";
  protected final String TEXT_17 = " = ";
  protected final String TEXT_18 = ".docContent;" + NL + "                        com.marklogic.client.io.marker.AbstractWriteHandle genericHandle_";
  protected final String TEXT_19 = " = null;";
  protected final String TEXT_20 = NL + "                            if(docContent_";
  protected final String TEXT_21 = " instanceof byte[]){" + NL + "                                 genericHandle_";
  protected final String TEXT_22 = " = new com.marklogic.client.io.BytesHandle((byte[])docContent_";
  protected final String TEXT_23 = ");" + NL + "                            }else if(docContent_";
  protected final String TEXT_24 = " instanceof java.io.File){" + NL + "                                 genericHandle_";
  protected final String TEXT_25 = " = new com.marklogic.client.io.FileHandle((java.io.File)docContent_";
  protected final String TEXT_26 = ");" + NL + "                            }else if(docContent_";
  protected final String TEXT_27 = " instanceof String){" + NL + "                                 genericHandle_";
  protected final String TEXT_28 = " = new com.marklogic.client.io.StringHandle((String)docContent_";
  protected final String TEXT_29 = ");" + NL + "                            }else{" + NL + "                                 throw new RuntimeException(\"Unsupported Content Represent in \" + docContent_";
  protected final String TEXT_30 = ".getClass());" + NL + "                            }";
  protected final String TEXT_31 = NL + "                                genericHandle_";
  protected final String TEXT_32 = " = new com.marklogic.client.io.StringHandle((String)docContent_";
  protected final String TEXT_33 = ");";
  protected final String TEXT_34 = NL + "                        com.marklogic.client.document.DocumentUriTemplate docUriTpl_";
  protected final String TEXT_35 = " = docMgr_";
  protected final String TEXT_36 = ".newDocumentUriTemplate(docIdSuffix_";
  protected final String TEXT_37 = ");";
  protected final String TEXT_38 = NL + "                           String docIdPrefix_";
  protected final String TEXT_39 = " = ";
  protected final String TEXT_40 = ";" + NL + "                           if(!(docIdPrefix_";
  protected final String TEXT_41 = ".endsWith(\"/\") || docIdPrefix_";
  protected final String TEXT_42 = ".endsWith(\"\\\\\"))){" + NL + "                               docIdPrefix_";
  protected final String TEXT_43 = " = docIdPrefix_";
  protected final String TEXT_44 = " + \"/\";" + NL + "                           }" + NL + "                           docUriTpl_";
  protected final String TEXT_45 = ".setDirectory(docIdPrefix_";
  protected final String TEXT_46 = ".replace(\"\\\\\", \"/\"));";
  protected final String TEXT_47 = NL + "                        com.marklogic.client.document.DocumentDescriptor docDesc_";
  protected final String TEXT_48 = " = docMgr_";
  protected final String TEXT_49 = ".create(docUriTpl_";
  protected final String TEXT_50 = ", genericHandle_";
  protected final String TEXT_51 = ");" + NL + "                        docId_";
  protected final String TEXT_52 = " = docDesc_";
  protected final String TEXT_53 = ".getUri();";
  protected final String TEXT_54 = NL + "                        docId_";
  protected final String TEXT_55 = " = ";
  protected final String TEXT_56 = ".docId;";
  protected final String TEXT_57 = NL + "                            docMgr_";
  protected final String TEXT_58 = ".write(docId_";
  protected final String TEXT_59 = ", genericHandle_";
  protected final String TEXT_60 = ");" + NL + "                            nb_line_inserted_";
  protected final String TEXT_61 = " ++;";
  protected final String TEXT_62 = NL + "                        docId_";
  protected final String TEXT_63 = " = ";
  protected final String TEXT_64 = ".docId;" + NL + "                        docContent_";
  protected final String TEXT_65 = " = ";
  protected final String TEXT_66 = ".docContent;" + NL + "                        com.marklogic.client.io.StringHandle strHandle_";
  protected final String TEXT_67 = " = new com.marklogic.client.io.StringHandle((String)docContent_";
  protected final String TEXT_68 = ");";
  protected final String TEXT_69 = NL + "                        strHandle_";
  protected final String TEXT_70 = ".withFormat(com.marklogic.client.io.Format.JSON);";
  protected final String TEXT_71 = NL + "                        strHandle_";
  protected final String TEXT_72 = ".withFormat(com.marklogic.client.io.Format.XML);";
  protected final String TEXT_73 = NL + "                           Compile Err: Unsupported Doc Type \"";
  protected final String TEXT_74 = "\" for Update.";
  protected final String TEXT_75 = NL + "                        docMgr_";
  protected final String TEXT_76 = ".patch(docId_";
  protected final String TEXT_77 = ", strHandle_";
  protected final String TEXT_78 = ");" + NL + "                        nb_line_updated_";
  protected final String TEXT_79 = " ++;";
  protected final String TEXT_80 = NL + "                       Compile Err: UNSUPPORTED_ACTION_TYPE ";
  protected final String TEXT_81 = NL + "                               ";
  protected final String TEXT_82 = " = new ";
  protected final String TEXT_83 = "Struct();";
  protected final String TEXT_84 = NL + "                       ";
  protected final String TEXT_85 = ".docId = docId_";
  protected final String TEXT_86 = ";";
  protected final String TEXT_87 = NL + "                       ";
  protected final String TEXT_88 = ".docContent = docContent_";
  protected final String TEXT_89 = ";";
  protected final String TEXT_90 = NL + "                        }catch(" + NL + "                                 com.marklogic.client.ResourceNotFoundException" + NL + "                               | com.marklogic.client.ForbiddenUserException" + NL + "                               | com.marklogic.client.FailedRequestException e" + NL + "                              ){";
  protected final String TEXT_91 = NL + "                           ";
  protected final String TEXT_92 = " = new ";
  protected final String TEXT_93 = "Struct();";
  protected final String TEXT_94 = NL + "                           ";
  protected final String TEXT_95 = ".docId = docId_";
  protected final String TEXT_96 = ";";
  protected final String TEXT_97 = NL + "                           ";
  protected final String TEXT_98 = ".docContent = docContent_";
  protected final String TEXT_99 = ";";
  protected final String TEXT_100 = NL + "                           ";
  protected final String TEXT_101 = ".errMessage = e.getMessage();" + NL + "                           nb_line_rejected_";
  protected final String TEXT_102 = " ++;";
  protected final String TEXT_103 = NL + "                           e.printStackTrace();";
  protected final String TEXT_104 = NL + "                        }catch(Exception e){" + NL + "                            e.printStackTrace();" + NL + "                        }";
  protected final String TEXT_105 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();

    String actionType = ElementParameterParser.getValue(node,"__ACTION_TYPE__");
    String docType = ElementParameterParser.getValue(node,"__DOC_TYPE__");

    String docIdPrefix = ElementParameterParser.getValue(node,"__DOC_ID_PREFIX__");
    boolean autoGenerateDocId = "true".equals(ElementParameterParser.getValue(node,"__AUTO_GENERATE_DOC_ID__"));
            autoGenerateDocId = "INSERT".equals(actionType) && autoGenerateDocId;

    boolean hasRejectConn = false;
    String rejectConnName = null;
    List<? extends IConnection> rejectConns = node.getOutgoingConnections("REJECT");
    if(rejectConns != null && rejectConns.size() > 0) {
        rejectConnName = rejectConns.get(0).getName();
        hasRejectConn = true;
        
    stringBuffer.append(TEXT_2);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_3);
    
    }

    List<IMetadataTable> metadataList = node.getMetadataList();
    if((metadataList != null) && (metadataList.size() > 0)){//Tag A
        IMetadataTable metadata = metadataList.get(0);
        if(metadata != null){//Tag B
            List<? extends IConnection> outgoingConns = node.getOutgoingSortedConnections();
            boolean hasMainFlow = false;
            for(IConnection conn : outgoingConns){
               if(conn.getConnectorName().equals("FLOW")){
                  hasMainFlow = true;
                  break;
               }
            }
            IConnection outputConn = null;
            if(hasMainFlow){
                outputConn = outgoingConns.get(0);
                
    stringBuffer.append(TEXT_4);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_5);
    
            }
            List<? extends IConnection> conns = node.getIncomingConnections();
            for(IConnection conn : conns){//Tag C
                if(conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)){//Tag D
                    String connName = conn.getName();
                    List<IMetadataColumn> columns = metadata.getListColumns();
                    
    stringBuffer.append(TEXT_6);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_9);
    
                    if("DELETE".equals(actionType)){
                    
    stringBuffer.append(TEXT_10);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_11);
    stringBuffer.append( connName );
    stringBuffer.append(TEXT_12);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_13);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_14);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_15);
    
                    }else if("INSERT".equals(actionType)){
                    
    stringBuffer.append(TEXT_16);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_17);
    stringBuffer.append( connName );
    stringBuffer.append(TEXT_18);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_19);
    
                         if("BINARY".equals(docType)){
                         
    stringBuffer.append(TEXT_20);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_21);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_22);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_23);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_24);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_25);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_26);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_29);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_30);
    
                         }else{
                         
    stringBuffer.append(TEXT_31);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_32);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_33);
    
                         }
                    
    
                        if(autoGenerateDocId){
                        
    stringBuffer.append(TEXT_34);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_36);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_37);
    
                        if(docIdPrefix != null && !"".equals(docIdPrefix) && !"\"\"".equals(docIdPrefix)){
                        
    stringBuffer.append(TEXT_38);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_39);
    stringBuffer.append( docIdPrefix );
    stringBuffer.append(TEXT_40);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_41);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_43);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_44);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_45);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_46);
    
                        }
                         
    stringBuffer.append(TEXT_47);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_48);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_49);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_50);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_51);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_52);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_53);
    
                        }else{
                        
    stringBuffer.append(TEXT_54);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_55);
    stringBuffer.append( connName );
    stringBuffer.append(TEXT_56);
    
                    }
                    
    stringBuffer.append(TEXT_57);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_58);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_59);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_60);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_61);
    
                    }else if("PATCH".equals(actionType)){
                    
    stringBuffer.append(TEXT_62);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_63);
    stringBuffer.append( connName );
    stringBuffer.append(TEXT_64);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_65);
    stringBuffer.append( connName );
    stringBuffer.append(TEXT_66);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_67);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_68);
    
                        if("JSON".equals(docType)){
                        
    stringBuffer.append(TEXT_69);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_70);
    
                        }else if("XML".equals(docType)){
                        
    stringBuffer.append(TEXT_71);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_72);
    
                        }else{
                        
    stringBuffer.append(TEXT_73);
    stringBuffer.append( docType );
    stringBuffer.append(TEXT_74);
    
                        }
                         
    stringBuffer.append(TEXT_75);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_76);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_77);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_78);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_79);
    
                    }else{
                    
    stringBuffer.append(TEXT_80);
    stringBuffer.append( actionType );
    
                    }
                    
    
                            if(hasMainFlow){
                    
    stringBuffer.append(TEXT_81);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_82);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_83);
    stringBuffer.append(TEXT_84);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_85);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_86);
    stringBuffer.append(TEXT_87);
    stringBuffer.append( outputConn.getName() );
    stringBuffer.append(TEXT_88);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_89);
    
                            }
                            
    stringBuffer.append(TEXT_90);
    
                        if(hasRejectConn){
                        
    stringBuffer.append(TEXT_91);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_92);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_93);
    stringBuffer.append(TEXT_94);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_95);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_96);
    stringBuffer.append(TEXT_97);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_98);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_99);
    stringBuffer.append(TEXT_100);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_101);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_102);
    
                        }else{
                        
    stringBuffer.append(TEXT_103);
    
                        }
                         
    stringBuffer.append(TEXT_104);
    
                }//Tag D
            }//Tag C
        }//Tag B
    }//Tag A

    stringBuffer.append(TEXT_105);
    return stringBuffer.toString();
  }
}
