package org.talend.designer.codegen.translators.bigdata.couchbase;

import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnectionCategory;
import java.util.List;

public class TCouchbaseOutputMainJava
{
  protected static String nl;
  public static synchronized TCouchbaseOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCouchbaseOutputMainJava result = new TCouchbaseOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "\t    \t";
  protected final String TEXT_3 = " = null;" + NL + "\t  \t";
  protected final String TEXT_4 = NL + "\t                        whetherReject_";
  protected final String TEXT_5 = " = false;" + NL + "\t\t\t\t\t\t\ttry {" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_6 = NL + "\t                            \tnet.spy.memcached.internal.OperationFuture<Boolean> operation_";
  protected final String TEXT_7 = " = client_";
  protected final String TEXT_8 = ".delete(";
  protected final String TEXT_9 = ".";
  protected final String TEXT_10 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_11 = NL + "\t                                \tnet.spy.memcached.internal.OperationFuture<Boolean> operation_";
  protected final String TEXT_12 = " = client_";
  protected final String TEXT_13 = ".";
  protected final String TEXT_14 = "(";
  protected final String TEXT_15 = ".";
  protected final String TEXT_16 = ".toString(), ";
  protected final String TEXT_17 = ", ";
  protected final String TEXT_18 = ".";
  protected final String TEXT_19 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_20 = NL + "\t                                \tnet.spy.memcached.internal.OperationFuture<Boolean> operation_";
  protected final String TEXT_21 = " = client_";
  protected final String TEXT_22 = ".";
  protected final String TEXT_23 = "(";
  protected final String TEXT_24 = ".";
  protected final String TEXT_25 = ", ";
  protected final String TEXT_26 = ", ";
  protected final String TEXT_27 = ".";
  protected final String TEXT_28 = ");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_29 = NL + "\t                        \tif (operation_";
  protected final String TEXT_30 = ".get().booleanValue()) {" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_31 = NL + "\t\t                                nb_line_inserted_";
  protected final String TEXT_32 = "++;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_33 = NL + "\t\t                                nb_line_update_";
  protected final String TEXT_34 = "++;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_35 = NL + "\t\t                                nb_line_inserted_updated_";
  protected final String TEXT_36 = "++;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_37 = NL + "\t\t                                nb_line_deleted_";
  protected final String TEXT_38 = "++;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_39 = NL + "\t\t\t\t\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_40 = " - Writing the record \" + (nb_line_";
  protected final String TEXT_41 = " + 1) + \" into bucket.\");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_42 = NL + "\t\t\t\t\t\t\t\t}else{" + NL + "\t\t\t\t\t\t\t\t\twhetherReject_";
  protected final String TEXT_43 = " = true;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_44 = NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_45 = " = new ";
  protected final String TEXT_46 = "Struct();" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_47 = NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_48 = ".";
  protected final String TEXT_49 = " = ";
  protected final String TEXT_50 = ".";
  protected final String TEXT_51 = ";" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_52 = NL + "\t\t\t\t\t\t\t\t\t\tnb_line_rejected_";
  protected final String TEXT_53 = " ++;" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_54 = ".errorMessage = \"";
  protected final String TEXT_55 = " key:\"+";
  protected final String TEXT_56 = ".";
  protected final String TEXT_57 = "+\" - Line: \" + tos_count_";
  protected final String TEXT_58 = ";" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_59 = NL + "\t\t\t\t\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_60 = " - Failed write the record \" + (nb_line_";
  protected final String TEXT_61 = " + 1) + \" into bucket.\");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_62 = NL + "\t\t                        }" + NL + "\t                    \t} catch (java.lang.Exception e_";
  protected final String TEXT_63 = ") {" + NL + "\t\t                        whetherReject_";
  protected final String TEXT_64 = " = true;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_65 = NL + "\t\t\t\t\t\t\t\t\tthrow(e_";
  protected final String TEXT_66 = ");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_67 = NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_68 = " = new ";
  protected final String TEXT_69 = "Struct();" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_70 = NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_71 = ".";
  protected final String TEXT_72 = " = ";
  protected final String TEXT_73 = ".";
  protected final String TEXT_74 = ";" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_75 = NL + "\t\t                                nb_line_rejected_";
  protected final String TEXT_76 = " ++;" + NL + "\t\t                                ";
  protected final String TEXT_77 = ".errorMessage = e_";
  protected final String TEXT_78 = ".getMessage() + \" - Line: \" + tos_count_";
  protected final String TEXT_79 = ";" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_80 = NL + "\t\t\t\t\t\t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_81 = " - ExecutionException while doing set: \" + e_";
  protected final String TEXT_82 = ".getMessage());" + NL + "\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_83 = NL + "\t\t                                System.err.println(\"ExecutionException while doing set: \" + e_";
  protected final String TEXT_84 = ".getMessage());" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_85 = NL + "\t\t\t\t\t\t\t\t\t\tlog.debug(\"";
  protected final String TEXT_86 = " - Failed write the record \" + nb_line_";
  protected final String TEXT_87 = " + \" into bucket.\");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_88 = NL + "\t\t                    }" + NL + "\t                    \tnb_line_";
  protected final String TEXT_89 = "++;" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_90 = NL + "\t\t\t\t\t\t\tif(!whetherReject_";
  protected final String TEXT_91 = ") {" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_92 = NL + "                                        \t";
  protected final String TEXT_93 = " = new ";
  protected final String TEXT_94 = "Struct();" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_95 = NL + "\t\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_96 = ".";
  protected final String TEXT_97 = " = ";
  protected final String TEXT_98 = ".";
  protected final String TEXT_99 = ";" + NL + "\t\t\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_100 = NL + "\t                        }" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_101 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	
	INode node = (INode)codeGenArgument.getArgument();
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
	
	String rejectConnName = null;
	List<? extends IConnection> rejectConns = node.getOutgoingConnections("REJECT");
	List<IMetadataColumn> rejectColumnList = null;
	if(rejectConns != null && rejectConns.size() > 0) {
		IConnection rejectConn = rejectConns.get(0);
		if(rejectConn!=null){
	    	rejectConnName = rejectConn.getName();
	    	IMetadataTable metadataTable = rejectConn.getMetadataTable();
	    	if(metadataTable!=null){
	        	rejectColumnList = metadataTable.getListColumns();
	    	}
	  	}
	}
	List<? extends IConnection> outgoingConns = node.getOutgoingSortedConnections();
	List<IMetadataColumn> columnList = null;
	
	for(IConnection tmpconn : outgoingConns) {
		if (tmpconn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			if(rejectConnName==null || !rejectConnName.equals(tmpconn.getName())){
	      		IMetadataTable outputMetadata = tmpconn.getMetadataTable();
	      		if(outputMetadata!=null){
	         		 columnList = outputMetadata.getListColumns();
	      		}
	    	}
	  		
    stringBuffer.append(TEXT_2);
    stringBuffer.append(tmpconn.getName() );
    stringBuffer.append(TEXT_3);
    
	  	}
	}
	String cid = node.getUniqueName();
	String dataAction = ElementParameterParser.getValue(node,"__DATA_ACTION__");
	String dieOnError = ElementParameterParser.getValue(node, "__DIE_ON_ERROR__");
	List<IMetadataTable> metadatas = node.getMetadataList();
	
	if ((metadatas!=null) && (metadatas.size() > 0)) {
	    IMetadataTable metadata = metadatas.get(0);
	    if (metadata != null) {
	        
	        String expire = ElementParameterParser.getValue(node, "__EXPIRE__");
	        String isjson = ElementParameterParser.getValue(node,"__IS_VIRTUAL_COMPONENT__");
	        List< ? extends IConnection> conns = node.getIncomingConnections();
	        if (conns != null){
	            if (conns.size()>0){
	                IConnection conn =conns.get(0);
	                String connName = conn.getName();
	                if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
	                    IMetadataTable meta =  conn.getMetadataTable();
	                    if(meta.getListColumns().size()>0){
	                        String key = ElementParameterParser.getValue(node,"__KEY__");
							
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    
	                        	if("delete".equals(dataAction)){
								
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(key);
    stringBuffer.append(TEXT_10);
    
								}else {
									if("true".equals(isjson)){
										String jsonfield = ElementParameterParser.getValue(node,"__JSONFIELD__");
										
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(dataAction);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(key);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(expire);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(jsonfield);
    stringBuffer.append(TEXT_19);
    
		                            }else{
		                                String value = ElementParameterParser.getValue(node,"__VALUE__");
										
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(dataAction);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(key);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(expire);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(value);
    stringBuffer.append(TEXT_28);
    
		                            }
		                        }
								
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    
									if("add".equals(dataAction)){
									
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    
									}else if("replace".equals(dataAction)){
									
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_34);
    
									}else if("set".equals(dataAction)){
									
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_36);
    
									}else if("delete".equals(dataAction)){
									
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_38);
    
									}
									if(isLog4jEnabled){
									
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_41);
    
									} 
									
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    
									if(rejectConnName != null && rejectColumnList != null && rejectColumnList.size() > 0) {
									
    stringBuffer.append(TEXT_44);
    stringBuffer.append(rejectConnName );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(rejectConnName );
    stringBuffer.append(TEXT_46);
    
										for(IMetadataColumn column1 : columnList) {
										
    stringBuffer.append(TEXT_47);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(column1.getLabel());
    stringBuffer.append(TEXT_49);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(column1.getLabel());
    stringBuffer.append(TEXT_51);
    
										}
										
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(dataAction);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(key);
    stringBuffer.append(TEXT_57);
    stringBuffer.append(node.getUniqueName() );
    stringBuffer.append(TEXT_58);
    
		                            }
		                            if(isLog4jEnabled){
									
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_61);
    
									}
		                            
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_64);
    
								if (("true").equals(dieOnError)) {
								
    stringBuffer.append(TEXT_65);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_66);
    
								} else {
									if(rejectConnName != null && rejectColumnList != null && rejectColumnList.size() > 0) {
									
    stringBuffer.append(TEXT_67);
    stringBuffer.append(rejectConnName );
    stringBuffer.append(TEXT_68);
    stringBuffer.append(rejectConnName );
    stringBuffer.append(TEXT_69);
    
										for(IMetadataColumn column1 : columnList) {
										
    stringBuffer.append(TEXT_70);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(column1.getLabel());
    stringBuffer.append(TEXT_72);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(column1.getLabel());
    stringBuffer.append(TEXT_74);
    
		                                }
		                                
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(rejectConnName);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    stringBuffer.append(node.getUniqueName() );
    stringBuffer.append(TEXT_79);
    
		                            } else {
										if(isLog4jEnabled){
										
    stringBuffer.append(TEXT_80);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    
										}
										
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    
		                            }
		                            if(isLog4jEnabled){
									
    stringBuffer.append(TEXT_85);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_87);
    
									}
		                        } 
		                        
    stringBuffer.append(TEXT_88);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_89);
    
						}       
                    	if(outgoingConns != null && outgoingConns.size() > 0) {
                        
    stringBuffer.append(TEXT_90);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_91);
    
								for(IConnection outgoingConn : outgoingConns) {
									if(rejectConnName == null || (rejectConnName != null && !outgoingConn.getName().equals(rejectConnName))) {
										if(outgoingConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
										
    stringBuffer.append(TEXT_92);
    stringBuffer.append(outgoingConn.getName());
    stringBuffer.append(TEXT_93);
    stringBuffer.append(outgoingConn.getName());
    stringBuffer.append(TEXT_94);
    
											for(IMetadataColumn column : columnList) {
											
    stringBuffer.append(TEXT_95);
    stringBuffer.append(outgoingConn.getName());
    stringBuffer.append(TEXT_96);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_97);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_99);
    
											}
										}
									}
								}
								
    stringBuffer.append(TEXT_100);
    
						}
					}
				}
			}
		}
	}
	
    stringBuffer.append(TEXT_101);
    return stringBuffer.toString();
  }
}
