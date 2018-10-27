package org.talend.designer.codegen.translators.deprecated;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import org.talend.commons.utils.StringUtils;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnectionCategory;

public class TQASAddressIncompleteMainJava
{
  protected static String nl;
  public static synchronized TQASAddressIncompleteMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TQASAddressIncompleteMainJava result = new TQASAddressIncompleteMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t";
  protected final String TEXT_3 = " = null;\t\t\t\t";
  protected final String TEXT_4 = NL + "\t";
  protected final String TEXT_5 = " = null;";
  protected final String TEXT_6 = NL + "\tString[] results_";
  protected final String TEXT_7 = " = null;" + NL + "    results_";
  protected final String TEXT_8 = " = qasClient_";
  protected final String TEXT_9 = ".getRightAddress(";
  protected final String TEXT_10 = ".";
  protected final String TEXT_11 = ");" + NL + "if(results_";
  protected final String TEXT_12 = "[0].equals(\"StreetPartial\") || results_";
  protected final String TEXT_13 = "[0].equals(\"PremisesPartial\")){";
  protected final String TEXT_14 = NL + "\tif(";
  protected final String TEXT_15 = " == null){" + NL + "\t\t";
  protected final String TEXT_16 = " = new ";
  protected final String TEXT_17 = "Struct();" + NL + "\t}";
  protected final String TEXT_18 = NL + "                                " + NL + "                                int iLenRetAddr_";
  protected final String TEXT_19 = " = results_";
  protected final String TEXT_20 = ".length;" + NL + "                                ";
  protected final String TEXT_21 = NL + "                                        ";
  protected final String TEXT_22 = ".";
  protected final String TEXT_23 = " = ";
  protected final String TEXT_24 = ".";
  protected final String TEXT_25 = ";";
  protected final String TEXT_26 = "                                 ";
  protected final String TEXT_27 = NL + "                                            ";
  protected final String TEXT_28 = ".";
  protected final String TEXT_29 = " = (";
  protected final String TEXT_30 = " < iLenRetAddr_";
  protected final String TEXT_31 = ") ? results_";
  protected final String TEXT_32 = "[";
  protected final String TEXT_33 = "] : \"\";";
  protected final String TEXT_34 = NL + "} else if( results_";
  protected final String TEXT_35 = "[0].equals(\"None\") || results_";
  protected final String TEXT_36 = "[0].equals(\"Multiple\") ){";
  protected final String TEXT_37 = NL + "\tif(";
  protected final String TEXT_38 = " == null){" + NL + "\t\t";
  protected final String TEXT_39 = " = new ";
  protected final String TEXT_40 = "Struct();" + NL + "\t}";
  protected final String TEXT_41 = NL + "                                " + NL + "                                int iLenRetAddr_";
  protected final String TEXT_42 = " = results_";
  protected final String TEXT_43 = ".length;" + NL + "                                ";
  protected final String TEXT_44 = NL + "                                        ";
  protected final String TEXT_45 = ".";
  protected final String TEXT_46 = " = ";
  protected final String TEXT_47 = ".";
  protected final String TEXT_48 = ";";
  protected final String TEXT_49 = "                                 ";
  protected final String TEXT_50 = NL + "                                            ";
  protected final String TEXT_51 = ".";
  protected final String TEXT_52 = " = (";
  protected final String TEXT_53 = " < iLenRetAddr_";
  protected final String TEXT_54 = ") ? results_";
  protected final String TEXT_55 = "[";
  protected final String TEXT_56 = "] : \"\";";
  protected final String TEXT_57 = NL + "}" + NL + "\t\t\t\t\tnb_line_";
  protected final String TEXT_58 = "++;";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String wsdlAddress = ElementParameterParser.getValue(node, "__WSDL_ADDRESS__");
	String country = ElementParameterParser.getValue(node, "__COUNTRY__");
	String addressToVerify = ElementParameterParser.getValue(node, "__ANALYZED_COLUMN__");
	List<IMetadataTable> metadatas = node.getMetadataList();
	
	boolean verifiedExist = false;
	boolean rejectExist = false;
	
	if ((metadatas!=null)&&(metadatas.size()>0)) {
		IMetadataTable metadata = metadatas.get(0);
		if (metadata!=null) {
			String cid = node.getUniqueName();
			List< ? extends IConnection> conns = node.getIncomingConnections();
			if(conns!=null){
				if (conns.size()>0){
					IConnection conn =conns.get(0);

String verifiedConnName = null;
List<? extends IConnection> connsVerified = node.getOutgoingConnections("INCOMPLETE");
if(connsVerified != null && connsVerified.size() > 0) {
    for(IConnection vConn : connsVerified) {
        verifiedConnName = vConn.getName();
        verifiedExist = true;

    stringBuffer.append(TEXT_2);
    stringBuffer.append( verifiedConnName );
    stringBuffer.append(TEXT_3);
           
    }
}

String rejectConnName = null;
List<? extends IConnection> connsReject = node.getOutgoingConnections("REJECT");
if(connsReject != null && connsReject.size() > 0) {
    for(IConnection rConn : connsReject) {
        rejectConnName = rConn.getName();
        rejectExist = true;

    stringBuffer.append(TEXT_4);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_5);
           
    }
}

    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_10);
    stringBuffer.append( addressToVerify );
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
    
	if( verifiedExist ){

    stringBuffer.append(TEXT_14);
    stringBuffer.append( verifiedConnName );
    stringBuffer.append(TEXT_15);
    stringBuffer.append( verifiedConnName );
    stringBuffer.append(TEXT_16);
    stringBuffer.append( verifiedConnName );
    stringBuffer.append(TEXT_17);
     }
	                if (connsVerified!=null) {
    	            	List<IMetadataColumn> columnsout = metadata.getListColumns();
            	        for (int i=0;i < connsVerified.size();i++) {
            	        	IConnection connout = connsVerified.get(i);
                    	    if(connout.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                    	        
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    
                                int k = 1;
                              
                                for (IMetadataColumn column : columnsout){
                                    String sColumnName = column.getLabel();
                                    boolean bReadOnly = column.isReadOnly();
                                    
                                    if (!bReadOnly){
                                    
    stringBuffer.append(TEXT_21);
    stringBuffer.append(connout.getName() );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_24);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_25);
    
                                      // ...addtional colums
                                    } else{
                                        
    stringBuffer.append(TEXT_26);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(connout.getName() );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_29);
    stringBuffer.append(k);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_32);
    stringBuffer.append(k++);
    stringBuffer.append(TEXT_33);
    
                                    }
                                }
                                
							}
						}
					}

    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_36);
    
	if( rejectExist ){

    stringBuffer.append(TEXT_37);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_38);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_39);
    stringBuffer.append( rejectConnName );
    stringBuffer.append(TEXT_40);
    		
	}
			if (connsReject!=null) {
    	            	List<IMetadataColumn> columnsout = metadata.getListColumns();
            	        for (int i=0;i<connsReject.size();i++) {
            	        	IConnection connout = connsReject.get(i);
                    	    if(connout.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                                
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_43);
    
                                int k = 1;
                              
                                for (IMetadataColumn column : columnsout){
                                    String sColumnName = column.getLabel();
                                    boolean bReadOnly = column.isReadOnly();
                                    
                                    if (!bReadOnly){
                                    
    stringBuffer.append(TEXT_44);
    stringBuffer.append(connout.getName() );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_48);
    
                                      // ...addtional colums
                                    } else{
                                        
    stringBuffer.append(TEXT_49);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(connout.getName() );
    stringBuffer.append(TEXT_51);
    stringBuffer.append(sColumnName);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(k);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(k++);
    stringBuffer.append(TEXT_56);
    
                                    }
                                }
                                
							}
						}
					}

    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_58);
    				}
			}
		}
	}

    return stringBuffer.toString();
  }
}
