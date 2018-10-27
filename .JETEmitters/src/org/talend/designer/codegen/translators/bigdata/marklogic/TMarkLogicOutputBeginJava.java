package org.talend.designer.codegen.translators.bigdata.marklogic;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TMarkLogicOutputBeginJava
{
  protected static String nl;
  public static synchronized TMarkLogicOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMarkLogicOutputBeginJava result = new TMarkLogicOutputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_3 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_4 = ");";
  protected final String TEXT_5 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_6 = " = ";
  protected final String TEXT_7 = "; ";
  protected final String TEXT_8 = NL + NL + "    int nb_line_";
  protected final String TEXT_9 = " = 0;" + NL + "    int nb_line_inserted_";
  protected final String TEXT_10 = " = 0;" + NL + "    int nb_line_deleted_";
  protected final String TEXT_11 = " = 0;" + NL + "    int nb_line_updated_";
  protected final String TEXT_12 = " = 0;" + NL + "    int nb_line_rejected_";
  protected final String TEXT_13 = " = 0;" + NL + "" + NL + "       String docIdSuffix_";
  protected final String TEXT_14 = " = \"\";" + NL + "       com.marklogic.client.DatabaseClient client_";
  protected final String TEXT_15 = " = null;";
  protected final String TEXT_16 = NL + "        client_";
  protected final String TEXT_17 = " = (com.marklogic.client.DatabaseClient)globalMap.get(\"";
  protected final String TEXT_18 = "\");";
  protected final String TEXT_19 = NL + "        com.marklogic.client.DatabaseClientFactory.Authentication authType_";
  protected final String TEXT_20 = " = null;";
  protected final String TEXT_21 = NL + "                   authType_";
  protected final String TEXT_22 = " = com.marklogic.client.DatabaseClientFactory.Authentication.DIGEST;";
  protected final String TEXT_23 = NL + "                   authType_";
  protected final String TEXT_24 = " = com.marklogic.client.DatabaseClientFactory.Authentication.BASIC;";
  protected final String TEXT_25 = NL + "                   Compile Err: UNKNOWN_AUTHENTICATION_TYPE: ";
  protected final String TEXT_26 = NL + "                   client_";
  protected final String TEXT_27 = " = com.marklogic.client.DatabaseClientFactory.newClient(";
  protected final String TEXT_28 = NL + "                          ";
  protected final String TEXT_29 = ", Integer.valueOf(";
  protected final String TEXT_30 = "), ";
  protected final String TEXT_31 = ", ";
  protected final String TEXT_32 = ", decryptedPassword_";
  protected final String TEXT_33 = ", authType_";
  protected final String TEXT_34 = NL + "                  );";
  protected final String TEXT_35 = NL + "                   com.marklogic.client.document.DocumentManager docMgr_";
  protected final String TEXT_36 = " = null;";
  protected final String TEXT_37 = NL + "       docMgr_";
  protected final String TEXT_38 = " = client_";
  protected final String TEXT_39 = ".newDocumentManager();";
  protected final String TEXT_40 = NL + "       docMgr_";
  protected final String TEXT_41 = " = client_";
  protected final String TEXT_42 = ".newTextDocumentManager();" + NL + "       docIdSuffix_";
  protected final String TEXT_43 = " = \"txt\";";
  protected final String TEXT_44 = NL + "       docMgr_";
  protected final String TEXT_45 = " = client_";
  protected final String TEXT_46 = ".newJSONDocumentManager();" + NL + "       docIdSuffix_";
  protected final String TEXT_47 = " = \"json\";";
  protected final String TEXT_48 = NL + "       docMgr_";
  protected final String TEXT_49 = " = client_";
  protected final String TEXT_50 = ".newXMLDocumentManager();" + NL + "       docIdSuffix_";
  protected final String TEXT_51 = " = \"xml\";";
  protected final String TEXT_52 = NL + "       docMgr_";
  protected final String TEXT_53 = " = client_";
  protected final String TEXT_54 = ".newBinaryDocumentManager();" + NL + "       docIdSuffix_";
  protected final String TEXT_55 = " = \"bin\";";
  protected final String TEXT_56 = NL + "       Compile Err: UNSUPPORTED_DOC_TYPE ";
  protected final String TEXT_57 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();

    String host = ElementParameterParser.getValue(node,"__HOST__");
    String port = ElementParameterParser.getValue(node,"__PORT__");
    String userName = ElementParameterParser.getValue(node,"__USER_NAME__");
    String database = ElementParameterParser.getValue(node,"__DATABASE__");
    String authType = ElementParameterParser.getValue(node,"__AUTHENTICATION_TYPE__");

    boolean reuseConn = "true".equals(ElementParameterParser.getValue(node,"__USE_EXISTING_CONNECTION__"));
    String docType = ElementParameterParser.getValue(node,"__DOC_TYPE__");
    String actionType = ElementParameterParser.getValue(node,"__ACTION_TYPE__");
    if("DELETE".equals(actionType)){
        docType = "GENERIC";
    }

    String passwordFieldName = "__PASSWORD__";
 
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_4);
    } else {
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_7);
    }
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_13);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_14);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_15);
    
    if(reuseConn){
      String connection = ElementParameterParser.getValue(node,"__CONNECTION__");
      String conn = "conn_" + connection;
     
    stringBuffer.append(TEXT_16);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(conn);
    stringBuffer.append(TEXT_18);
    
    }else{
     
    stringBuffer.append(TEXT_19);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_20);
    
            if("DIGEST".equals(authType)){
                
    stringBuffer.append(TEXT_21);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_22);
    
            }else if("BASIC".equals(authType)){
                
    stringBuffer.append(TEXT_23);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_24);
    
            }else{
                
    stringBuffer.append(TEXT_25);
    stringBuffer.append( authType );
    
            }
        
    stringBuffer.append(TEXT_26);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(TEXT_28);
    stringBuffer.append( host );
    stringBuffer.append(TEXT_29);
    stringBuffer.append( port );
    stringBuffer.append(TEXT_30);
    stringBuffer.append( database );
    stringBuffer.append(TEXT_31);
    stringBuffer.append( userName );
    stringBuffer.append(TEXT_32);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_33);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_34);
    
    }
 
    stringBuffer.append(TEXT_35);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_36);
    
    if("GENERIC".equals(docType)){
    
    stringBuffer.append(TEXT_37);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_38);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_39);
    
    }else if("TEXT".equals(docType)){
    
    stringBuffer.append(TEXT_40);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_41);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_43);
    
    }else if("JSON".equals(docType)){
    
    stringBuffer.append(TEXT_44);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_45);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_46);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_47);
    
    }else if("XML".equals(docType)){
    
    stringBuffer.append(TEXT_48);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_49);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_50);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_51);
    
    }else if("BINARY".equals(docType)){
    
    stringBuffer.append(TEXT_52);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_53);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_54);
    stringBuffer.append( cid );
    stringBuffer.append(TEXT_55);
    
    }else {
    
    stringBuffer.append(TEXT_56);
    stringBuffer.append( docType );
    
    }

    stringBuffer.append(TEXT_57);
    return stringBuffer.toString();
  }
}
