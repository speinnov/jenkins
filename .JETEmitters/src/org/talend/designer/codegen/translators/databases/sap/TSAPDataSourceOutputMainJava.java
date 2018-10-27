package org.talend.designer.codegen.translators.databases.sap;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.utils.TalendTextUtils;

public class TSAPDataSourceOutputMainJava
{
  protected static String nl;
  public static synchronized TSAPDataSourceOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TSAPDataSourceOutputMainJava result = new TSAPDataSourceOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\tdata_";
  protected final String TEXT_3 = ".appendRow();";
  protected final String TEXT_4 = NL + "\t\t\tdata_";
  protected final String TEXT_5 = ".setString(";
  protected final String TEXT_6 = ", ";
  protected final String TEXT_7 = ".";
  protected final String TEXT_8 = ");";
  protected final String TEXT_9 = NL + "\t\t\tdata_";
  protected final String TEXT_10 = ".setInteger(";
  protected final String TEXT_11 = ", ";
  protected final String TEXT_12 = ".";
  protected final String TEXT_13 = ");";
  protected final String TEXT_14 = NL + "\t\t\tdata_";
  protected final String TEXT_15 = ".setLong(";
  protected final String TEXT_16 = ", ";
  protected final String TEXT_17 = ".";
  protected final String TEXT_18 = ");";
  protected final String TEXT_19 = NL + "\t\t\tdata_";
  protected final String TEXT_20 = ".setShort(";
  protected final String TEXT_21 = ", ";
  protected final String TEXT_22 = ".";
  protected final String TEXT_23 = ");";
  protected final String TEXT_24 = NL + "\t\t\tdata_";
  protected final String TEXT_25 = ".setDate(";
  protected final String TEXT_26 = ", ";
  protected final String TEXT_27 = ".";
  protected final String TEXT_28 = ");";
  protected final String TEXT_29 = NL + "\t\t\tdata_";
  protected final String TEXT_30 = ".setByte(";
  protected final String TEXT_31 = ", ";
  protected final String TEXT_32 = ".";
  protected final String TEXT_33 = ");";
  protected final String TEXT_34 = NL + "\t\t\tdata_";
  protected final String TEXT_35 = ".setDouble(";
  protected final String TEXT_36 = ", ";
  protected final String TEXT_37 = ".";
  protected final String TEXT_38 = ");";
  protected final String TEXT_39 = NL + "\t\t\tdata_";
  protected final String TEXT_40 = ".setFloat(";
  protected final String TEXT_41 = ", ";
  protected final String TEXT_42 = ".";
  protected final String TEXT_43 = ");";
  protected final String TEXT_44 = NL + "\t\t\tdata_";
  protected final String TEXT_45 = ".setBigDecimal(";
  protected final String TEXT_46 = ", ";
  protected final String TEXT_47 = ".";
  protected final String TEXT_48 = ");";
  protected final String TEXT_49 = NL + "\t\t\tdata_";
  protected final String TEXT_50 = ".setBigInteger(";
  protected final String TEXT_51 = ", ";
  protected final String TEXT_52 = ".";
  protected final String TEXT_53 = ");";
  protected final String TEXT_54 = NL + "\t\t\tdata_";
  protected final String TEXT_55 = ".setString(";
  protected final String TEXT_56 = ", String.valueOf(";
  protected final String TEXT_57 = ".";
  protected final String TEXT_58 = "));";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	List<? extends IConnection> inputConnections = node.getIncomingConnections();
	if((inputConnections == null) || (inputConnections.size() == 0)) {
		return "";
	}
	
	IConnection inputConnection = null;
	for(IConnection inputConn : inputConnections) {
		if(inputConn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
			inputConnection = inputConn;
			break;
		}
	}
	
	if(inputConnection == null) {
		return "";
	}
	
	List<IMetadataTable> metadatas = node.getMetadataList();
	if ((metadatas == null) && (metadatas.size() == 0) || (metadatas.get(0) == null)) {
		return "";
	}
	IMetadataTable metadata = metadatas.get(0);
	
	List<IMetadataColumn> columnList = metadata.getListColumns();
	if((columnList == null) || (columnList.size() == 0)) {
		return "";
	}

    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
	for(int i=0;i<columnList.size();i++) {
		IMetadataColumn column = columnList.get(i);
	    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(),column.isNullable());
	    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
	    String dbType = column.getType();
	    
		if(javaType == JavaTypesManager.STRING) {

    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_8);
    
		} else if(javaType == JavaTypesManager.INTEGER) {

    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_12);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_13);
    
		} else if(javaType == JavaTypesManager.LONG) {

    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_17);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_18);
    
		} else if(javaType == JavaTypesManager.SHORT) {

    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_23);
    
		} else if(javaType == JavaTypesManager.DATE) {

    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_28);
    
		} else if(javaType == JavaTypesManager.BYTE) {

    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_31);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_32);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_33);
    
		} else if(javaType == JavaTypesManager.DOUBLE) {

    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_35);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_36);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_38);
    
		} else if(javaType == JavaTypesManager.FLOAT) {

    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_40);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_41);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_43);
    
		} else if(javaType == JavaTypesManager.BIGDECIMAL) {

    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_45);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_46);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_48);
    
		} else if(javaType == JavaTypesManager.OBJECT && "BIG_INTEGER".equals(dbType)) {

    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_53);
    
		} else {

    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(inputConnection.getName() );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_58);
    
		}
	}

    return stringBuffer.toString();
  }
}
