package org.talend.designer.codegen.translators.talendcloud.action_deprecated_;

import java.util.List;
import org.talend.core.model.process.INode;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class TActionOutputMainJava
{
  protected static String nl;
  public static synchronized TActionOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TActionOutputMainJava result = new TActionOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t\t\t\tif (null != iPaasObject) {" + NL + "\t\t\t\t\tjava.util.Map<String, Object> map_";
  protected final String TEXT_2 = " = new java.util.HashMap<String, Object>(";
  protected final String TEXT_3 = ");";
  protected final String TEXT_4 = NL + "\t\t\t\t\t\t\tjava.util.Map<IPaasField, Object> dynamic_map_";
  protected final String TEXT_5 = " = new java.util.LinkedHashMap<IPaasField, Object>();" + NL + "\t\t\t\t\t\t\tfor (routines.system.DynamicMetadata metadata_";
  protected final String TEXT_6 = " : ";
  protected final String TEXT_7 = ".";
  protected final String TEXT_8 = ".metadatas) {" + NL + "\t\t\t\t\t\t\t\tIPaasFieldImpl ipaasField_";
  protected final String TEXT_9 = " = new IPaasFieldImpl(metadata_";
  protected final String TEXT_10 = ".getName());" + NL + "\t\t\t\t\t\t\t\tipaasField_";
  protected final String TEXT_11 = ".setType(metadata_";
  protected final String TEXT_12 = ".getType());" + NL + "\t\t\t\t\t\t\t\tipaasField_";
  protected final String TEXT_13 = ".setLength(metadata_";
  protected final String TEXT_14 = ".getLength());" + NL + "\t\t\t\t\t\t\t\tdynamic_map_";
  protected final String TEXT_15 = ".put(ipaasField_";
  protected final String TEXT_16 = ", ";
  protected final String TEXT_17 = ".";
  protected final String TEXT_18 = ".getColumnValue(metadata_";
  protected final String TEXT_19 = ".getName()));" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\ttry {" + NL + "\t\t\t\t\t\t\t\tiPaasObject.putDynamic(dynamic_map_";
  protected final String TEXT_20 = ");" + NL + "\t\t\t\t\t\t\t} catch (java.lang.NoSuchMethodError nsme_";
  protected final String TEXT_21 = ") {" + NL + "\t\t\t\t\t\t\t\tthrow new java.lang.Exception(\"Dynamic types don't supported in this version\");" + NL + "\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_22 = NL + "\t\t\t\t\t\t\tmap_";
  protected final String TEXT_23 = ".put(\"";
  protected final String TEXT_24 = "\", ";
  protected final String TEXT_25 = ".";
  protected final String TEXT_26 = ".getDocument()";
  protected final String TEXT_27 = ");" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_28 = NL + "\t\t\t\t\tiPaasObject.put(map_";
  protected final String TEXT_29 = ");" + NL + "\t\t\t\t}";
  protected final String TEXT_30 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode) codeGenArgument.getArgument();
String cid = node.getUniqueName();

for (IConnection conn : node.getIncomingConnections()) {
	if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
		List<IMetadataTable> metadatas = node.getMetadataList();
		if (null != metadatas && !metadatas.isEmpty()) {
			IMetadataTable metadata = metadatas.get(0);
			if (null != metadata) {
				List<IMetadataColumn> metadataColumns = metadata.getListColumns();

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(metadataColumns.size());
    stringBuffer.append(TEXT_3);
    					for (IMetadataColumn column : metadataColumns) {
						String name = column.getLabel();
						String type = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
						if ("Dynamic".equals(type)) { 
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(conn.getName());
    stringBuffer.append(TEXT_7);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
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
    stringBuffer.append(conn.getName());
    stringBuffer.append(TEXT_17);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
     } else { 
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(name);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(conn.getName());
    stringBuffer.append(TEXT_25);
    stringBuffer.append(name);
     if ("Document".equals(type)) { 
    stringBuffer.append(TEXT_26);
     } 
    stringBuffer.append(TEXT_27);
     } 
    					} 
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_29);
    
				break;
			}
		}
	}
}

    stringBuffer.append(TEXT_30);
    return stringBuffer.toString();
  }
}
