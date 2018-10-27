package org.talend.designer.codegen.translators.internet.maprstreams;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

public class TMapRStreamsOutputBeginJava
{
  protected static String nl;
  public static synchronized TMapRStreamsOutputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMapRStreamsOutputBeginJava result = new TMapRStreamsOutputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL;
  protected final String TEXT_4 = NL + NL + "// Producer configuration" + NL + "java.util.Properties ";
  protected final String TEXT_5 = "_producerProperties = new java.util.Properties();";
  protected final String TEXT_6 = NL;
  protected final String TEXT_7 = "_producerProperties.setProperty(\"compression.type\", \"";
  protected final String TEXT_8 = "\");";
  protected final String TEXT_9 = NL + "\t\t";
  protected final String TEXT_10 = "_producerProperties.setProperty(";
  protected final String TEXT_11 = ", ";
  protected final String TEXT_12 = ");";
  protected final String TEXT_13 = NL + NL + "org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> ";
  protected final String TEXT_14 = "_producer = (org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]>)globalMap.get(\"";
  protected final String TEXT_15 = "_producer\");" + NL + "if(null == ";
  protected final String TEXT_16 = "_producer) {";
  protected final String TEXT_17 = NL + "    ";
  protected final String TEXT_18 = "_producer = new org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]>(";
  protected final String TEXT_19 = "_producerProperties, new org.apache.kafka.common.serialization.ByteArraySerializer(), new org.apache.kafka.common.serialization.ByteArraySerializer());" + NL + "    globalMap.put(\"";
  protected final String TEXT_20 = "_producer\", ";
  protected final String TEXT_21 = "_producer);" + NL + "}";
  protected final String TEXT_22 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
class TMapRStreamsOutputUtil {
	
	 private INode node;
	 
	 private INode connNode;

    private IConnection incomingConnection;

    private Map<String, String> producerProperties;
    
    public TMapRStreamsOutputUtil(INode node) {
        this.node = node;
        this.connNode = Boolean.valueOf(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"))
				? ElementParameterParser.getLinkedNodeValue(node, "__CONNECTION__")
				: node;
        this.incomingConnection = findIncomingConnection();
        this.producerProperties = findProducerProperties();
    }
    
    public String getInStructName() {
		return this.incomingConnection.getName() + "Struct";
	 }

    public String getEncoding() {
        return ElementParameterParser.getValue(node, "__ENCODING__");
    }

    public String getCompression() {
        String compression = "none";
        if ("true".equals(ElementParameterParser.getValue(node, "__COMPRESS__"))) {
            compression = ElementParameterParser.getValue(node, "__COMPRESSION__").toLowerCase();
        }
        return compression;
    }

    public IConnection getIncomingConnection() {
        return this.incomingConnection;
    }

    public Map<String, String> getProducerProperties() {
        return this.producerProperties;
    }

    public String getTopic() {
        return ElementParameterParser.getValue(node, "__STREAM_TOPIC__");
    }

    public String getIncomingColumnName() {
        return this.incomingConnection.getMetadataTable().getListColumns().get(0).getLabel();
    }
    
    private IConnection findIncomingConnection() {
        IConnection result = null;
        if (node.getIncomingConnections() != null) {
            for (IConnection in : node.getIncomingConnections()) {
                if (in.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                    result = in;
                    break;
                }
            }
        }
        return result;
    }

    private Map<String, String> findProducerProperties() {
        Map<String, String> result = new HashMap<String, String>();
        List<Map<String, String>> producerPropertiesFromStudio = (List<Map<String, String>>) ElementParameterParser
                .getObjectValue(node, "__PRODUCER_PROPERTIES__");
        if(producerPropertiesFromStudio != null) {
	        for (Map<String, String> producerPropertyFromStudio : producerPropertiesFromStudio) {
	            result.put(producerPropertyFromStudio.get("PROPERTY"), producerPropertyFromStudio.get("VALUE"));
	        }
        }
        return result;
    }
}

    stringBuffer.append(TEXT_3);
    
// Parse the inputs to this javajet generator.
final CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
final INode node = (INode)codeGenArgument.getArgument();
final String cid = node.getUniqueName();

TMapRStreamsOutputUtil tMapRStreamsOutputUtil = new TMapRStreamsOutputUtil(node);

    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(tMapRStreamsOutputUtil.getCompression());
    stringBuffer.append(TEXT_8);
    
	// Advanced configuration
	for(Entry<String, String> producerProperty : tMapRStreamsOutputUtil.getProducerProperties().entrySet()) {

    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(producerProperty.getKey());
    stringBuffer.append(TEXT_11);
    stringBuffer.append(producerProperty.getValue());
    stringBuffer.append(TEXT_12);
    
	} // end for

    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(TEXT_22);
    return stringBuffer.toString();
  }
}
