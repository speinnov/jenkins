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

public class TMapRStreamsOutputMainJava
{
  protected static String nl;
  public static synchronized TMapRStreamsOutputMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMapRStreamsOutputMainJava result = new TMapRStreamsOutputMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL;
  protected final String TEXT_3 = NL;
  protected final String TEXT_4 = NL;
  protected final String TEXT_5 = NL;
  protected final String TEXT_6 = "_producer.send(new org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]>(";
  protected final String TEXT_7 = ", ";
  protected final String TEXT_8 = ".";
  protected final String TEXT_9 = "));" + NL + NL;
  protected final String TEXT_10 = NL;

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
final TMapRStreamsOutputUtil tMapRStreamsOutputUtil = new TMapRStreamsOutputUtil(node);

final String cid = node.getUniqueName();

    stringBuffer.append(TEXT_4);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(tMapRStreamsOutputUtil.getTopic());
    stringBuffer.append(TEXT_7);
    stringBuffer.append(tMapRStreamsOutputUtil.getIncomingConnection().getName());
    stringBuffer.append(TEXT_8);
    stringBuffer.append(tMapRStreamsOutputUtil.getIncomingColumnName());
    stringBuffer.append(TEXT_9);
    stringBuffer.append(TEXT_10);
    return stringBuffer.toString();
  }
}
