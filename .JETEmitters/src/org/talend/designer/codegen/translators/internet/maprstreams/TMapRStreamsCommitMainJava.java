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
import java.util.HashMap;

public class TMapRStreamsCommitMainJava
{
  protected static String nl;
  public static synchronized TMapRStreamsCommitMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMapRStreamsCommitMainJava result = new TMapRStreamsCommitMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + NL;
  protected final String TEXT_3 = NL + NL + "if (globalMap.containsKey(\"";
  protected final String TEXT_4 = "_consumer\")) {" + NL + "    ((org.apache.kafka.clients.consumer.KafkaConsumer) globalMap.get(\"";
  protected final String TEXT_5 = "_consumer\")).commitSync();" + NL + "} else {" + NL + "    throw new RuntimeException(\"Could not commit offsets : no consumer has been found from ";
  protected final String TEXT_6 = "\");" + NL + "}" + NL + NL + NL;
  protected final String TEXT_7 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    

class TMapRStreamsInputUtil {

    private INode node;
    
    private INode connNode;

    private IConnection outgoingConnection;

    private Map<String, String> consumerProperties;
    
    private boolean useExistingConn;

    public TMapRStreamsInputUtil(INode node) {
        this.node = node;
        this.connNode = Boolean.valueOf(ElementParameterParser.getValue(node, "__USE_EXISTING_CONNECTION__"))
				? ElementParameterParser.getLinkedNodeValue(node, "__CONNECTION__")
				: node;
        this.outgoingConnection = findOutgoingConnection();
        this.consumerProperties = findConsumerProperties();
    }

    public String getOutputType() {
        return ElementParameterParser.getValue(node, "__OUTPUT_TYPE__");
    }

    public String getTopic() {
        return ElementParameterParser.getValue(node, "__STREAM_TOPIC__");
    }

    public String getGroupId() {
        return ElementParameterParser.getValue(node, "__GROUP_ID__");
    }

    public boolean isResetNewConsumerGroup() {
        return "true".equals(ElementParameterParser.getValue(node, "__RESET_OFFSET__"));
    }

    public String getAutoOffsetReset() {
        return ElementParameterParser.getValue(node, "__AUTO_OFFSET_RESET__").toLowerCase();
	 }

    public boolean isAutoCommitOffset() {
        return "true".equals(ElementParameterParser.getValue(node, "__AUTO_COMMIT_OFFSET__"));
    }

    public String getAutoCommitInterval() {
        return ElementParameterParser.getValue(node, "__COMMIT_INTERVAL__");
    }

    public boolean isStopOnMaxDuration() {
        return "true".equals(ElementParameterParser.getValue(node, "__USE_BATCH_MAX_DURATION__"));
    }

    public String getMaxDuration() {
        return ElementParameterParser.getValue(node, "__BATCH_MAX_DURATION__");
    }

    public boolean isStopOnMaxSize() {
        return "true".equals(ElementParameterParser.getValue(node, "__USE_BATCH_MAX_SIZE__"));
    }

    public String getMaxSize() {
        return ElementParameterParser.getValue(node, "__BATCH_MAX_SIZE__");
    }

    public boolean isStopOnMaxMsgWait() {
        return "true".equals(ElementParameterParser.getValue(node, "__USE_BATCH_MESSAGE_TIMEOUT__"));
    }

    public String getMaxMsgWait() {
        return ElementParameterParser.getValue(node, "__BATCH_MESSAGE_TIMEOUT__");
    }

    public String getConsumerTimeout() {
        if (isStopOnMaxDuration()) {
            return getPrecision();
        } else if (isStopOnMaxMsgWait()) {
            return getMaxMsgWait();
        } else {
            return getPrecision();
        }
    }

    public String getEncoding() {
        String encoding = ElementParameterParser.getValue(node, "__ENCODING__");
        if (encoding == null || "".equals(encoding)) {
            encoding = "\"UTF-8\"";
        }
        return encoding;
   }

    public Map<String, String> getConsumerProperties() {
        return this.consumerProperties;
    }

    public String getOutStructName() {
        return this.outgoingConnection.getName() + "Struct";
    }

    public String getOutgoingConnectionName() {
        return this.outgoingConnection.getName();
    }

    public String getOutgoingColumnName() {
        return this.outgoingConnection.getMetadataTable().getListColumns().get(0).getLabel();
    }
    
    private String getPrecision() {
			String precision = ElementParameterParser.getValue(node, "__CONSUMER_TIMEOUT__");
			return "\"-1\"".equals(precision) || "-1".equals(precision) ? "1000" : precision;
    }

    private String getIntProperty(String inputMaybeNumeric) {
        if (inputMaybeNumeric.matches("-?\\d+"))
            return "\"" + inputMaybeNumeric + "\"";
        return inputMaybeNumeric;
    }

    public String getAsLong(String inputMaybeLong) {
        if (inputMaybeLong.matches("-?\\d+[lL]?"))
            return inputMaybeLong;
        if (inputMaybeLong.matches("\"-?\\d+[lL]?\""))
            return inputMaybeLong.substring(1, inputMaybeLong.length() - 1);
        return "Long.valueOf("+inputMaybeLong+")";
    }

    private Map<String, String> findConsumerProperties() {
    	java.util.Map<String, String> result = new java.util.TreeMap<String, String>();
		result.put("\"enable.auto.commit\"", "\"" + isAutoCommitOffset() + "\"");
		result.put("\"auto.commit.interval.ms\"", getIntProperty(getAutoCommitInterval()));
		result.put("\"auto.offset.reset\"", "\"" + getAutoOffsetReset() + "\"");
		result.put("\"group.id\"", getGroupId());
		fillWithCustomProps(result);
    	return result;
   }
   
   private void fillWithCustomProps(java.util.Map<String, String> result) {
   	java.util.List<java.util.Map<String, String>> customProps = (java.util.List<java.util.Map<String, String>>)
				        ElementParameterParser.getObjectValue(node, "__CONSUMER_PROPERTIES__");
		if(customProps != null) {
		    for (java.util.Map<String, String> prop : customProps) {
		        result.put(prop.get("PROPERTY"), prop.get("VALUE"));
		    }
		}
   }

    private IConnection findOutgoingConnection() {
        IConnection result = null;
        List<? extends IConnection> connections = node.getOutgoingConnections();
        for(IConnection connection : connections) {
            if(connection.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                result = connection;
                break;
            }
        }
        return result;
    }
}

    stringBuffer.append(TEXT_2);
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

INode msiNode = ElementParameterParser.getLinkedNodeValue(node, "__MAPRSTREAMS_INPUT_COMPONENT__");
String msiCid = msiNode.getUniqueName();

TMapRStreamsInputUtil tMapRStreamsInputUtil = new TMapRStreamsInputUtil(msiNode);

    stringBuffer.append(TEXT_3);
    stringBuffer.append(msiCid);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(msiCid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(msiCid);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(TEXT_7);
    return stringBuffer.toString();
  }
}
