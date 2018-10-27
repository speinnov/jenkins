package org.talend.designer.codegen.translators.mapreduce.input;

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
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TInputFormatAvroSparkcodeJava
{
  protected static String nl;
  public static synchronized TInputFormatAvroSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TInputFormatAvroSparkcodeJava result = new TInputFormatAvroSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + NL + "//Use this object to override the ouput struct default ";
  protected final String TEXT_3 = NL + "//public static class ";
  protected final String TEXT_4 = " extends org.apache.hadoop.io.ObjectWritable {" + NL + "//}" + NL + "" + NL + "public static class ";
  protected final String TEXT_5 = "StructInputFormat" + NL + "    extends FileInputFormat<NullWritable, ";
  protected final String TEXT_6 = "> {" + NL + "    private ContextProperties context;" + NL + "" + NL + "    public final static String EXT = \".avro\";" + NL + "" + NL + "    protected org.apache.hadoop.fs.FileStatus[] listStatus(JobConf conf) throws IOException {" + NL + "        List<org.apache.hadoop.fs.FileStatus> result = new java.util.ArrayList<org.apache.hadoop.fs.FileStatus>();" + NL + "        for (org.apache.hadoop.fs.FileStatus file : super.listStatus(conf)) {" + NL + "            if (file.getPath().getName().endsWith(EXT)) {" + NL + "                result.add(file);" + NL + "            }" + NL + "        }" + NL + "        return result.toArray(new org.apache.hadoop.fs.FileStatus[0]);" + NL + "    }" + NL + "" + NL + "    public InputSplit[] getSplits(JobConf job, int numSplits)" + NL + "            throws IOException {" + NL + "        context = new ContextProperties(job);" + NL + "        job.set(\"mapred.input.dir\", ";
  protected final String TEXT_7 = ");" + NL + "        return super.getSplits(job, numSplits);" + NL + "    }" + NL + "" + NL + "    public RecordReader<NullWritable, ";
  protected final String TEXT_8 = "> getRecordReader(" + NL + "            InputSplit split, JobConf job, Reporter reporter)" + NL + "            throws IOException {" + NL + "        reporter.setStatus(split.toString());" + NL + "        return new AvroRecordReader(job, (FileSplit) split);" + NL + "    }" + NL + "" + NL + "    protected static class AvroRecordReader implements" + NL + "        RecordReader<NullWritable, ";
  protected final String TEXT_9 = "> {" + NL + "" + NL + "        private ContextProperties context;" + NL + "" + NL + "        private org.apache.avro.file.FileReader<org.apache.avro.generic.GenericRecord> reader;" + NL + "        private long start;" + NL + "        private long end;" + NL + "" + NL + "        org.apache.avro.generic.GenericRecord record = null;" + NL + "" + NL + "        protected AvroRecordReader(JobConf job, FileSplit split)" + NL + "                throws IOException {" + NL + "            this.context = new ContextProperties(job);" + NL + "" + NL + "            //create file reader" + NL + "            org.apache.avro.io.DatumReader<org.apache.avro.generic.GenericRecord> datumReader = new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>();" + NL + "            reader = org.apache.avro.file.DataFileReader.openReader(new org.apache.avro.mapred.FsInput(split.getPath(),job),datumReader);" + NL + "" + NL + "            // get the schema" + NL + "            org.apache.avro.Schema schema = reader.getSchema();" + NL + "" + NL + "            //sync to start" + NL + "            reader.sync(split.getStart());" + NL + "            this.start = reader.tell();" + NL + "            this.end = split.getStart() + split.getLength();" + NL + "" + NL + "            record = new org.apache.avro.generic.GenericData.Record(schema);" + NL + "        }" + NL + "" + NL + "        public boolean next(NullWritable key, ";
  protected final String TEXT_10 = " value) throws IOException {" + NL + "            if (!reader.hasNext() || reader.pastSync(end)) {" + NL + "                return false;" + NL + "            }" + NL + "            value.set(reader.next(record));" + NL + "            return true;" + NL + "        }" + NL + "" + NL + "        public NullWritable createKey() {" + NL + "            return NullWritable.get();" + NL + "        }" + NL + "" + NL + "        public ";
  protected final String TEXT_11 = " createValue() {" + NL + "            return new ";
  protected final String TEXT_12 = "();" + NL + "        }" + NL + "" + NL + "        public long getPos() throws IOException {" + NL + "            return reader.tell();" + NL + "        }" + NL + "" + NL + "        public void close() throws IOException {" + NL + "            if (reader != null) {" + NL + "                reader.close();" + NL + "            }" + NL + "        }" + NL + "" + NL + "        public float getProgress() throws IOException {" + NL + "            if (start == end) {" + NL + "                return 0.0f;" + NL + "            } else {" + NL + "                return Math" + NL + "                        .min(1.0f, (getPos() - start) / (float) (end - start));" + NL + "            }" + NL + "        }" + NL + "    }" + NL + "}";
  protected final String TEXT_13 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
final INode node = (INode)codeGenArgument.getArgument();
final String cid = node.getUniqueName();

    stringBuffer.append(TEXT_1);
    
String folder = ElementParameterParser.getValue(node,"__FILENAME__");

String uriPrefix = "";
// Used for Spark only for now.
boolean useConfigurationComponent = "true".equals(ElementParameterParser.getValue(node, "__DEFINE_STORAGE_CONFIGURATION__"));
if (useConfigurationComponent) {
    uriPrefix = org.talend.designer.spark.generator.storage.SparkStorageUtils.getURIPrefix(node);
    folder = uriPrefix + " + " + folder;
}

List<IMetadataTable> metadatas = node.getMetadataList();
List<? extends IConnection> connections = node.getOutgoingConnections();
if (connections == null || connections.size() == 0 || metadatas == null || metadatas.size() == 0)
    return "";

String connectionName = connections.get(0).getName();
List<IMetadataColumn> columns = metadatas.get(0).getListColumns();
int nbColumns = columns.size();

// Don't use the auto-generated Avro record struct with the outgoing connection.
// It will be embedded in the job code.
//String avroRecordStruct = connectionName + "_ObjectWritableStruct";
String avroRecordStruct = "org.apache.hadoop.io.ObjectWritable";
codeGenArgument.getRecordStructGenerator().reserveRecordStructName(connections.get(0), avroRecordStruct);

    stringBuffer.append(TEXT_2);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(connectionName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(folder);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(avroRecordStruct);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(TEXT_13);
    return stringBuffer.toString();
  }
}
