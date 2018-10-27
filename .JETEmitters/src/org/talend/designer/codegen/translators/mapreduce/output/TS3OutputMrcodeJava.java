package org.talend.designer.codegen.translators.mapreduce.output;

import java.util.List;
import java.util.Map;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TS3OutputMrcodeJava
{
  protected static String nl;
  public static synchronized TS3OutputMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TS3OutputMrcodeJava result = new TS3OutputMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = NL + "\t";
  protected final String TEXT_2 = NL + "    \t\t\tpublic static class ";
  protected final String TEXT_3 = "StructOutputFormat extends FileOutputFormat<NullWritable, ";
  protected final String TEXT_4 = "Struct> {" + NL + "    \t\t\t" + NL + "    \t\t\t\tpublic RecordWriter<NullWritable, ";
  protected final String TEXT_5 = "Struct> getRecordWriter(" + NL + "    \t\t\t\t\t\tFileSystem ignored, JobConf job, String name, Progressable progress) throws IOException{" + NL + "                        // get the path of the temporary output file " + NL + "            \t\t\tPath file = FileOutputFormat.getTaskOutputPath(job, name);" + NL + "                    \tFileSystem fs = file.getFileSystem(job);";
  protected final String TEXT_6 = NL + "                        final SequenceFile.Writer out = new SequenceFile.Writer(fs, job, file, ";
  protected final String TEXT_7 = ".class, ";
  protected final String TEXT_8 = ".class);" + NL + "                       \t";
  protected final String TEXT_9 = NL + "    \t\t\t\t\t\tfinal SequenceFile.Writer out = SequenceFile.createWriter(fs, job, file, ";
  protected final String TEXT_10 = ".class, ";
  protected final String TEXT_11 = ".class, SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.GzipCodec());" + NL + "    \t\t\t\t\t\t";
  protected final String TEXT_12 = NL + "    \t\t\t\t\t\tfinal SequenceFile.Writer out = SequenceFile.createWriter(fs, job, file, ";
  protected final String TEXT_13 = ".class, ";
  protected final String TEXT_14 = ".class, SequenceFile.CompressionType.BLOCK, new org.apache.hadoop.io.compress.BZip2Codec());" + NL + "    \t\t\t\t\t\t";
  protected final String TEXT_15 = NL + "                        " + NL + "                        return new RecordWriter<NullWritable, ";
  protected final String TEXT_16 = "Struct>() {" + NL + "                    " + NL + "                            public void write(NullWritable key, ";
  protected final String TEXT_17 = "Struct value)" + NL + "                              throws IOException {" + NL + "                              " + NL + "                              \tout.append(new ";
  protected final String TEXT_18 = "(value.";
  protected final String TEXT_19 = "),new ";
  protected final String TEXT_20 = "(value.";
  protected final String TEXT_21 = "));" + NL + "                            }" + NL + "                    " + NL + "                            public void close(Reporter reporter) throws IOException { out.close();}" + NL + "         \t\t\t \t};" + NL + "      \t\t\t\t}" + NL + "    \t\t\t\t" + NL + "    \t\t\t}" + NL + "\t\t    ";
  protected final String TEXT_22 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_23 = "StructOutputFormat extends FileOutputFormat<NullWritable, ";
  protected final String TEXT_24 = "Struct> {" + NL + "\t\t\t\tprotected static class S3RecordWriter implements RecordWriter<NullWritable, ";
  protected final String TEXT_25 = "Struct> {" + NL + "\t\t\t\t\tprotected DataOutputStream out;" + NL + "\t\t\t\t\tprivate ContextProperties context;" + NL + "\t\t" + NL + "\t\t\t\t\tpublic S3RecordWriter(DataOutputStream out, JobConf job) {" + NL + "\t\t\t\t\t\tthis.out = out;" + NL + "\t\t\t\t\t\tthis.context = new ContextProperties(job);" + NL + "\t\t\t\t\t}" + NL + "\t\t" + NL + "\t\t\t\t\tprivate void writeObject(";
  protected final String TEXT_26 = "Struct value) throws IOException {" + NL + "\t\t\t\t\t\tStringBuilder sb = new StringBuilder();" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_27 = NL + "\t\t\t\t\t\t    \tif(value.";
  protected final String TEXT_28 = " != null){" + NL + "\t\t\t\t\t\t    ";
  protected final String TEXT_29 = NL + "\t\t\t\t\t\t\t\tsb.append(FormatterUtils.format_DateInUTC(value.";
  protected final String TEXT_30 = ", ";
  protected final String TEXT_31 = "));" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_32 = NL + "    \t\t\t\t\t\t\t\tsb.append(FormatterUtils.format_Number(";
  protected final String TEXT_33 = ".toPlainString(), ";
  protected final String TEXT_34 = ", ";
  protected final String TEXT_35 = "));\t\t\t\t\t" + NL + "    \t\t\t\t\t\t\t";
  protected final String TEXT_36 = NL + "    \t\t\t\t\t\t\t\tsb.append(FormatterUtils.format_Number(new java.math.BigDecimal(String.valueOf(value.";
  protected final String TEXT_37 = ")).toPlainString(), ";
  protected final String TEXT_38 = ", ";
  protected final String TEXT_39 = "));\t\t\t\t\t\t" + NL + "    \t\t\t\t\t\t\t";
  protected final String TEXT_40 = NL + "    \t\t\t\t\t\t\tsb.append(";
  protected final String TEXT_41 = ".toPlainString());" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_42 = NL + "    \t\t\t\t\t\t\tsb.append(java.nio.charset.Charset.forName(";
  protected final String TEXT_43 = ").decode(java.nio.ByteBuffer.wrap(value.";
  protected final String TEXT_44 = ")).toString());" + NL + "    \t\t\t\t\t\t";
  protected final String TEXT_45 = NL + "    \t\t\t\t\t\t\tsb.append(value.";
  protected final String TEXT_46 = ");" + NL + "    \t\t\t\t\t\t";
  protected final String TEXT_47 = NL + "\t\t\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_48 = NL + "\t\t\t\t\t\t\t\tsb.append(";
  protected final String TEXT_49 = ");" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_50 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_51 = NL + "\t\t\t\t\t\tthis.out.write(sb.toString().getBytes(";
  protected final String TEXT_52 = "));" + NL + "\t\t\t\t\t}" + NL + "\t\t" + NL + "\t\t\t\t\tpublic synchronized void write(NullWritable key, ";
  protected final String TEXT_53 = "Struct value)" + NL + "\t\t\t\t\t\t\tthrows IOException {" + NL + "\t\t" + NL + "\t\t\t\t\t\tboolean nullValue = value == null;" + NL + "\t\t\t\t\t\tif (nullValue) {" + NL + "\t\t\t\t\t\t\treturn;" + NL + "\t\t\t\t\t\t} else {" + NL + "\t\t\t\t\t\t\twriteObject(value);" + NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t\tout.write(";
  protected final String TEXT_54 = ".getBytes(";
  protected final String TEXT_55 = "));" + NL + "\t\t\t\t\t}" + NL + "\t\t" + NL + "\t\t\t\t\tpublic synchronized void close(Reporter reporter) throws IOException {" + NL + "\t\t\t\t\t\tout.close();" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t}" + NL + "\t\t" + NL + "\t\t\t\tpublic RecordWriter<NullWritable, ";
  protected final String TEXT_56 = "Struct> getRecordWriter(" + NL + "\t\t\t\t\t\tFileSystem ignored, JobConf job, String name, Progressable progress) throws IOException{" + NL + "" + NL + "\t\t\t\t\t";
  protected final String TEXT_57 = NL + "\t\t\t\t\t\tPath file = FileOutputFormat.getTaskOutputPath(job, name);" + NL + "\t\t\t\t\t\tFileSystem fs = file.getFileSystem(job);" + NL + "\t\t\t\t\t\tDataOutputStream fileOut = fs.create(file, progress);" + NL + "\t\t\t\t\t";
  protected final String TEXT_58 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_59 = NL + "\t\t\t\t\t\tCompressionCodec codec = ReflectionUtils.newInstance(org.apache.hadoop.io.compress.GzipCodec.class," + NL + "\t\t\t\t\t\t\t\tjob);" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_60 = NL + "\t\t\t\t\t\tCompressionCodec codec = ReflectionUtils.newInstance(org.apache.hadoop.io.compress.BZip2Codec.class," + NL + "\t\t\t\t\t\t\t\tjob);" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_61 = NL + "\t\t\t\t\t\t// build the filename including the extension" + NL + "\t\t\t\t\t\tPath file = FileOutputFormat.getTaskOutputPath(job," + NL + "\t\t\t\t\t\t\t\tname + codec.getDefaultExtension());" + NL + "\t\t\t\t\t\tFileSystem fs = file.getFileSystem(job);" + NL + "\t\t\t\t\t\tDataOutputStream fileOut = new DataOutputStream(" + NL + "\t\t\t\t\t\t\t\tcodec.createOutputStream(fs.create(file, progress)));" + NL + "\t\t\t\t\t";
  protected final String TEXT_62 = NL + "\t\t\t\t\t";
  protected final String TEXT_63 = NL + "\t\t\t\t\t\tfileOut.write(\"";
  protected final String TEXT_64 = "\".getBytes(";
  protected final String TEXT_65 = "));" + NL + "\t\t\t\t\t\tfileOut.write(";
  protected final String TEXT_66 = ".getBytes(";
  protected final String TEXT_67 = "));" + NL + "\t\t\t\t\t";
  protected final String TEXT_68 = NL + "\t\t\t\t\treturn new S3RecordWriter(fileOut, job);" + NL + "\t\t\t\t}" + NL + "\t\t\t}" + NL + "\t\t";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();  
    String cid = node.getUniqueName();
    
    List<IMetadataTable> metadatas = node.getMetadataList();
    if(metadatas != null && metadatas.size() > 0) {
        IMetadataTable metadata = metadatas.get(0);
        if(metadata != null){
        
			String rowSeparator = ElementParameterParser.getValue(node,"__ROWSEPARATOR__");
			String fieldSeparator = ElementParameterParser.getValue(node,"__FIELDSEPARATOR__");
			boolean includeHeader = "true".equals(ElementParameterParser.getValue(node, "__INCLUDEHEADER__"));
			boolean customEncoding = "true".equals(ElementParameterParser.getValue(node, "__CUSTOM_ENCODING__"));
			String encoding = ElementParameterParser.getValue(node,"__ENCODING__");
			
			String advancedSeparatorStr = ElementParameterParser.getValue(node, "__ADVANCED_SEPARATOR__");
    		boolean advancedSeparator = (advancedSeparatorStr!=null&&!("").equals(advancedSeparatorStr))?("true").equals(advancedSeparatorStr):false;
    		String thousandsSeparator = ElementParameterParser.getValueWithJavaType(node, "__THOUSANDS_SEPARATOR__", JavaTypesManager.CHARACTER);
    		String decimalSeparator = ElementParameterParser.getValueWithJavaType(node, "__DECIMAL_SEPARATOR__", JavaTypesManager.CHARACTER);
			
			boolean compress = "true".equals(ElementParameterParser.getValue(node, "__COMPRESS__"));
			String compression = ElementParameterParser.getValue(node, "__COMPRESSION__");
			// option __MERGE_RESULT__ removed.
			boolean merge = false;
			String typeFile = ElementParameterParser.getValue(node,"__TYPEFILE__");
			
	  		String connName = "";
	        
		    List<? extends IConnection> inConns = node.getIncomingConnections(EConnectionType.FLOW_MAIN);
		    if(inConns != null && inConns.size() > 0){
		    	IConnection inConn = inConns.get(0); 
		        connName = inConn.getName();
		    }else{
		        return "";
		    }
    
		    List<IMetadataColumn> columns = metadata.getListColumns();
		    int columnSize = columns.size();
		    
		    if ("SEQUENCE".equals(typeFile)) {
    			String keyColumn = ElementParameterParser.getValue(node,"__KEYCOLUMN__");
    			String valueColumn = ElementParameterParser.getValue(node,"__VALUECOLUMN__");
    			
    			String talendKeyClass = "";
    			String talendValueClass = "";
    			
    			for ( int i = 0; i < columnSize; i++ ){
    				IMetadataColumn column = columns.get(i);
    				if (column.getLabel().equals(keyColumn)) {
    					talendKeyClass = column.getTalendType();
    				}
    				if (column.getLabel().equals(valueColumn)) {
    					talendValueClass = column.getTalendType();
    				}
    			}
    			
    			String keyClass="org.apache.hadoop.io.Text";
    			if (talendKeyClass.equals("id_Boolean")) keyClass="org.apache.hadoop.io.BooleanWritable";
    			if (talendKeyClass.equals("id_Byte")) keyClass="org.apache.hadoop.io.ByteWritable";
    			if (talendKeyClass.equals("id_byte[]")) keyClass="org.apache.hadoop.io.BytesWritable";
    			if (talendKeyClass.equals("id_Double")) keyClass="org.apache.hadoop.io.DoubleWritable";
    			if (talendKeyClass.equals("id_Float")) keyClass="org.apache.hadoop.io.FloatWritable";
    			if (talendKeyClass.equals("id_Integer")) keyClass="org.apache.hadoop.io.IntWritable";
    			if (talendKeyClass.equals("id_Long")) keyClass="org.apache.hadoop.io.LongWritable";
    			if (talendKeyClass.equals("id_Short")) keyClass="org.apache.hadoop.io.IntWritable";
    			if (talendKeyClass.equals("id_String")) keyClass="org.apache.hadoop.io.Text";
    			
    			String valueClass="org.apache.hadoop.io.Text";
    			if (talendValueClass.equals("id_Boolean")) valueClass="org.apache.hadoop.io.BooleanWritable";
    			if (talendValueClass.equals("id_Byte")) valueClass="org.apache.hadoop.io.ByteWritable";
    			if (talendValueClass.equals("id_byte[]")) valueClass="org.apache.hadoop.io.BytesWritable";
    			if (talendValueClass.equals("id_Double")) valueClass="org.apache.hadoop.io.DoubleWritable";
    			if (talendValueClass.equals("id_Float")) valueClass="org.apache.hadoop.io.FloatWritable";
    			if (talendValueClass.equals("id_Integer")) valueClass="org.apache.hadoop.io.IntWritable";
    			if (talendValueClass.equals("id_Long")) valueClass="org.apache.hadoop.io.LongWritable";
    			if (talendValueClass.equals("id_Short")) valueClass="org.apache.hadoop.io.IntWritable";
    			if (talendValueClass.equals("id_String")) valueClass="org.apache.hadoop.io.Text";
			
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_5);
    if(!compress || merge){
    stringBuffer.append(TEXT_6);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_8);
    } else {
    						if("GZIP".equals(compression)) {
    						
    stringBuffer.append(TEXT_9);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_11);
    
    						} else if("BZIP2".equals(compression)){
    						
    stringBuffer.append(TEXT_12);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_13);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_14);
    
    						}
    					}
    stringBuffer.append(TEXT_15);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(keyClass );
    stringBuffer.append(TEXT_18);
    stringBuffer.append(keyColumn );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(valueClass );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(valueColumn );
    stringBuffer.append(TEXT_21);
    
				return stringBuffer.toString();
		    }
			
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_24);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_25);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_26);
    
						for (int i = 0; i < columnSize; i++) {
						    IMetadataColumn column = columns.get(i);
						    String columnName = column.getLabel();
						    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
						    String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
						    boolean isPrimitive = JavaTypesManager.isJavaPrimitiveType(javaType, column.isNullable());
						    if(!isPrimitive){
						    
    stringBuffer.append(TEXT_27);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_28);
    
						    }
						    if(javaType == JavaTypesManager.DATE && pattern != null && pattern.trim().length() != 0){
						    
    stringBuffer.append(TEXT_29);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_30);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_31);
    
							}else if(advancedSeparator && JavaTypesManager.isNumberType(javaType, column.isNullable())){ 
								if(javaType == JavaTypesManager.BIGDECIMAL){
    stringBuffer.append(TEXT_32);
    stringBuffer.append(column.getPrecision() == null? "value." + columnName : "value." + columnName + ".setScale(" + column.getPrecision() + ", java.math.RoundingMode.HALF_UP)" );
    stringBuffer.append(TEXT_33);
    stringBuffer.append( thousandsSeparator );
    stringBuffer.append(TEXT_34);
    stringBuffer.append( decimalSeparator );
    stringBuffer.append(TEXT_35);
    }else{
    stringBuffer.append(TEXT_36);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_37);
    stringBuffer.append( thousandsSeparator );
    stringBuffer.append(TEXT_38);
    stringBuffer.append( decimalSeparator );
    stringBuffer.append(TEXT_39);
    }
							} else if(javaType == JavaTypesManager.BIGDECIMAL){
    						
    stringBuffer.append(TEXT_40);
    stringBuffer.append(column.getPrecision() == null ? "value." + columnName : "value." + columnName + ".setScale(" + column.getPrecision() + ", java.math.RoundingMode.HALF_UP)" );
    stringBuffer.append(TEXT_41);
    
							} else if(javaType == JavaTypesManager.BYTE_ARRAY){
    						
    stringBuffer.append(TEXT_42);
    stringBuffer.append(customEncoding?encoding:"utf8Charset" );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_44);
    
    			        	} else {
    			            
    stringBuffer.append(TEXT_45);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_46);
    				
    			        	}
							if(!isPrimitive){
							
    stringBuffer.append(TEXT_47);
    
							}
							if(i < columnSize-1){
    stringBuffer.append(TEXT_48);
    stringBuffer.append(fieldSeparator);
    stringBuffer.append(TEXT_49);
    }
    stringBuffer.append(TEXT_50);
    
						}
						
    stringBuffer.append(TEXT_51);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_52);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(rowSeparator);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_56);
    if(!compress || merge){
    stringBuffer.append(TEXT_57);
    }else{
    stringBuffer.append(TEXT_58);
    
						if("GZIP".equals(compression)){
						
    stringBuffer.append(TEXT_59);
    
						}else if("BZIP2".equals(compression)){
						
    stringBuffer.append(TEXT_60);
    
						}
						
    stringBuffer.append(TEXT_61);
    }
    stringBuffer.append(TEXT_62);
    
					if(includeHeader){
						String header = "";
						int i = 1;
						for(IMetadataColumn column : columns){
							header += column.getLabel() + (i!=columns.size() ? fieldSeparator.substring(1,fieldSeparator.length()-1) : "");
							i++;
						}
						
    stringBuffer.append(TEXT_63);
    stringBuffer.append(header);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_65);
    stringBuffer.append(rowSeparator);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(customEncoding?encoding:"" );
    stringBuffer.append(TEXT_67);
    }
    stringBuffer.append(TEXT_68);
    
    	}
	}   
	
    return stringBuffer.toString();
  }
}
