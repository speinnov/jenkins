package org.talend.designer.codegen.translators.databases.oracle;

import java.util.List;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.designer.common.BigDataCodeGeneratorArgument;

public class TOracleInputMrcodeJava
{
  protected static String nl;
  public static synchronized TOracleInputMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TOracleInputMrcodeJava result = new TOracleInputMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "            {" + NL + "                throw new Exception(\"";
  protected final String TEXT_2 = " must contain at least one Main output connection.\");" + NL + "            }";
  protected final String TEXT_3 = NL + NL + "\tpublic static class ";
  protected final String TEXT_4 = "StructInputFormat implements InputFormat<NullWritable, ";
  protected final String TEXT_5 = "Struct>, JobConfigurable {" + NL + "" + NL + "\t    private java.sql.Connection connection;" + NL + "\t\tContextProperties context;" + NL + "" + NL + "\t\tpublic void configure(JobConf job) {" + NL + "\t\t\tcontext = new ContextProperties(job);" + NL + "" + NL + "\t\t\ttry {" + NL + "\t\t\t\tClass.forName(";
  protected final String TEXT_6 = ");" + NL;
  protected final String TEXT_7 = NL;
  protected final String TEXT_8 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_9 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_10 = ");";
  protected final String TEXT_11 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_12 = " = ";
  protected final String TEXT_13 = "; ";
  protected final String TEXT_14 = NL + NL + "\t\t        this.connection = java.sql.DriverManager.getConnection(";
  protected final String TEXT_15 = ", ";
  protected final String TEXT_16 = ", decryptedPassword_";
  protected final String TEXT_17 = ");" + NL + "\t\t        this.connection.setAutoCommit(false);" + NL + "\t\t\t\tthis.connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);" + NL + "\t\t    } catch (Exception ex) {" + NL + "\t\t        throw new RuntimeException(ex);" + NL + "\t\t    }" + NL + "\t    }" + NL + "" + NL + "\t\tpublic RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {" + NL + "\t\t\ttry {" + NL + "\t\t  \t\treturn new DBRecordReader(connection, (new org.talend.hadoop.mapred.lib.db.JDBCHelper(connection)).getDBSql(split,";
  protected final String TEXT_18 = ",\"";
  protected final String TEXT_19 = "\"), split.getLength());" + NL + "\t\t  \t} catch (SQLException ex) {" + NL + "\t            throw new IOException(ex.getMessage());" + NL + "\t        }" + NL + "\t\t}" + NL + "\t\tpublic InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {" + NL + "            try {" + NL + "\t            java.sql.Statement statement = connection.createStatement();" + NL + "\t" + NL + "\t            java.sql.ResultSet results = statement.executeQuery(";
  protected final String TEXT_20 = ");" + NL + "\t            results.next();" + NL + "\t" + NL + "\t            long count = results.getLong(1);" + NL + "\t            long chunkSize = (count / numSplits);" + NL + "\t" + NL + "\t            results.close();" + NL + "\t            statement.close();" + NL + "\t" + NL + "\t            InputSplit[] splits = new InputSplit[numSplits];" + NL + "\t" + NL + "\t            // Split the rows into n-number of numSplits and adjust the last chunk" + NL + "\t            // accordingly" + NL + "\t            for (int i = 0; i < numSplits; i++) {" + NL + "\t                org.talend.hadoop.mapred.lib.db.DBTableSplit split;" + NL + "\t" + NL + "\t                if ((i + 1) == numSplits)" + NL + "\t                    split = new org.talend.hadoop.mapred.lib.db.DBTableSplit(i * chunkSize, count);" + NL + "\t                else" + NL + "\t                    split = new org.talend.hadoop.mapred.lib.db.DBTableSplit(i * chunkSize, (i * chunkSize) + chunkSize);" + NL + "\t" + NL + "\t                splits[i] = split;" + NL + "\t            }" + NL + "\t" + NL + "\t            return splits;" + NL + "\t        } catch (java.sql.SQLException e) {" + NL + "\t            throw new IOException(e.getMessage());" + NL + "\t        }" + NL + "        }" + NL + "" + NL + "\t\tprotected static class DBRecordReader implements RecordReader<NullWritable, ";
  protected final String TEXT_21 = "Struct> {" + NL + "\t        private java.sql.ResultSet results;" + NL + "" + NL + "\t        private java.sql.Statement statement;" + NL + "" + NL + "\t        private long pos = 0;" + NL + "" + NL + "\t        private long splitLength = 0;" + NL + "\t        /**" + NL + "\t         * @param split The InputSplit to read data for" + NL + "\t         * @throws SQLException" + NL + "\t         */" + NL + "\t        protected DBRecordReader(java.sql.Connection connection, String query, long splitLength) throws SQLException {" + NL + "\t            statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);" + NL + "\t            results = statement.executeQuery(query);" + NL + "\t            this.splitLength = splitLength; " + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public void close() throws IOException {" + NL + "\t            try {" + NL + "\t                results.close();" + NL + "\t                statement.close();" + NL + "\t            } catch (SQLException e) {" + NL + "\t                throw new IOException(e.getMessage());" + NL + "\t            }" + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public NullWritable createKey() {" + NL + "\t            return NullWritable.get();" + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public ";
  protected final String TEXT_22 = "Struct createValue() {" + NL + "\t            return new ";
  protected final String TEXT_23 = "Struct();" + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public long getPos() throws IOException {" + NL + "\t            return pos;" + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public float getProgress() throws IOException {" + NL + "\t            return pos / (float)splitLength;" + NL + "\t        }" + NL + "\t" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public boolean next(NullWritable key, ";
  protected final String TEXT_24 = "Struct value) throws IOException{" + NL + "\t            try{" + NL + "\t                if(!results.next())" + NL + "\t                    return false;" + NL + "\t" + NL + "\t                ";
  protected final String TEXT_25 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_26 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_27 = " = results.getBoolean(";
  protected final String TEXT_28 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_29 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_30 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_31 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_32 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_33 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_34 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_35 = " = results.getByte(";
  protected final String TEXT_36 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_37 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_38 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_39 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_40 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_41 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_42 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_43 = " = results.getBytes(";
  protected final String TEXT_44 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_45 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_46 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_47 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_48 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_49 = NL + "\t\t                \tif(results.getString(";
  protected final String TEXT_50 = ") != null && results.getString(";
  protected final String TEXT_51 = ").length() > 0){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_52 = " = results.getString(";
  protected final String TEXT_53 = ").charAt(0);" + NL + "\t\t                    }else{" + NL + "\t\t                    \tif(results.getString(";
  protected final String TEXT_54 = ") == null){" + NL + "\t\t                    \t\t";
  protected final String TEXT_55 = NL + "\t\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_56 = " = null;" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_57 = NL + "\t\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t\t";
  protected final String TEXT_58 = NL + "\t\t                    \t}else{" + NL + "\t\t                    \t\tvalue.";
  protected final String TEXT_59 = " = '\\0';" + NL + "\t\t                    \t}" + NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_60 = NL + "\t\t                \ttry{" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_61 = " = results.getTimestamp(";
  protected final String TEXT_62 = ");" + NL + "\t\t                    }catch(java.lang.Exception e){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_63 = " = results.getDate(";
  protected final String TEXT_64 = ");" + NL + "\t\t                    }" + NL + "\t\t                    " + NL + "\t\t                ";
  protected final String TEXT_65 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_66 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_67 = " = results.getDouble(";
  protected final String TEXT_68 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_69 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_70 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_71 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_72 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_73 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_74 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_75 = " = results.getFloat(";
  protected final String TEXT_76 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_77 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_78 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_79 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_80 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_81 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_82 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_83 = " = results.getBigDecimal(";
  protected final String TEXT_84 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_85 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_86 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_87 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_88 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_89 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_90 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_91 = " = results.getInt(";
  protected final String TEXT_92 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_93 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_94 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_95 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_96 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_97 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_98 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_99 = " = results.getLong(";
  protected final String TEXT_100 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_101 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_102 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_103 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_104 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_105 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_106 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_107 = " = results.getObject(";
  protected final String TEXT_108 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_109 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_110 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_111 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_112 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_113 = NL + "\t\t                \tif(results.getObject(";
  protected final String TEXT_114 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_115 = " = results.getShort(";
  protected final String TEXT_116 = ");" + NL + "\t\t                    }else{" + NL + "\t\t                    \t";
  protected final String TEXT_117 = NL + "\t\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_118 = " = null;" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_119 = NL + "\t\t\t\t\t\t\t\t\tthrow new RuntimeException(\"Null value in non-Nullable column\");" + NL + "\t\t\t\t\t\t\t\t";
  protected final String TEXT_120 = NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_121 = NL + "\t\t                \tif(results.getString(";
  protected final String TEXT_122 = ") != null){" + NL + "\t\t                    \tvalue.";
  protected final String TEXT_123 = " = results.getString(";
  protected final String TEXT_124 = ");" + NL + "\t\t                    }else{" + NL + "\t\t\t\t\t\t\t\tvalue.";
  protected final String TEXT_125 = " = null;" + NL + "\t\t                    }" + NL + "\t\t                ";
  protected final String TEXT_126 = NL + "\t                    \tvalue.";
  protected final String TEXT_127 = " = (java.util.List)results.getObject(";
  protected final String TEXT_128 = ");" + NL + "\t\t                ";
  protected final String TEXT_129 = NL + "\t                pos++;" + NL + "\t            } catch (SQLException e) {" + NL + "\t                ";
  protected final String TEXT_130 = NL + "                        throw new IOException(e.getMessage());";
  protected final String TEXT_131 = NL + "\t                    System.err.println(e.getMessage());" + NL + "\t                    return next(key, value);" + NL + "\t                    ";
  protected final String TEXT_132 = NL + "\t            }" + NL + "\t            return true;" + NL + "\t        }" + NL + "\t    }" + NL + "\t}";
  protected final String TEXT_133 = "    ";
  protected final String TEXT_134 = NL + NL + "\tpublic static class ";
  protected final String TEXT_135 = "TemporaryStruct extends org.apache.hadoop.io.ObjectWritable {" + NL + "\t}" + NL + "" + NL + "\tpublic static class ";
  protected final String TEXT_136 = "StructInputFormat implements InputFormat<NullWritable, ";
  protected final String TEXT_137 = "TemporaryStruct>, JobConfigurable {" + NL + "" + NL + "\t\tprivate java.sql.Connection connection;" + NL + "" + NL + "\t\tContextProperties context;" + NL + "" + NL + "\t\tpublic void configure(JobConf job) {" + NL + "\t\t\tcontext = new ContextProperties(job);" + NL + "\t\t\ttry {" + NL + "\t\t\t\tClass.forName(";
  protected final String TEXT_138 = ");" + NL;
  protected final String TEXT_139 = NL;
  protected final String TEXT_140 = " " + NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_141 = " = routines.system.PasswordEncryptUtil.decryptPassword(";
  protected final String TEXT_142 = ");";
  protected final String TEXT_143 = NL + "\tfinal String decryptedPassword_";
  protected final String TEXT_144 = " = ";
  protected final String TEXT_145 = "; ";
  protected final String TEXT_146 = NL + NL + "\t\t        this.connection = java.sql.DriverManager.getConnection(";
  protected final String TEXT_147 = ", ";
  protected final String TEXT_148 = ", decryptedPassword_";
  protected final String TEXT_149 = ");" + NL + "\t\t        this.connection.setAutoCommit(false);" + NL + "\t\t\t\tthis.connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);" + NL + "\t\t    } catch (Exception ex) {" + NL + "\t\t        throw new RuntimeException(ex);" + NL + "\t\t    }" + NL + "\t    }" + NL + "" + NL + "\t\tpublic RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {" + NL + "\t\t\ttry {" + NL + "\t\t  \t\treturn new DBRecordReader(connection, (new org.talend.hadoop.mapred.lib.db.JDBCHelper(connection)).getDBSql(split,";
  protected final String TEXT_150 = ",\"";
  protected final String TEXT_151 = "\"), split.getLength());" + NL + "\t\t  \t} catch (SQLException ex) {" + NL + "\t            throw new IOException(ex.getMessage());" + NL + "\t        }" + NL + "\t\t}" + NL + "" + NL + "\t\tpublic InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {" + NL + "            try {" + NL + "\t            java.sql.Statement statement = connection.createStatement();" + NL + "" + NL + "\t            java.sql.ResultSet results = statement.executeQuery(";
  protected final String TEXT_152 = ");" + NL + "\t            results.next();" + NL + "" + NL + "\t            long count = results.getLong(1);" + NL + "\t            long chunkSize = (count / numSplits);" + NL + "" + NL + "\t            results.close();" + NL + "\t            statement.close();" + NL + "" + NL + "\t            InputSplit[] splits = new InputSplit[numSplits];" + NL + "" + NL + "\t            // Split the rows into n-number of numSplits and adjust the last chunk" + NL + "\t            // accordingly" + NL + "\t            for (int iterator = 0; iterator < numSplits; iterator++) {" + NL + "\t                org.talend.hadoop.mapred.lib.db.DBTableSplit split;" + NL + "" + NL + "\t                if ((iterator + 1) == numSplits) {" + NL + "\t                    split = new org.talend.hadoop.mapred.lib.db.DBTableSplit(iterator * chunkSize, count);" + NL + "\t                } else {" + NL + "\t                    split = new org.talend.hadoop.mapred.lib.db.DBTableSplit(iterator * chunkSize, (iterator * chunkSize) + chunkSize);" + NL + "\t                }" + NL + "\t                splits[iterator] = split;" + NL + "\t            }" + NL + "" + NL + "\t            return splits;" + NL + "\t        } catch (java.sql.SQLException e) {" + NL + "\t            throw new IOException(e.getMessage());" + NL + "\t        }" + NL + "        }" + NL + "" + NL + "        protected static class DBRecordReader implements RecordReader<NullWritable, ";
  protected final String TEXT_153 = "TemporaryStruct> {" + NL + "" + NL + "\t        private java.sql.ResultSet results;" + NL + "" + NL + "\t        private java.sql.Statement statement;" + NL + "" + NL + "\t        private long pos = 0;" + NL + "" + NL + "\t        private long splitLength = 0;" + NL + "" + NL + "\t        /**" + NL + "\t         * @param split The InputSplit to read data for" + NL + "\t         * @throws SQLException" + NL + "\t         */" + NL + "\t        protected DBRecordReader(java.sql.Connection connection, String query, long splitLength) throws SQLException {" + NL + "\t            statement = connection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);" + NL + "\t            results = statement.executeQuery(query);" + NL + "\t            this.splitLength = splitLength; " + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public void close() throws IOException {" + NL + "\t            try {" + NL + "\t                results.close();" + NL + "\t                statement.close();" + NL + "\t            } catch (SQLException e) {" + NL + "\t                throw new IOException(e.getMessage());" + NL + "\t            }" + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public NullWritable createKey() {" + NL + "\t            return NullWritable.get();" + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public ";
  protected final String TEXT_154 = "TemporaryStruct createValue() {" + NL + "\t            return new ";
  protected final String TEXT_155 = "TemporaryStruct();" + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public long getPos() throws IOException {" + NL + "\t            return pos;" + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public float getProgress() throws IOException {" + NL + "\t            return pos / (float)splitLength;" + NL + "\t        }" + NL + "" + NL + "\t        /** {@inheritDoc} */" + NL + "\t        public boolean next(NullWritable key, ";
  protected final String TEXT_156 = "TemporaryStruct value) throws IOException{" + NL + "                value.set(results);" + NL + "                try {" + NL + "                    return !results.isAfterLast();" + NL + "                } catch (SQLException sqlEx_";
  protected final String TEXT_157 = ") {" + NL + "                    return false;" + NL + "                }" + NL + "\t        }" + NL + "\t    }" + NL + "\t} // end of ";
  protected final String TEXT_158 = "StructInputFormat" + NL + "" + NL + "\tpublic static class ";
  protected final String TEXT_159 = "_InputMapper extends MapReduceBase " + NL + "    implements Mapper<NullWritable, ";
  protected final String TEXT_160 = "TemporaryStruct, NullWritable, WritableComparable>{" + NL + "        private ContextProperties context;" + NL + "        private GlobalVar globalMap;" + NL + "        public MultipleOutputs mos_";
  protected final String TEXT_161 = ";" + NL + "        private ";
  protected final String TEXT_162 = "Struct ";
  protected final String TEXT_163 = " = null;" + NL + "        private ";
  protected final String TEXT_164 = "Struct ";
  protected final String TEXT_165 = " = null;" + NL + "" + NL + "        public void configure(JobConf job_";
  protected final String TEXT_166 = "){" + NL + "            context = new ContextProperties(job_";
  protected final String TEXT_167 = ");" + NL + "            globalMap = new GlobalVar(job_";
  protected final String TEXT_168 = ");" + NL + "            mos_";
  protected final String TEXT_169 = " = new MultipleOutputs(job_";
  protected final String TEXT_170 = ");" + NL;
  protected final String TEXT_171 = NL + "            ";
  protected final String TEXT_172 = " = new ";
  protected final String TEXT_173 = "Struct();";
  protected final String TEXT_174 = NL + "            ";
  protected final String TEXT_175 = " = new ";
  protected final String TEXT_176 = "Struct();" + NL;
  protected final String TEXT_177 = NL + "                    try {" + NL + "                        FileSystem fs = FileSystem.get(job_";
  protected final String TEXT_178 = ");" + NL + "                        Path path = new Path(" + NL + "                                \"/tmp/";
  protected final String TEXT_179 = "\"" + NL + "                                + \"/tMROutput_";
  protected final String TEXT_180 = "\"" + NL + "                                + \"/";
  protected final String TEXT_181 = "\");" + NL + "                        fs.mkdirs(path);" + NL + "                    } catch (IOException ex_";
  protected final String TEXT_182 = ") {" + NL + "                        throw new RuntimeException(ex_";
  protected final String TEXT_183 = ".getMessage());" + NL + "                    }";
  protected final String TEXT_184 = NL + "        }" + NL + "" + NL + "        public void map(NullWritable key_";
  protected final String TEXT_185 = ", ";
  protected final String TEXT_186 = "TemporaryStruct value_";
  protected final String TEXT_187 = ", " + NL + "                OutputCollector<NullWritable, WritableComparable> output_";
  protected final String TEXT_188 = ", Reporter reporter_";
  protected final String TEXT_189 = ") throws IOException{" + NL + "                try {" + NL + "                ResultSet resultSet = (ResultSet)value_";
  protected final String TEXT_190 = ".get();" + NL + "                while (resultSet.next()) {" + NL + "                    try {";
  protected final String TEXT_191 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_192 = ") != null){";
  protected final String TEXT_193 = NL + "                                    ";
  protected final String TEXT_194 = ".";
  protected final String TEXT_195 = " = resultSet.getBoolean(";
  protected final String TEXT_196 = ");" + NL + "                                }else{";
  protected final String TEXT_197 = NL + "                                        ";
  protected final String TEXT_198 = ".";
  protected final String TEXT_199 = " = null;";
  protected final String TEXT_200 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_201 = " in non-Nullable column\");";
  protected final String TEXT_202 = NL + "                                }";
  protected final String TEXT_203 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_204 = ") != null){";
  protected final String TEXT_205 = NL + "                                    ";
  protected final String TEXT_206 = ".";
  protected final String TEXT_207 = " = resultSet.getByte(";
  protected final String TEXT_208 = ");" + NL + "                                }else{";
  protected final String TEXT_209 = NL + "                                        ";
  protected final String TEXT_210 = ".";
  protected final String TEXT_211 = " = null;";
  protected final String TEXT_212 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_213 = " in non-Nullable column\");";
  protected final String TEXT_214 = NL + "                                }";
  protected final String TEXT_215 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_216 = ") != null){";
  protected final String TEXT_217 = NL + "                                    ";
  protected final String TEXT_218 = ".";
  protected final String TEXT_219 = " = resultSet.getBytes(";
  protected final String TEXT_220 = ");" + NL + "                                }else{";
  protected final String TEXT_221 = NL + "                                        ";
  protected final String TEXT_222 = ".";
  protected final String TEXT_223 = " = null;";
  protected final String TEXT_224 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_225 = " in non-Nullable column\");";
  protected final String TEXT_226 = NL + "                                }";
  protected final String TEXT_227 = NL + "                                if(resultSet.getString(";
  protected final String TEXT_228 = ") != null && resultSet.getString(";
  protected final String TEXT_229 = ").length() > 0){";
  protected final String TEXT_230 = NL + "                                    ";
  protected final String TEXT_231 = ".";
  protected final String TEXT_232 = " = resultSet.getString(";
  protected final String TEXT_233 = ").charAt(0);" + NL + "                                }else{" + NL + "                                    if(resultSet.getString(";
  protected final String TEXT_234 = ") == null){";
  protected final String TEXT_235 = NL + "                                            ";
  protected final String TEXT_236 = ".";
  protected final String TEXT_237 = " = null;";
  protected final String TEXT_238 = NL + "                                            throw new RuntimeException(\"Null ";
  protected final String TEXT_239 = " in non-Nullable column\");";
  protected final String TEXT_240 = NL + "                                    }else{";
  protected final String TEXT_241 = NL + "                                        ";
  protected final String TEXT_242 = ".";
  protected final String TEXT_243 = " = '\\0';" + NL + "                                    }" + NL + "                                }";
  protected final String TEXT_244 = NL + "                                try{";
  protected final String TEXT_245 = NL + "                                    ";
  protected final String TEXT_246 = ".";
  protected final String TEXT_247 = " = resultSet.getTimestamp(";
  protected final String TEXT_248 = ");" + NL + "                                }catch(java.lang.Exception e){";
  protected final String TEXT_249 = NL + "                                    ";
  protected final String TEXT_250 = ".";
  protected final String TEXT_251 = " = resultSet.getDate(";
  protected final String TEXT_252 = ");" + NL + "                                }";
  protected final String TEXT_253 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_254 = ") != null){";
  protected final String TEXT_255 = NL + "                                    ";
  protected final String TEXT_256 = ".";
  protected final String TEXT_257 = " = resultSet.getDouble(";
  protected final String TEXT_258 = ");" + NL + "                                }else{";
  protected final String TEXT_259 = NL + "                                        ";
  protected final String TEXT_260 = ".";
  protected final String TEXT_261 = " = null;";
  protected final String TEXT_262 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_263 = " in non-Nullable column\");";
  protected final String TEXT_264 = NL + "                                }";
  protected final String TEXT_265 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_266 = ") != null){";
  protected final String TEXT_267 = NL + "                                    ";
  protected final String TEXT_268 = ".";
  protected final String TEXT_269 = " = resultSet.getFloat(";
  protected final String TEXT_270 = ");" + NL + "                                }else{";
  protected final String TEXT_271 = NL + "                                        ";
  protected final String TEXT_272 = ".";
  protected final String TEXT_273 = " = null;";
  protected final String TEXT_274 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_275 = " in non-Nullable column\");";
  protected final String TEXT_276 = NL + "                                }";
  protected final String TEXT_277 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_278 = ") != null){";
  protected final String TEXT_279 = NL + "                                    ";
  protected final String TEXT_280 = ".";
  protected final String TEXT_281 = " = resultSet.getBigDecimal(";
  protected final String TEXT_282 = ");" + NL + "                                }else{";
  protected final String TEXT_283 = NL + "                                        ";
  protected final String TEXT_284 = ".";
  protected final String TEXT_285 = " = null;";
  protected final String TEXT_286 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_287 = " in non-Nullable column\");";
  protected final String TEXT_288 = NL + "                                }";
  protected final String TEXT_289 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_290 = ") != null){";
  protected final String TEXT_291 = NL + "                                    ";
  protected final String TEXT_292 = ".";
  protected final String TEXT_293 = " = resultSet.getInt(";
  protected final String TEXT_294 = ");" + NL + "                                }else{";
  protected final String TEXT_295 = NL + "                                        ";
  protected final String TEXT_296 = ".";
  protected final String TEXT_297 = " = null;";
  protected final String TEXT_298 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_299 = " in non-Nullable column\");";
  protected final String TEXT_300 = NL + "                                }";
  protected final String TEXT_301 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_302 = ") != null){";
  protected final String TEXT_303 = NL + "                                    ";
  protected final String TEXT_304 = ".";
  protected final String TEXT_305 = " = resultSet.getLong(";
  protected final String TEXT_306 = ");" + NL + "                                }else{";
  protected final String TEXT_307 = NL + "                                        ";
  protected final String TEXT_308 = ".";
  protected final String TEXT_309 = " = null;";
  protected final String TEXT_310 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_311 = " in non-Nullable column\");";
  protected final String TEXT_312 = NL + "                                }";
  protected final String TEXT_313 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_314 = ") != null){";
  protected final String TEXT_315 = NL + "                                    ";
  protected final String TEXT_316 = ".";
  protected final String TEXT_317 = " = resultSet.getObject(";
  protected final String TEXT_318 = ");" + NL + "                                }else{";
  protected final String TEXT_319 = NL + "                                        ";
  protected final String TEXT_320 = ".";
  protected final String TEXT_321 = " = null;";
  protected final String TEXT_322 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_323 = " in non-Nullable column\");";
  protected final String TEXT_324 = NL + "                                }";
  protected final String TEXT_325 = NL + "                                if(resultSet.getObject(";
  protected final String TEXT_326 = ") != null){";
  protected final String TEXT_327 = NL + "                                    ";
  protected final String TEXT_328 = ".";
  protected final String TEXT_329 = " = resultSet.getShort(";
  protected final String TEXT_330 = ");" + NL + "                                }else{";
  protected final String TEXT_331 = NL + "                                        ";
  protected final String TEXT_332 = ".";
  protected final String TEXT_333 = " = null;";
  protected final String TEXT_334 = NL + "                                        throw new RuntimeException(\"Null ";
  protected final String TEXT_335 = " in non-Nullable column\");";
  protected final String TEXT_336 = NL + "                                }";
  protected final String TEXT_337 = NL + "                                if(resultSet.getString(";
  protected final String TEXT_338 = ") != null){";
  protected final String TEXT_339 = NL + "                                    ";
  protected final String TEXT_340 = ".";
  protected final String TEXT_341 = " = resultSet.getString(";
  protected final String TEXT_342 = ");" + NL + "                                }else{";
  protected final String TEXT_343 = NL + "                                    ";
  protected final String TEXT_344 = ".";
  protected final String TEXT_345 = " = null;" + NL + "                                }";
  protected final String TEXT_346 = NL + "                                ";
  protected final String TEXT_347 = ".";
  protected final String TEXT_348 = " = (java.util.List)resultSet.getObject(";
  protected final String TEXT_349 = ");";
  protected final String TEXT_350 = NL + "                        output_";
  protected final String TEXT_351 = ".collect(NullWritable.get(), ";
  protected final String TEXT_352 = ");" + NL + "                    } catch (Exception ex_";
  protected final String TEXT_353 = ") {";
  protected final String TEXT_354 = NL + "                        ";
  protected final String TEXT_355 = ".invalidInputLine = value_";
  protected final String TEXT_356 = ".toString();";
  protected final String TEXT_357 = NL + "                        ";
  protected final String TEXT_358 = ".errorCode = ex_";
  protected final String TEXT_359 = ".toString();" + NL + "                        mos_";
  protected final String TEXT_360 = ".getCollector(\"";
  protected final String TEXT_361 = "\", reporter_";
  protected final String TEXT_362 = ")" + NL + "                                .collect(NullWritable.get(), ";
  protected final String TEXT_363 = ");" + NL + "                    }" + NL + "                }" + NL + "            } catch (SQLException sqlEx_";
  protected final String TEXT_364 = ") {";
  protected final String TEXT_365 = NL + "                ";
  protected final String TEXT_366 = ".invalidInputLine = value_";
  protected final String TEXT_367 = ".toString();";
  protected final String TEXT_368 = NL + "                ";
  protected final String TEXT_369 = ".errorCode = sqlEx_";
  protected final String TEXT_370 = ".toString();" + NL + "                mos_";
  protected final String TEXT_371 = ".getCollector(\"";
  protected final String TEXT_372 = "\", reporter_";
  protected final String TEXT_373 = ")" + NL + "                        .collect(NullWritable.get(), ";
  protected final String TEXT_374 = ");" + NL + "            }" + NL + "        }" + NL + "" + NL + "        public void close() throws IOException{" + NL + "            mos_";
  protected final String TEXT_375 = ".close();" + NL + "        }" + NL + "    } // end of ";
  protected final String TEXT_376 = "_InputMapper" + NL + "" + NL + "\t";
  protected final String TEXT_377 = "    ";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

List<IMetadataTable> metadatas = node.getMetadataList();
if ((metadatas!=null) && (metadatas.size() > 0)) {
    // We don't what to get the metadata of the main flow, so we check if the current metadata is not the reject flow
    int flowMetadataPosition = 0;
    while ((flowMetadataPosition < metadatas.size()) && ("REJECT".equals(metadatas.get(flowMetadataPosition).getTableName()))) {
        flowMetadataPosition++;
    }
    IMetadataTable metadata = metadatas.get(flowMetadataPosition);

    if (metadata != null) {

		String localServiceName = ElementParameterParser.getValue(node, "__LOCAL_SERVICE_NAME__");
		String connectionType = ElementParameterParser.getValue(node, "__CONNECTION_TYPE__");
		String jdbcURL = ElementParameterParser.getValue(node, "__JDBC_URL__");
		String racURL = ElementParameterParser.getValue(node, "__RAC_URL__");
		String dbhost = ElementParameterParser.getValue(node, "__HOST__");
		String dbport = ElementParameterParser.getValue(node, "__PORT__");
		String dbname = ElementParameterParser.getValue(node, "__DBNAME__");
		String dbVersion = ElementParameterParser.getValue(node, "__DB_VERSION__");
		
		String url = null;
		if("ORACLE_RAC".equals(connectionType)) {
			url = racURL;
		} else if(("ORACLE_SID").equals(connectionType)) {
			url = "\"jdbc:oracle:thin:@\"+" + dbhost + "+\":\"+" + dbport + "+\":\"+" + dbname;
		} else if(("ORACLE_SERVICE_NAME").equals(connectionType)) {
			url = "\"jdbc:oracle:thin:@(description=(address=(protocol=tcp)(host=\"+" + dbhost + "+\")(port=\"+" + dbport + "+\"))(connect_data=(service_name=\"+" + dbname + "+\")))\"";
		} else if(("ORACLE_OCI").equals(connectionType)) {
			url = "\"jdbc:oracle:oci8:@\"+" + localServiceName;
		}else if(("ORACLE_WALLET").equals(connectionType)) {
   		url = jdbcURL;
		}
		
		String driverClass = null;
		if("ORACLE_11".equals(dbVersion) || "ORACLE_11-6".equals(dbVersion) || "ORACLE_12".equals(dbVersion) ){
			driverClass = "\"oracle.jdbc.OracleDriver\"";	
		}else {
			driverClass = "\"oracle.jdbc.driver.OracleDriver\"";	
		}
 
        String dbuser = ElementParameterParser.getValue(node, "__USER__");
        String dbquery= ElementParameterParser.getValue(node, "__QUERY__");
        boolean dieOnError = "true".equals(ElementParameterParser.getValue(node, "__DIE_ON_ERROR__"));
        dbquery = dbquery.replaceAll("\n"," ");
        dbquery = dbquery.replaceAll("\r"," ");
        String dbCountQuery = "\"select count(*) from (\"+" + dbquery + "+\") talendTable\"";

        List< ? extends IConnection> conns = node.getOutgoingSortedConnections();
        List< ? extends IConnection> rejectedConnections = node.getOutgoingConnections("REJECT");

        // Fail fast when no output connections exist.
        if (conns == null || conns.size() == 0)
            return "";

        // Fail fast when only reject connections exist (there must be at least
        // one main connection for this component.)
        if (rejectedConnections != null && rejectedConnections.size() == conns.size()) {
            
    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    
        } else if ((rejectedConnections == null) || (rejectedConnections.size() == 0)) {
            
    
IConnection conn = conns.get(0);
String connName = conn.getName();

if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

	List<IMetadataColumn> columns = metadata.getListColumns();
	String columnStr="";
	String firstColumn="";
	for(IMetadataColumn column:columns){
		if(columnStr!=""){
			columnStr +=",";
		}else{
			firstColumn = column.getOriginalDbColumnName();
		}
		columnStr +=column.getOriginalDbColumnName();
	}
	
    stringBuffer.append(TEXT_3);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(driverClass);
    stringBuffer.append(TEXT_6);
    
                String passwordFieldName = "__PASS__";
                
    stringBuffer.append(TEXT_7);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_10);
    } else {
    stringBuffer.append(TEXT_11);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_12);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_13);
    }
    stringBuffer.append(TEXT_14);
    stringBuffer.append(url);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(dbquery);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(columnStr);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(dbCountQuery);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_23);
    stringBuffer.append(connName);
    stringBuffer.append(TEXT_24);
    
	                int i = 1;
		            for(IMetadataColumn column: columns){
		                String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
						String defaultValue = JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
		                if("Boolean".equals(typeToGenerate) || "boolean".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_25);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_26);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_27);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_28);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_29);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_30);
    
								}else{
								
    stringBuffer.append(TEXT_31);
    
								}
								
    stringBuffer.append(TEXT_32);
    
		                }else if("Byte".equals(typeToGenerate) || "byte".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_33);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_34);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_35);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_36);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_37);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_38);
    
								}else{
								
    stringBuffer.append(TEXT_39);
    
								}
								
    stringBuffer.append(TEXT_40);
    
		                }else if("byte[]".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_41);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_42);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_43);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_44);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_45);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_46);
    
								}else{
								
    stringBuffer.append(TEXT_47);
    
								}
								
    stringBuffer.append(TEXT_48);
    
		                }else if("char".equals(typeToGenerate) || "Character".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_49);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_52);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_54);
    
			                    	if(column.isNullable()){
									
    stringBuffer.append(TEXT_55);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_56);
    
									}else{
									
    stringBuffer.append(TEXT_57);
    
									}
									
    stringBuffer.append(TEXT_58);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_59);
    
		                }else if("java.util.Date".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_60);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_61);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_63);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_64);
    
		                }else if("Double".equals(typeToGenerate) || "double".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_65);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_67);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_68);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_69);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_70);
    
								}else{
								
    stringBuffer.append(TEXT_71);
    
								}
								
    stringBuffer.append(TEXT_72);
    
		                }else if("Float".equals(typeToGenerate) || "float".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_73);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_75);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_76);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_77);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_78);
    
								}else{
								
    stringBuffer.append(TEXT_79);
    
								}
								
    stringBuffer.append(TEXT_80);
    
		                }else if("BigDecimal".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_81);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_83);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_84);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_85);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_86);
    
								}else{
								
    stringBuffer.append(TEXT_87);
    
								}
								
    stringBuffer.append(TEXT_88);
    
		                }else if("Integer".equals(typeToGenerate) || "int".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_89);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_90);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_91);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_92);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_93);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_94);
    
								}else{
								
    stringBuffer.append(TEXT_95);
    
								}
								
    stringBuffer.append(TEXT_96);
    
		                }else if("Long".equals(typeToGenerate) || "long".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_97);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_99);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_100);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_101);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_102);
    
								}else{
								
    stringBuffer.append(TEXT_103);
    
								}
								
    stringBuffer.append(TEXT_104);
    
		                }else if("Object".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_105);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_107);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_108);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_109);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_110);
    
								}else{
								
    stringBuffer.append(TEXT_111);
    
								}
								
    stringBuffer.append(TEXT_112);
    
		                }else if("Short".equals(typeToGenerate) || "short".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_113);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_114);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_115);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_116);
    
		                    	if(column.isNullable()){
								
    stringBuffer.append(TEXT_117);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_118);
    
								}else{
								
    stringBuffer.append(TEXT_119);
    
								}
								
    stringBuffer.append(TEXT_120);
    
		                }else if("String".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_121);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_122);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_123);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_125);
    
		                }else if("List".equals(typeToGenerate)){
		                
    stringBuffer.append(TEXT_126);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_127);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_128);
    
		                }
		        		i++;
		            }
	                
    stringBuffer.append(TEXT_129);
    
	                if (dieOnError) {
	                    
    stringBuffer.append(TEXT_130);
    
	                } else {
	                    
    stringBuffer.append(TEXT_131);
    
	                }
	                
    stringBuffer.append(TEXT_132);
    
}

    stringBuffer.append(TEXT_133);
    
        } else if ((rejectedConnections != null)  && (rejectedConnections.size() == 1)){
            
    
List< ? extends IConnection> mainConnections = node.getOutgoingConnections("FLOW");
IConnection mainConnection = mainConnections.get(0);
String mainConnectionName = mainConnection.getName();
IConnection rejectedConnection = rejectedConnections.get(0);
String rejectedConnectionName = rejectedConnection.getName();

if (mainConnection.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {

	List<IMetadataColumn> columns = metadata.getListColumns();
	String columnStr="";
	String firstColumn="";
	for(IMetadataColumn column:columns){
		if(columnStr!=""){
			columnStr +=",";
		}else{
			firstColumn = column.getOriginalDbColumnName();
		}
		columnStr +=column.getOriginalDbColumnName();
	}
	
    stringBuffer.append(TEXT_134);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(driverClass);
    stringBuffer.append(TEXT_138);
    
                String passwordFieldName = "__PASS__";
                
    stringBuffer.append(TEXT_139);
    if (ElementParameterParser.canEncrypt(node, passwordFieldName)) {
    stringBuffer.append(TEXT_140);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(ElementParameterParser.getEncryptedValue(node, passwordFieldName));
    stringBuffer.append(TEXT_142);
    } else {
    stringBuffer.append(TEXT_143);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_144);
    stringBuffer.append( ElementParameterParser.getValue(node, passwordFieldName));
    stringBuffer.append(TEXT_145);
    }
    stringBuffer.append(TEXT_146);
    stringBuffer.append(url);
    stringBuffer.append(TEXT_147);
    stringBuffer.append(dbuser);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(dbquery);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(columnStr);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(dbCountQuery);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_163);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_164);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_172);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_173);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_176);
    
            // Force the creation of the output directory
            for (INode virtualNode : node.getProcess().getGraphicalNodes()) {
                if (virtualNode.getUniqueName().equals(cid)) {
                    
    stringBuffer.append(TEXT_177);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(node.getProcess().getName());
    stringBuffer.append(TEXT_179);
    stringBuffer.append(virtualNode.getUniqueName());
    stringBuffer.append(TEXT_180);
    stringBuffer.append(virtualNode.getOutgoingConnections("REJECT").get(0).getName());
    stringBuffer.append(TEXT_181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_183);
    
                    break;
                }
            }
            
    stringBuffer.append(TEXT_184);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_188);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_190);
    
                        int i = 1;
                        for(IMetadataColumn column: columns){
                            String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                            String defaultValue = JavaTypesManager.getDefaultValueFromJavaType(typeToGenerate);
                            if("Boolean".equals(typeToGenerate) || "boolean".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_191);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_194);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_195);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_196);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_197);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_198);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_199);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_200);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_201);
    
                                    }
                                    
    stringBuffer.append(TEXT_202);
    
                            }else if("Byte".equals(typeToGenerate) || "byte".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_203);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(TEXT_205);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_206);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_207);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_208);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_209);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_211);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_212);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_213);
    
                                    }
                                    
    stringBuffer.append(TEXT_214);
    
                            }else if("byte[]".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_215);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_218);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_219);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_220);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_221);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_222);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_223);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_224);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_225);
    
                                    }
                                    
    stringBuffer.append(TEXT_226);
    
                            }else if("char".equals(typeToGenerate) || "Character".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_227);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_232);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_233);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_234);
    
                                        if(column.isNullable()){
                                        
    stringBuffer.append(TEXT_235);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_236);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_237);
    
                                        }else{
                                        
    stringBuffer.append(TEXT_238);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_239);
    
                                        }
                                        
    stringBuffer.append(TEXT_240);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_243);
    
                            }else if("java.util.Date".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_244);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_247);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_251);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_252);
    
                            }else if("Double".equals(typeToGenerate) || "double".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_253);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_254);
    stringBuffer.append(TEXT_255);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_256);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_257);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_258);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_259);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_260);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_261);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_262);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_263);
    
                                    }
                                    
    stringBuffer.append(TEXT_264);
    
                            }else if("Float".equals(typeToGenerate) || "float".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_265);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_266);
    stringBuffer.append(TEXT_267);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_268);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_269);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_270);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_271);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_273);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_274);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_275);
    
                                    }
                                    
    stringBuffer.append(TEXT_276);
    
                            }else if("BigDecimal".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_277);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(TEXT_279);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_280);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_281);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_282);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_283);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_285);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_286);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_287);
    
                                    }
                                    
    stringBuffer.append(TEXT_288);
    
                            }else if("Integer".equals(typeToGenerate) || "int".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_289);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_293);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_294);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_295);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_296);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_297);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_298);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_299);
    
                                    }
                                    
    stringBuffer.append(TEXT_300);
    
                            }else if("Long".equals(typeToGenerate) || "long".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_301);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_302);
    stringBuffer.append(TEXT_303);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_304);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_305);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_306);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_307);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_309);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_310);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_311);
    
                                    }
                                    
    stringBuffer.append(TEXT_312);
    
                            }else if("Object".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_313);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(TEXT_315);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_316);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_317);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_318);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_319);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_320);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_321);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_322);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_323);
    
                                    }
                                    
    stringBuffer.append(TEXT_324);
    
                            }else if("Short".equals(typeToGenerate) || "short".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_325);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_326);
    stringBuffer.append(TEXT_327);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_328);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_329);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_330);
    
                                    if(column.isNullable()){
                                    
    stringBuffer.append(TEXT_331);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_332);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_333);
    
                                    }else{
                                    
    stringBuffer.append(TEXT_334);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_335);
    
                                    }
                                    
    stringBuffer.append(TEXT_336);
    
                            }else if("String".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_337);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_338);
    stringBuffer.append(TEXT_339);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_340);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_341);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(TEXT_343);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_344);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_345);
    
                            }else if("List".equals(typeToGenerate)){
                            
    stringBuffer.append(TEXT_346);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_347);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_348);
    stringBuffer.append(i);
    stringBuffer.append(TEXT_349);
    
                            }
                            i++;
                        }
                        
    stringBuffer.append(TEXT_350);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_351);
    stringBuffer.append(mainConnectionName);
    stringBuffer.append(TEXT_352);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_353);
    stringBuffer.append(TEXT_354);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_355);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_356);
    stringBuffer.append(TEXT_357);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_358);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_359);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_361);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_362);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_363);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_364);
    stringBuffer.append(TEXT_365);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_366);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_369);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_370);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_371);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_372);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_373);
    stringBuffer.append(rejectedConnectionName);
    stringBuffer.append(TEXT_374);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_375);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_376);
    
}

    stringBuffer.append(TEXT_377);
    
        }
    }
}

    return stringBuffer.toString();
  }
}
