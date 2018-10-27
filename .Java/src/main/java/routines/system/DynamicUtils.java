/**
 * 
 */
package routines.system;

/**
 * @author Administrator
 * 
 */

public class DynamicUtils {

    /**
     * @author parham parvizi
     * @description: write all the values in the dynamic schema to a delimited file writer. fields are separated by the
     * fieldSeparator.
     * @param column the dynamic column to write
     * @param out a java.io.Writer
     * @param fieldSeparator field delimiter
     * @throws java.io.IOException
     */
    public static void writeValuesToDelimitedFile(Dynamic column, java.io.Writer out, String fieldSeparator)
            throws java.io.IOException {
        for (int i = 0; i < column.getColumnCount(); i++) {
            if (column.getColumnValue(i) != null) {
                DynamicMetadata metadata = column.getColumnMetadata(i);
                if ("id_Date".equals(metadata.getType())
                        && !(DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId()) && !(metadata.getDbType()
                                .toLowerCase().indexOf("timestamp") < 0))) {
                    out.write(formatDate(column.getColumnValue(i), metadata.getFormat()));
                } else {
                    out.write(String.valueOf(column.getColumnValue(i)));
                }
            }
            if (i != (column.getColumnCount() - 1)) {
                out.write(fieldSeparator);
            }
        }
        out.flush();
    }

    public static void writeValuesToStringBuilder(Dynamic column, Appendable sb, String fieldSeparator)
            throws java.io.IOException {
        for (int i = 0; i < column.getColumnCount(); i++) {
            if (column.getColumnValue(i) != null) {
                DynamicMetadata metadata = column.getColumnMetadata(i);
                if ("id_Date".equals(metadata.getType())
                        && !(DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId()) && !(metadata.getDbType()
                                .toLowerCase().indexOf("timestamp") < 0))) {
                    sb.append(formatDate(column.getColumnValue(i), metadata.getFormat()));
                } else {
                    sb.append(String.valueOf(column.getColumnValue(i)));
                }
            }
            if (i != (column.getColumnCount() - 1)) {
                sb.append(fieldSeparator);
            }
        }
    }

    public static void writeValuesToStringArray(Dynamic column, String[] row, int offset) {
        for (int i = 0; i < column.getColumnCount(); i++) {
            if (column.getColumnValue(i) != null) {
                DynamicMetadata metadata = column.getColumnMetadata(i);
                if ("id_Date".equals(metadata.getType())
                        && !(DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId()) && !(metadata.getDbType()
                                .toLowerCase().indexOf("timestamp") < 0))) {
                    row[offset + i] = formatDate(column.getColumnValue(i), metadata.getFormat());
                } else {
                    row[offset + i] = String.valueOf(column.getColumnValue(i));
                }
            } else {
                row[offset + i] = "";
            }
        }
    }

    public static void writeValuesToStringArrayEnhance(Dynamic column, String[] row, int offset,String str4Null) {
        for (int i = 0; i < column.getColumnCount(); i++) {
            if (column.getColumnValue(i) != null) {
                DynamicMetadata metadata = column.getColumnMetadata(i);
                if ("id_Date".equals(metadata.getType())
                        && !(DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId()) && !(metadata.getDbType()
                                .toLowerCase().indexOf("timestamp") < 0))) {
                    row[offset + i] = formatDate(column.getColumnValue(i), metadata.getFormat());
                } else {
                    row[offset + i] = String.valueOf(column.getColumnValue(i));
                }
            } else {
                if(str4Null == null){
                	row[offset + i] = "";
                }else{
                	row[offset + i] = null;
                }                
            }
        }
    }

    public static void writeHeaderToDelimitedFile(Dynamic column, java.io.Writer out, String fieldSeparator)
            throws java.io.IOException {
        for (int i = 0; i < column.getColumnCount(); i++) {
            out.write(column.getColumnMetadata(i).getName());
            if (i != (column.getColumnCount() - 1)) {
                out.write(fieldSeparator);
            }
        }
        out.flush();
    }

    public static void readColumnsFromDelimitedFile(Dynamic column, org.talend.fileprocess.FileInputDelimited fid,
            int fixedColumnCount) throws Exception {
        int fieldCount = fid.getColumnsCountOfCurrentRow();
        for (int i = 0; i < column.getColumnCount(); i++) {
            if ((fixedColumnCount + i) < fieldCount) {
                column.addColumnValue(fid.get((fixedColumnCount + i)));
            } else {
                column.addColumnValue("");
            }
        }
    }

    public static void readColumnsFromDatabase(Dynamic column, java.sql.ResultSet rs, int fixedColumnCount, boolean trim) throws Exception {
        column.clearColumnValues();
        for (int i = 0; i < column.getColumnCount(); i++) {
            DynamicMetadata dcm = column.getColumnMetadata(i);

            if ("id_String".equals(dcm.getType())) {
                String value = rs.getString(fixedColumnCount + i + 1);
                column.addColumnValue((trim && (value!=null)) ? value.trim() : value);
            } else if ("id_Date".equals(dcm.getType())) {
                if (DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId())
                        && !(dcm.getDbType().toLowerCase().indexOf("timestamp") < 0)) {
                    column.addColumnValue(rs.getString(fixedColumnCount + i + 1));
                } else if (DBMSConstants.NETEZZA.getDBmsId().equalsIgnoreCase(column.getDbmsId())
                        && "time".equalsIgnoreCase(dcm.getDbType())) {
                    column.addColumnValue(rs.getTime(fixedColumnCount + i + 1));
                } else {
                    column.addColumnValue(rs.getTimestamp(fixedColumnCount + i + 1));
                }
            } else if ("id_Integer".equals(dcm.getType()) || "id_Long".equals(dcm.getType()) || "id_Double".equals(dcm.getType())
                    || "id_Byte".equals(dcm.getType()) || "id_byte[]".equals(dcm.getType())) {
                if (rs.getObject(fixedColumnCount + i + 1) == null) {
                    column.addColumnValue(null);
                    continue;
                }
                if ("id_Integer".equals(dcm.getType())) {
                    column.addColumnValue(rs.getInt(fixedColumnCount + i + 1));
                } else if ("id_Long".equals(dcm.getType())) {
                    column.addColumnValue(rs.getLong(fixedColumnCount + i + 1));
                } else if ("id_Double".equals(dcm.getType())) {
                    column.addColumnValue(rs.getDouble(fixedColumnCount + i + 1));
                } else if ("id_Byte".equals(dcm.getType())) {
                    column.addColumnValue(rs.getByte(fixedColumnCount + i + 1));
                } else if ("id_byte[]".equals(dcm.getType())) {
                    column.addColumnValue(rs.getBytes(fixedColumnCount + i + 1));
                }
            }else  if ("id_Boolean".equals(dcm.getType())) {
                if (rs.getObject(fixedColumnCount + i + 1) == null) {
                    column.addColumnValue(null);
                }else{
                    column.addColumnValue(rs.getBoolean(fixedColumnCount + i + 1));
                }
            } else {
                column.addColumnValue(rs.getObject(fixedColumnCount + i + 1));
            }
        }
    }

    public static void readColumnsFromDatabase_Access(Dynamic column, java.sql.ResultSet rs, int fixedColumnCount, boolean trim)
            throws Exception {
        column.clearColumnValues();
        for (int i = 0; i < column.getColumnCount(); i++) {
            DynamicMetadata dcm = column.getColumnMetadata(i);
            if ("id_String".equals(dcm.getType())) {
                String value = rs.getString(fixedColumnCount + i + 1);
                column.addColumnValue((trim && (value!=null)) ? value.trim() : value);
            } else if ("id_Date".equals(dcm.getType())) {
                column.addColumnValue(rs.getTimestamp(fixedColumnCount + i + 1));
            } else if ("id_Byte".equals(dcm.getType()) || "id_Short".equals(dcm.getType()) || "id_Integer".equals(dcm.getType())
                    || "id_Long".equals(dcm.getType()) || "id_Float".equals(dcm.getType()) || "id_Double".equals(dcm.getType())) {
                Object obj = rs.getObject(fixedColumnCount + i + 1);
                if (obj == null) {
                    column.addColumnValue(null);
                    continue;
                }
                if ("id_Byte".equals(dcm.getType())) {
                    column.addColumnValue(Byte.parseByte(obj.toString()));
                } else if ("id_Short".equals(dcm.getType())) {
                    column.addColumnValue(Short.parseShort(obj.toString()));
                } else if ("id_Integer".equals(dcm.getType())) {
                    column.addColumnValue(Integer.parseInt(obj.toString()));
                } else if ("id_Long".equals(dcm.getType())) {
                    column.addColumnValue(Long.parseLong(obj.toString()));
                } else if ("id_Float".equals(dcm.getType())) {
                    column.addColumnValue(Float.parseFloat(obj.toString()));
                } else if ("id_Double".equals(dcm.getType())) {
                    column.addColumnValue(Double.parseDouble(obj.toString()));
                }
            } else {
                column.addColumnValue(rs.getObject(fixedColumnCount + i + 1));
            }
        }
    }

    public static void readColumnsFromDatabase_Mssql(Dynamic column, java.sql.ResultSet rs, int fixedColumnCount,
            java.util.List<String> list, boolean trim) throws Exception {
        column.clearColumnValues();
        for (int i = 0; i < column.getColumnCount(); i++) {
            DynamicMetadata dcm = column.getColumnMetadata(i);

            if ("id_String".equals(dcm.getType())) {
                if (DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId())
                        && "NTEXT".equals(dcm.getDbType().toUpperCase())) {
                    String value = list.get(0);
                    column.addColumnValue((trim && (value!=null)) ? value.trim() : value);
                    list.remove(0);
                } else {
                    String value = rs.getString(fixedColumnCount + i + 1);
                    column.addColumnValue((trim && (value!=null)) ? value.trim() : value);
                }
            } else if ("id_Date".equals(dcm.getType())) {
                if (DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(column.getDbmsId())
                        && !(dcm.getDbType().toLowerCase().indexOf("timestamp") < 0)) {
                    column.addColumnValue(rs.getString(fixedColumnCount + i + 1));
                } else {
                    column.addColumnValue(rs.getTimestamp(fixedColumnCount + i + 1));
                }
            } else if ("id_Integer".equals(dcm.getType()) || "id_Long".equals(dcm.getType()) || "id_Double".equals(dcm.getType())) {
                if (rs.getObject(fixedColumnCount + i + 1) == null) {
                    column.addColumnValue(null);
                    continue;
                }
                if ("id_Integer".equals(dcm.getType())) {
                    column.addColumnValue(rs.getInt(fixedColumnCount + i + 1));
                } else if ("id_Long".equals(dcm.getType())) {
                    column.addColumnValue(rs.getLong(fixedColumnCount + i + 1));
                } else if ("id_Double".equals(dcm.getType())) {
                    column.addColumnValue(rs.getDouble(fixedColumnCount + i + 1));
                }
            } else {
                column.addColumnValue(rs.getObject(fixedColumnCount + i + 1));
            }
        }
    }

    public static int writeColumnsToDatabse(Dynamic column, java.sql.PreparedStatement pstmt, int fixedColumnCount,
            String database) throws Exception {

        for (int i = 0; i < column.getColumnCount(); i++) {
            // DynamicColumnMetadata dcm = column.getColumnMetadata(i);
            Object value = column.getColumnValue(i);

            if (value == null) {
                if (DBMSConstants.SYBASE.getDBmsId().equalsIgnoreCase(database)) {
                    pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.VARCHAR);
                } else if (DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(database)
                        && "id_byte[]".equals(column.getColumnMetadata(i).getType())) {
                    pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.BINARY);
                } else if (DBMSConstants.TERADATA.getDBmsId().equalsIgnoreCase(database)
                        || DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(database)
                        || DBMSConstants.REDSHIFT.getDBmsId().equalsIgnoreCase(database)) {
                	if("id_String".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.VARCHAR);
                	} else if("id_Integer".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.INTEGER);
                	} else if("id_Long".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.BIGINT);
                	} else if("id_Short".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.SMALLINT);
                	} else if("id_Byte".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.TINYINT);
                	} else if("id_Date".equals(column.getColumnMetadata(i).getType())) {
                		pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.TIMESTAMP);
                	} else if ("id_Time".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.TIMESTAMP);
                	} else if ("id_Timestamp".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.TIMESTAMP);
                	} else if ("id_Float".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.FLOAT);
                    } else if ("id_Double".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.DOUBLE);
                    } else if ("id_BigDecimal".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setBigDecimal(fixedColumnCount + i + 1, null);
                    } else if ("id_Boolean".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.BOOLEAN);
                    } else if ("id_Character".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.CHAR);
                    } else {
                        pstmt.setNull(fixedColumnCount + i + 1, java.sql.Types.NULL);
                    }
                } else if (DBMSConstants.DB2.getDBmsId().equalsIgnoreCase(database)) {
                    if ("id_String".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.VARCHAR);
                    } else if ("id_Long".equals(column.getColumnMetadata(i).getType())
                            || "id_Integer".equals(column.getColumnMetadata(i).getType())
                            || "id_Short".equals(column.getColumnMetadata(i).getType())
                            || "id_Byte".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.INTEGER);
                    } else if ("id_Date".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.DATE);
                    } else if ("id_Float".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.FLOAT);
                    } else if ("id_Double".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.DOUBLE);
                    } else if ("id_BigDecimal".equals(column.getColumnMetadata(i).getType())) {
                        pstmt.setBigDecimal((fixedColumnCount + i + 1), null);
                    } else {
                        pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.NULL);
                    }
                } else {
                    pstmt.setNull((fixedColumnCount + i + 1), java.sql.Types.NULL);
                }
            } else if ("id_Integer".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setInt(fixedColumnCount + i + 1, Integer.valueOf(value.toString()));
            } else if ("id_String".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setString((fixedColumnCount + i + 1), String.valueOf(value));
            } else if ("id_Double".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setDouble((fixedColumnCount + i + 1), Double.valueOf(value.toString()));
            } else if ("id_Float".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setFloat((fixedColumnCount + i + 1), Float.valueOf(value.toString()));
            } else if ("id_Long".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setLong((fixedColumnCount + i + 1), Long.valueOf(value.toString()));
            } else if ("id_BigDecimal".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setBigDecimal((fixedColumnCount + i + 1), new java.math.BigDecimal(value.toString()));
            } else if ("id_Boolean".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setBoolean((fixedColumnCount + i + 1), Boolean.valueOf(value.toString()));
            } else if ("id_Byte".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setByte((fixedColumnCount + i + 1), Byte.valueOf(value.toString()));
            } else if ("id_Short".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setShort((fixedColumnCount + i + 1), Short.valueOf(value.toString()));
            } else if ("id_Date".equals(column.getColumnMetadata(i).getType())
                    || "id_Time".equals(column.getColumnMetadata(i).getType())
                    || "id_Timestamp".equals(column.getColumnMetadata(i).getType())) {
                if (value instanceof java.sql.Timestamp) {
                    pstmt.setTimestamp((fixedColumnCount + i + 1), (java.sql.Timestamp) value);
                } else {
                    String formatValue = formatDate(value, "yyyy-MM-dd HH:mm:ss.SSS");
                    pstmt.setTimestamp((fixedColumnCount + i + 1), java.sql.Timestamp.valueOf(formatValue));
                }
            } else if ("id_Blob".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setBlob((fixedColumnCount + i + 1), (java.sql.Blob) value);
            } else if ("id_Clob".equals(column.getColumnMetadata(i).getType())) {
                pstmt.setClob((fixedColumnCount + i + 1), (java.sql.Clob) value);
            } else if ("id_Character".equals(column.getColumnMetadata(i).getType())) {
                try {
                    pstmt.setObject((fixedColumnCount + i + 1), value);
                } catch (java.sql.SQLException e) {
                    pstmt.setString((fixedColumnCount + i + 1), String.valueOf(value));
                }
            } else {
                pstmt.setObject((fixedColumnCount + i + 1), value);
            }

        }
        return column.getColumnCount();
    }

    public static StringBuffer debugDynamicSql(StringBuffer query, Dynamic column, int fixedColumnCount, String[] sqlsplit)
            throws Exception {
        if (query.length() == 0) {
            query.append(sqlsplit[0]);
        }
        for (int i = 0; i < column.getColumnCount(); i++) {
            Object value = column.getColumnValue(i);
            String talendType = column.getColumnMetadata(i).getType();
            if (value == null) {
                query.append("null");
            } else if ("id_Character".equals(talendType)) {
                query.append("'" + String.valueOf(value) + "'");
            } else if ("id_String".equals(talendType)) {
                query.append("'" + value.toString() + "'");
            } else if ("id_Date".equals(talendType) || "id_Time".equals(talendType) || "id_Timestamp".equals(talendType)) {
                String formatValue = formatDate(value, column.getColumnMetadata(i).getFormat());
                query.append("'" + formatValue + "'");
            } else {
                query.append(String.valueOf(value));
            }
            if (i < column.getColumnCount()) {
                query.append(sqlsplit[fixedColumnCount + i]);
            }

        }
        return query;
    }

    public static String getCreateTableSQL(Dynamic column, String dbmsId) {
        DBManager manager = DBManagerFactory.getDBManager(dbmsId);
        String str = manager.getCreateTableSQL(column);
        return str;
    }

    public static String getInsertIntoStmtColumnsList(Dynamic column, String database) {
        DBManager manager = DBManagerFactory.getDBManager(database);
        String str = manager.getInsertTableSQL(column);
        return str;
    }

    public static String getInsertIntoStmtValuesList(Dynamic column) {
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < column.getColumnCount(); i++) {
            if (i < (column.getColumnCount() - 1)) {
                list.append("?,");
            } else {
                list.append("?");
            }
        }
        return list.toString();
    }
    
    public static String getInsertIntoStmtValuesList(Dynamic column, String database) {
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < column.getColumnCount(); i++) {
        	String sign = "?";
        	if(DBMSConstants.TERADATA.getDBmsId().equalsIgnoreCase(database)) {
        		if("id_Date".equals(column.getColumnMetadata(i).getType())) {
        			int dbTypeId = column.getColumnMetadata(i).getDbTypeId();//it only work for database source
        			if(dbTypeId == java.sql.Types.DATE) {
        				sign = "cast(cast(? as timestamp) as DATE)";
        			} else if(dbTypeId == java.sql.Types.TIME) {
        				sign = "cast(cast(? as timestamp) as TIME)";
        			}
        		}
        	}
            if (i < (column.getColumnCount() - 1)) {
                list.append(sign).append(",");
            } else {
                list.append(sign);
            }
        }
        return list.toString();
    }

    public static String getUpdateSet(Dynamic column, String dbmsId) {
        DBManager manager = DBManagerFactory.getDBManager(dbmsId);
        String updateSqlSet = manager.getUpdateSetSQL(column);
        return updateSqlSet;
    }
    
    public static String formatDate(Object value, String pattern) {
        if(value instanceof Long){
            return FormatterUtils.format_Date(new java.util.Date((Long)value), pattern);
        }else{
            return FormatterUtils.format_Date((java.util.Date) value, pattern);
        }
    }
}
