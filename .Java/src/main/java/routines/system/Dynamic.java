package routines.system;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Dynamic implements Cloneable, java.io.Serializable {

    private static final long serialVersionUID = 7990658608074365829L;

    public List<DynamicMetadata> metadatas;

    private List<Object> values = new ArrayList<Object>(30);

    private String dbmsId = "";

    // private static String[][] dbMapping = { { "VARCHAR", "id_String" }, { "BIGINT", "id_Long" }, { "INTEGER",
    // "id_Integer" },
    // { "DOUBLE", "id_Double" }, { "DATETIME", "id_Date" } };

    // private constructor for internal/static use only
    public Dynamic() {
        this.metadatas = new ArrayList<DynamicMetadata>();
    }

    public void setDbmsId(String dbmsId) {
        this.dbmsId = dbmsId;
    }

    public String getDbmsId() {
        return this.dbmsId;
    }

    public int getColumnCount() {
        return this.metadatas.size();
    }

    public DynamicMetadata getColumnMetadata(int index) {
        return this.metadatas.get(index);
    }

    public int getIndex(String columnName) {
        for (int i = 0; i < this.getColumnCount(); i++) {
            if (this.metadatas.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public Object getColumnValue(int index) {
        if (index < this.metadatas.size()) {
            return values.get(index);
        }
        return null;
    }

    public Object getColumnValue(String columnName) {
        for (int i = 0; i < this.getColumnCount(); i++) {
            if (this.metadatas.get(i).getName().equals(columnName)) {
                return this.getColumnValue(i);
            }
        }
        return null;
    }

    public void addColumnValue(Object value) {
        if (values.size() < metadatas.size())
            values.add(value);
    }

    public void setColumnValue(int index, Object value) {
        if (index < this.metadatas.size()) {
            modifyColunmValue(index, value);
        }
    }

    /**
     * Need to replace the element if the index already exists or add the new element if it does not exist yet.
     * 
     * @param index
     * @param value
     */
    private void modifyColunmValue(int index, Object value) {
    	
        if (index < values.size()) {
        	values.set(index, value);
        } else if (index < this.metadatas.size()) {
        	for (int i = values.size(); i < index;i++) {
        		values.add(null);
        	}
        	values.add(value);
        }
    }
    
    public void clearColumnValues() {
        values.clear();
    }

    public void writeValuesToStream(java.io.OutputStream out, String delimiter) throws java.io.IOException {
        for (int i = 0; i < metadatas.size(); i++) {
            out.write((String.valueOf(values.get(i))).getBytes());
            if (i != (metadatas.size() - 1))
                out.write(delimiter.getBytes());
        }
        out.flush();
    }

    public void writeHeaderToStream(java.io.OutputStream out, String delimiter) throws java.io.IOException {
        for (int i = 0; i < metadatas.size(); i++) {
            out.write((String.valueOf(metadatas.get(i).getName())).getBytes());
            if (i != (metadatas.size() - 1))
                out.write(delimiter.getBytes());
        }
        out.flush();
    }

    public static String getTalendTypeFromDBType(String dbName, String dbType, int length, int precision) {
        // TODO:: replace this fucntion with full implementation with XML mappings
        String talendType = null;
        if(dbType.contains("(")) {
        	dbType = dbType.substring(0,dbType.indexOf('('));
        }

        talendType = MetadataTalendType.getDefaultSelectedTalendType(dbName, dbType, length, precision);
        if (talendType == null) {
            talendType = "id_String";
        }

        return talendType;
    }

    /**
     * Comment method "getDBTypeFromTalendType"<br>
     * generate and return a string composed of DB type, length and precision, such
     * as:int(4),String(200),DATE,DECIMAL(20,4) .
     * 
     * @author talend
     * @param dbmsId
     * @param talendType
     * @param length
     * @param precision
     * @return
     */
    public static String getDBTypeFromTalendType(String dbmsId, String talendType, int length, int precision) {

        String dbmsType = MetadataTalendType.getDefaultSelectedDbType(dbmsId, talendType, length, precision);

        StringBuilder returnResult = new StringBuilder(dbmsType);

        boolean isIgnoreLen = ("true").equals(MetadataTalendType.getDefaultDBTypes(dbmsId, dbmsType,
                MetadataTalendType.IGNORE_LEN));
        String defaultLength = MetadataTalendType.getDefaultDBTypes(dbmsId, dbmsType, MetadataTalendType.DEFAULT_LENGTH);
        // generate a string like this:(100,3)
        if (!isIgnoreLen && defaultLength != null) {
            returnResult.append("(");
            if (length > 0) {
                returnResult.append(length);
            } else {// use the default length of the DB type
                returnResult.append(defaultLength);
            }
            boolean isIgnorePre = ("true").equals(MetadataTalendType.getDefaultDBTypes(dbmsId, dbmsType,
                    MetadataTalendType.IGNORE_PRE));
            String defaultPre = MetadataTalendType.getDefaultDBTypes(dbmsId, dbmsType, MetadataTalendType.DEFAULT_PRECISION);
            if (!isIgnorePre && defaultPre != null) {
                returnResult.append(",");
                if (precision > 0) {
                    returnResult.append(precision);
                } else {// use the default precision of the db type
                    returnResult.append(defaultPre);
                }
            }
            returnResult.append(")");
        }
        return returnResult.toString();

    }

    public String toString() {
        return this.toString(" - ");
    }
    
    public String toString(String delimiter) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < metadatas.size(); i++) {
            DynamicMetadata metadata = metadatas.get(i);
            if("id_Date".equals(metadata.getType()) && !(DBMSConstants.MSSQL.getDBmsId().equalsIgnoreCase(getDbmsId())
                        && !(metadata.getDbType().toLowerCase().indexOf("timestamp") < 0))) { 
                out.append(DynamicUtils.formatDate(values.get(i),metadata.getFormat()));
            } else {
                out.append((String.valueOf(values.get(i))));
            }
            if (i != (metadatas.size() - 1))
                out.append(delimiter);
        }
        return out.toString();
    }

    public int hashCode() {
        return this.values.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass())
            return false;
        boolean b = true;
        Dynamic D = (Dynamic) obj;
        if (this.metadatas.size() != D.metadatas.size()) {
            b = false;
        } else {
            for (int i = 0; i < this.metadatas.size(); i++) {
                if (!(this.metadatas.get(i).equals(D.metadatas.get(i)))) {
                    b = false;
                }
            }
        }
        if (!b) {
            return b;
        }
        if (this.values.size() != D.values.size()) {
            b = false;
        } else {
            b = this.values.equals(D.values);
        }
        return b;
    }

    public Dynamic clone() {
        Dynamic dynamic = new Dynamic();
        dynamic.dbmsId = this.dbmsId;
        for (int i = 0; i < this.metadatas.size(); i++) {
            dynamic.metadatas.add(this.metadatas.get(i));
        }
        for (int j = 0; j < this.values.size(); j++) {
            dynamic.values.add(values.get(j));
        }
        return dynamic;
    }

    public boolean contains(String columnName) {

        for (int i = 0; i < this.getColumnCount(); i++) {
            if (columnName.equals(this.getColumnMetadata(i).getName())) {
                return true;
            }
        }
        return false;
    }

    public routines.system.Dynamic copy() {

        routines.system.Dynamic dynamicTarget = new routines.system.Dynamic();

        dynamicTarget.dbmsId = this.dbmsId;

        for (int i = 0; i < this.getColumnCount(); i++) {
            dynamicTarget.metadatas.add(this.metadatas.get(i));
            dynamicTarget.addColumnValue(this.getColumnValue(i));
        }
        return dynamicTarget;
    }

    public routines.system.Dynamic merge(routines.system.Dynamic dynamicSource) {
        routines.system.Dynamic dynamicTarget = new routines.system.Dynamic();
        dynamicTarget = this.copy();

        for (int i = 0; i < dynamicSource.getColumnCount(); i++) {
            if (!this.contains(dynamicSource.metadatas.get(i).getName()))
                dynamicTarget.metadatas.add(dynamicSource.metadatas.get(i));
            dynamicTarget.addColumnValue(dynamicSource.getColumnValue(i));
        }
        return dynamicTarget;
    }

    public void removeDynamicElement(String columnName) {
        if (columnName != null) {
            for (int i = 0; i < this.getColumnCount(); i++) {
                if (columnName.equals(this.metadatas.get(i).getName())) {
                    this.metadatas.remove(i);
                    this.values.remove(i);
                }
            }
        }
    }

    public void removeDynamicElement(int index) {
        if (index < this.getColumnCount()) {
            this.metadatas.remove(index);
            this.values.remove(index);
        }
    }
    
}
