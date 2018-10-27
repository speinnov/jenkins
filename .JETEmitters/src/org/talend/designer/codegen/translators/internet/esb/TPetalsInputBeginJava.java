package org.talend.designer.codegen.translators.internet.esb;

import org.talend.core.model.process.INode;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import java.util.List;

public class TPetalsInputBeginJava
{
  protected static String nl;
  public static synchronized TPetalsInputBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TPetalsInputBeginJava result = new TPetalsInputBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = NL + "int nb_line_";
  protected final String TEXT_2 = " = 0;" + NL + "" + NL + "// Convert the input document into a Talend data flow" + NL + "if( petalsInMessage != null " + NL + "\t\t&& petalsInMessage.getDocumentElement() != null ) {" + NL + "" + NL + "\torg.w3c.dom.NodeList petalsRows = petalsInMessage.getDocumentElement().getElementsByTagNameNS( \"*\", \"in-data-bean\" );" + NL + "\tfor( int i=0; i<petalsRows.getLength(); i++ ) {" + NL + "\t\torg.w3c.dom.Element elt = (org.w3c.dom.Element) petalsRows.item( i );" + NL + "\t\tnb_line_";
  protected final String TEXT_3 = " ++; " + NL + "" + NL + "\t\torg.w3c.dom.NodeList children = null;" + NL + "\t\torg.w3c.dom.Element columnElt = null;" + NL + "\t\tString value = null;";
  protected final String TEXT_4 = "        \t\t" + NL + "        \tchildren = elt.getElementsByTagNameNS( \"*\", \"";
  protected final String TEXT_5 = "\" ); " + NL + "        \tcolumnElt = null;" + NL + "        \tif( children.getLength() > 0 )" + NL + "        \t\tcolumnElt = (org.w3c.dom.Element) children.item( 0 );" + NL + "        " + NL + "        \tvalue = null;" + NL + "        \tif( columnElt != null ) {" + NL + "        \t\tvalue = \t\"true\".equals( columnElt.getAttributeNS( \"http://www.w3.org/2001/XMLSchema-instance\", \"nil\" )) " + NL + "\t\t\t\t\t\t\t? null : columnElt.getTextContent().trim();" + NL + "\t\t\t}";
  protected final String TEXT_6 = NL + "\t\tif( value == null ) { " + NL + "\t\t\t";
  protected final String TEXT_7 = ".";
  protected final String TEXT_8 = " = ";
  protected final String TEXT_9 = ";" + NL + "\t\t} else if( value.length() == 0 ) {" + NL + "\t\t\t";
  protected final String TEXT_10 = ".";
  protected final String TEXT_11 = " = \"\";" + NL + "\t\t} else {";
  protected final String TEXT_12 = "\t" + NL + "\t\tif( value == null || value.length() == 0 ) {" + NL + "\t\t\t";
  protected final String TEXT_13 = ".";
  protected final String TEXT_14 = " = ";
  protected final String TEXT_15 = ";" + NL + "\t\t} else {";
  protected final String TEXT_16 = NL + "\t\tif( value == null || value.length() == 0 ) {" + NL + "\t\t\t";
  protected final String TEXT_17 = ".";
  protected final String TEXT_18 = " = ";
  protected final String TEXT_19 = ";" + NL + "\t\t} else {";
  protected final String TEXT_20 = NL + "\t\t\t";
  protected final String TEXT_21 = ".";
  protected final String TEXT_22 = " = value;";
  protected final String TEXT_23 = NL + "\t\t\t";
  protected final String TEXT_24 = ".";
  protected final String TEXT_25 = " = new java.util.Date( Long.valueOf( value ));";
  protected final String TEXT_26 = "\t\t\t\t\t\t\t" + NL + "\t\t\t";
  protected final String TEXT_27 = ".";
  protected final String TEXT_28 = " = value.getBytes( \"UTF-8\" );\t// Petals propagates all the messages with UTF-8 encoding";
  protected final String TEXT_29 = NL + "\t\t\t";
  protected final String TEXT_30 = ".";
  protected final String TEXT_31 = " = ParserUtils.parseTo_";
  protected final String TEXT_32 = "(ParserUtils.parseTo_Number(value, ',', '.'));";
  protected final String TEXT_33 = NL + "\t\t\t";
  protected final String TEXT_34 = ".";
  protected final String TEXT_35 = " = ParserUtils.parseTo_";
  protected final String TEXT_36 = "(value);";
  protected final String TEXT_37 = NL + "\t\t}";
  protected final String TEXT_38 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
     
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode) codeGenArgument.getArgument();
    String cid = node.getUniqueName();	
    
    // Principle: if the input message is not null...
    // Get all the rows. For every schema column, get the associated value in the row.
    // If the value is null or is not defined:
    //	Nullable => null
    //	Otherwise, get a default value (column's default value or type-based default value)
    // Otherwise, adapt it

    stringBuffer.append(TEXT_1);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    
	List<IMetadataTable> metadatas = node.getMetadataList();
	if( metadatas != null 
			&& metadatas.size() > 0
			&& metadatas.get( 0 ) != null ) {
			
    	List<IMetadataColumn> columns = metadatas.get( 0 ).getListColumns();
    	for( IConnection conn : node.getOutgoingConnections()) {
        	if( ! conn.getLineStyle().hasConnectionCategory( IConnectionCategory.DATA ))
        		continue;
        		
        	for( IMetadataColumn column : columns ) {

    stringBuffer.append(TEXT_4);
    stringBuffer.append( column.getLabel() );
    stringBuffer.append(TEXT_5);
    
				String typeToGenerate = JavaTypesManager.getTypeToGenerate( column.getTalendType(), column.isNullable());
				JavaType javaType = JavaTypesManager.getJavaTypeFromId( column.getTalendType());
				String patternValue = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
		
				boolean primitive = JavaTypesManager.isJavaPrimitiveType( column.getTalendType(), column.isNullable());
				String defaultValue = column.getDefault();
				if( defaultValue == null )
					defaultValue = JavaTypesManager.getDefaultValueFromJavaIdType( column.getTalendType(), column.isNullable());
		
				// Case: String
				if(javaType == JavaTypesManager.STRING) {

    stringBuffer.append(TEXT_6);
    stringBuffer.append( conn.getName() );
    stringBuffer.append(TEXT_7);
    stringBuffer.append( column.getLabel() );
    stringBuffer.append(TEXT_8);
    stringBuffer.append( column.isNullable() ? "null" : (defaultValue != null ? defaultValue : "") );
    stringBuffer.append(TEXT_9);
    stringBuffer.append( conn.getName() );
    stringBuffer.append(TEXT_10);
    stringBuffer.append( column.getLabel() );
    stringBuffer.append(TEXT_11);
    
				} 
				// Case: other type
				else {
					if(column.isNullable()) {

    stringBuffer.append(TEXT_12);
    stringBuffer.append( conn.getName() );
    stringBuffer.append(TEXT_13);
    stringBuffer.append( column.getLabel() );
    stringBuffer.append(TEXT_14);
    stringBuffer.append( primitive ? defaultValue : null );
    stringBuffer.append(TEXT_15);
    
			  		} else { 

    stringBuffer.append(TEXT_16);
    stringBuffer.append( conn.getName() );
    stringBuffer.append(TEXT_17);
    stringBuffer.append( column.getLabel() );
    stringBuffer.append(TEXT_18);
    stringBuffer.append( defaultValue );
    stringBuffer.append(TEXT_19);
    
					}
				}	// Enf of case "other type"
									
				if( javaType == JavaTypesManager.STRING || javaType == JavaTypesManager.OBJECT ) {

    stringBuffer.append(TEXT_20);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_22);
    
				} else if( javaType == JavaTypesManager.DATE ) {

    stringBuffer.append(TEXT_23);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_24);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_25);
      
				} else if( javaType == JavaTypesManager.BYTE_ARRAY ) { 

    stringBuffer.append(TEXT_26);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_28);
    
				} else if( JavaTypesManager.isNumberType(javaType)) {

    stringBuffer.append(TEXT_29);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_30);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_31);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_32);
    
				} else {

    stringBuffer.append(TEXT_33);
    stringBuffer.append(conn.getName() );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_35);
    stringBuffer.append( typeToGenerate );
    stringBuffer.append(TEXT_36);
    
				}

    stringBuffer.append(TEXT_37);
    
       	}	// End of loop
		}	// End of loop
	}	// End of condition (metadata != null)

    stringBuffer.append(TEXT_38);
    return stringBuffer.toString();
  }
}
