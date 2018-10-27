package org.talend.designer.codegen.translators.databases.cassandra;

public class TCassandraInputSparkcodeJava
{
  protected static String nl;
  public static synchronized TCassandraInputSparkcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TCassandraInputSparkcodeJava result = new TCassandraInputSparkcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    return stringBuffer.toString();
  }
}
