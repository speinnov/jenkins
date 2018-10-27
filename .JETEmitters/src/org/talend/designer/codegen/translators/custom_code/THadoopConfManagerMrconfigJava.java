package org.talend.designer.codegen.translators.custom_code;

import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.runprocess.ProcessorUtilities;
import java.util.List;

public class THadoopConfManagerMrconfigJava
{
  protected static String nl;
  public static synchronized THadoopConfManagerMrconfigJava create(String lineSeparator)
  {
    nl = lineSeparator;
    THadoopConfManagerMrconfigJava result = new THadoopConfManagerMrconfigJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "        routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();" + NL + "        java.net.URLClassLoader currentLoader";
  protected final String TEXT_3 = " = (java.net.URLClassLoader) Thread.currentThread().getContextClassLoader();" + NL + "        java.lang.reflect.Method method_";
  protected final String TEXT_4 = " = java.net.URLClassLoader.class.getDeclaredMethod(\"addURL\", new Class[] { java.net.URL.class });" + NL + "        method_";
  protected final String TEXT_5 = ".setAccessible(true);" + NL + "" + NL + "        String confJarName = null;" + NL + "        String confLib = ";
  protected final String TEXT_6 = ";" + NL + "        if (this.contextStr != null && this.contextStr.length() > 0) {" + NL + "            int lastDotIndex = confLib.lastIndexOf(\".\");" + NL + "            String jarName = confLib.substring(0, lastDotIndex);" + NL + "            String jarExt = confLib.substring(lastDotIndex);" + NL + "            confJarName = jarName + \"_\" + this.contextStr + jarExt;" + NL + "        }" + NL + "        if(confJarName != null) {";
  protected final String TEXT_7 = NL + "                String libPath_";
  protected final String TEXT_8 = " = getJarsToRegister.replaceJarPaths(\"../lib/\" + confJarName);";
  protected final String TEXT_9 = NL + "                String libPath_";
  protected final String TEXT_10 = " = getJarsToRegister.replaceJarPaths(new java.io.File(\"";
  protected final String TEXT_11 = "/\" + confJarName).getAbsolutePath());";
  protected final String TEXT_12 = " log.info(\"Loading of the custom hadoop configuration '\" + libPath_";
  protected final String TEXT_13 = " + \"'...\"); ";
  protected final String TEXT_14 = NL + "            method_";
  protected final String TEXT_15 = ".invoke(currentLoader";
  protected final String TEXT_16 = ", new Object[] { new java.io.File(libPath_";
  protected final String TEXT_17 = ").toURL() });" + NL + "        }";
  protected final String TEXT_18 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    INode node = (INode)codeGenArgument.getArgument();
    String cid = node.getUniqueName();
    String confLib = ElementParameterParser.getValue(node, "__CONF_LIB__");
    boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));
 
    if(confLib != null && confLib.length() > 0) {

    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(confLib );
    stringBuffer.append(TEXT_6);
    if (ProcessorUtilities.isExportConfig()) {
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    } else {
                String libFolder = ProcessorUtilities.getJavaProjectLibFolder().getAbsolutePath().replace("\\", "/");
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(libFolder );
    stringBuffer.append(TEXT_11);
    }
     if (isLog4jEnabled) { 
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
     } 
    stringBuffer.append(TEXT_14);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    
    }

    stringBuffer.append(TEXT_18);
    return stringBuffer.toString();
  }
}
