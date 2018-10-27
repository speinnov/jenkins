package org.talend.designer.codegen.translators.file.hadoop;

import org.talend.core.model.process.INode;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.designer.codegen.config.CodeGeneratorArgument;

public class THDFSPutMainJava
{
  protected static String nl;
  public static synchronized THDFSPutMainJava create(String lineSeparator)
  {
    nl = lineSeparator;
    THDFSPutMainJava result = new THDFSPutMainJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = "\t      \t\t\t\t" + NL + "   \t\tjava.util.Set<String> keySet_";
  protected final String TEXT_3 = " = map_";
  protected final String TEXT_4 = ".keySet();" + NL + "      \tfor (String key_";
  protected final String TEXT_5 = " : keySet_";
  protected final String TEXT_6 = "){     " + NL + "\t\t\tString tempdir_";
  protected final String TEXT_7 = " =  localdir_";
  protected final String TEXT_8 = ";" + NL + "\t\t\tString filemask_";
  protected final String TEXT_9 = " = key_";
  protected final String TEXT_10 = "; " + NL + "\t\t\tString dir_";
  protected final String TEXT_11 = " = null;\t";
  protected final String TEXT_12 = NL + "                String mask_";
  protected final String TEXT_13 = " = filemask_";
  protected final String TEXT_14 = ".replaceAll(\"\\\\\\\\\", \"/\") ;";
  protected final String TEXT_15 = NL + "                String mask_";
  protected final String TEXT_16 = " = filemask_";
  protected final String TEXT_17 = ";";
  protected final String TEXT_18 = "   " + NL + "\t\t\tint i_";
  protected final String TEXT_19 = " = mask_";
  protected final String TEXT_20 = ".lastIndexOf('/');" + NL + "  \t\t\tif (i_";
  protected final String TEXT_21 = "!=-1){" + NL + "\t\t\t\tdir_";
  protected final String TEXT_22 = " = mask_";
  protected final String TEXT_23 = ".substring(0, i_";
  protected final String TEXT_24 = "); " + NL + "\t\t\t\tmask_";
  protected final String TEXT_25 = " = mask_";
  protected final String TEXT_26 = ".substring(i_";
  protected final String TEXT_27 = "+1);\t " + NL + "    \t\t}" + NL + "    \t\tif (dir_";
  protected final String TEXT_28 = "!=null && !\"\".equals(dir_";
  protected final String TEXT_29 = ")) tempdir_";
  protected final String TEXT_30 = " = tempdir_";
  protected final String TEXT_31 = " + \"/\" + dir_";
  protected final String TEXT_32 = ";  ";
  protected final String TEXT_33 = NL + "                mask_";
  protected final String TEXT_34 = " = mask_";
  protected final String TEXT_35 = ".replaceAll(\"\\\\.\", \"\\\\\\\\.\").replaceAll(\"\\\\*\", \".*\");";
  protected final String TEXT_36 = " " + NL + "    \t\tfinal String finalMask_";
  protected final String TEXT_37 = " = mask_";
  protected final String TEXT_38 = ";" + NL + "    \t\tjava.io.File[] listings_";
  protected final String TEXT_39 = " = null;       " + NL + "        \tjava.io.File file_";
  protected final String TEXT_40 = " = new java.io.File(tempdir_";
  protected final String TEXT_41 = ");" + NL + "        \tif (file_";
  protected final String TEXT_42 = ".isDirectory()) {" + NL + "\t            listings_";
  protected final String TEXT_43 = " = file_";
  protected final String TEXT_44 = ".listFiles(new java.io.FileFilter() {" + NL + "\t                public boolean accept(java.io.File pathname) {" + NL + "\t                    boolean result = false;" + NL + "\t                    if (pathname != null && pathname.isFile()) {                      " + NL + "\t                            result = java.util.regex.Pattern.compile(finalMask_";
  protected final String TEXT_45 = ").matcher(pathname.getName()).find(); " + NL + "\t                    \t}" + NL + "\t                    return result;" + NL + "\t                }" + NL + "\t            });" + NL + "        \t} " + NL + "\t    \tif(listings_";
  protected final String TEXT_46 = " == null || listings_";
  protected final String TEXT_47 = ".length <= 0){" + NL + "\t    \t\tSystem.err.println(\"No match file(\"+key_";
  protected final String TEXT_48 = "+\") exists!\");" + NL + "\t    \t\t";
  protected final String TEXT_49 = NL + "       \t\t\tlog.error(\"";
  protected final String TEXT_50 = " - No match file(\"+key_";
  protected final String TEXT_51 = "+\") exists!\");" + NL + "        \t\t";
  protected final String TEXT_52 = NL + "\t    \t}else{" + NL + "\t    \t\tString localFilePath_";
  protected final String TEXT_53 = " = \"\";" + NL + "\t    \t\tString hdfsFilePath_";
  protected final String TEXT_54 = " = \"\";" + NL + "\t    \t\tfor (int m_";
  protected final String TEXT_55 = " = 0; m_";
  protected final String TEXT_56 = " < listings_";
  protected final String TEXT_57 = ".length; m_";
  protected final String TEXT_58 = "++){ " + NL + "\t     \t\t\tif (listings_";
  protected final String TEXT_59 = "[m_";
  protected final String TEXT_60 = "].getName().matches(mask_";
  protected final String TEXT_61 = ")){    " + NL + "\t     \t\t\t\tlocalFilePath_";
  protected final String TEXT_62 = " = listings_";
  protected final String TEXT_63 = "[m_";
  protected final String TEXT_64 = "].getAbsolutePath();" + NL + "\t\t\t\t\t\thdfsFilePath_";
  protected final String TEXT_65 = " = ";
  protected final String TEXT_66 = "+\"/\"+map_";
  protected final String TEXT_67 = ".get(key_";
  protected final String TEXT_68 = "); " + NL + "\t\t\t\t\t\ttry{" + NL + "\t\t\t\t\t\t\tfs_";
  protected final String TEXT_69 = ".copyFromLocalFile(false, ";
  protected final String TEXT_70 = ", new org.apache.hadoop.fs.Path(localFilePath_";
  protected final String TEXT_71 = "), new org.apache.hadoop.fs.Path(hdfsFilePath_";
  protected final String TEXT_72 = "));" + NL + "\t\t    \t\t\t\t// add info to list will return" + NL + "\t\t    \t\t\t\tmsg_";
  protected final String TEXT_73 = ".add(\"file: \" + listings_";
  protected final String TEXT_74 = "[m_";
  protected final String TEXT_75 = "].getAbsolutePath() + \", size: \"" + NL + "\t\t                    \t+ listings_";
  protected final String TEXT_76 = "[m_";
  protected final String TEXT_77 = "].length() + \" bytes upload successfully\");" + NL + "\t\t                   " + NL + "\t\t    \t\t\t\tnb_success_";
  protected final String TEXT_78 = "++;" + NL + "\t\t    \t\t\t}catch(java.io.IOException e) {" + NL + "\t\t                \t";
  protected final String TEXT_79 = NL + "            \t\t\t\tthrow(e);" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_80 = NL + "\t\t\t\t\t\t\tSystem.err.print(e.getMessage());" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_81 = NL + "       \t\t\t\t\t\tlog.error(\"";
  protected final String TEXT_82 = " - \" + e.getMessage());" + NL + "        \t\t\t\t\t";
  protected final String TEXT_83 = NL + "\t\t                }" + NL + "\t\t\t\t\t\tnb_file_";
  protected final String TEXT_84 = "++;" + NL + "\t      \t\t\t}" + NL + "\t    \t\t}\t" + NL + "\t    \t}" + NL + "\t    }";
  protected final String TEXT_85 = NL;

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
	INode node = (INode)codeGenArgument.getArgument();
	String cid = node.getUniqueName();
	
	String localdir = ElementParameterParser.getValue(node, "__LOCALDIR__");
	String dieOnError = ElementParameterParser.getValue(node, "__DIE_ON_ERROR__");
	boolean overwrite = ("true").equals(ElementParameterParser.getValue(node, "__OVERWRITE__"));
	String remotedir = ElementParameterParser.getValue(node, "__REMOTEDIR__");
	boolean append = ("true").equals(ElementParameterParser.getValue(node, "__APPEND__"));
	boolean useGlob = ("true").equals(ElementParameterParser.getValue(node, "__PERL5_REGEX__"));
	
	boolean isLog4jEnabled = ("true").equals(ElementParameterParser.getValue(node.getProcess(), "__LOG4J_ACTIVATE__"));

    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_3);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_5);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_7);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_9);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_11);
    if(!useGlob) { //perl5 mode does not support windows (\) path separator in the mask string
            
    stringBuffer.append(TEXT_12);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_14);
    } else {
    stringBuffer.append(TEXT_15);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_16);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_17);
    }
    stringBuffer.append(TEXT_18);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_23);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_24);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_25);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_26);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_27);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_28);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_29);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_30);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_31);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_32);
    if(!useGlob) {
    stringBuffer.append(TEXT_33);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_34);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_35);
    }
    stringBuffer.append(TEXT_36);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_37);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_38);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_39);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_40);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_41);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_42);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_43);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_44);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_45);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_46);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_47);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_48);
    if(isLog4jEnabled) {
    stringBuffer.append(TEXT_49);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_51);
    }
    stringBuffer.append(TEXT_52);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_53);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_54);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_55);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_56);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_57);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_58);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_59);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_60);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_61);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_63);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_64);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(remotedir);
    stringBuffer.append(TEXT_66);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_67);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_68);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_69);
    stringBuffer.append(overwrite);
    stringBuffer.append(TEXT_70);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_71);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_72);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_73);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_74);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_75);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_76);
    stringBuffer.append(cid );
    stringBuffer.append(TEXT_77);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_78);
    
  						      	if(("true").equals(dieOnError)){
							
    stringBuffer.append(TEXT_79);
    
								}else{
							
    stringBuffer.append(TEXT_80);
       
									if(isLog4jEnabled) {
							
    stringBuffer.append(TEXT_81);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_82);
    
        							}
								}
						    
    stringBuffer.append(TEXT_83);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(TEXT_85);
    return stringBuffer.toString();
  }
}
