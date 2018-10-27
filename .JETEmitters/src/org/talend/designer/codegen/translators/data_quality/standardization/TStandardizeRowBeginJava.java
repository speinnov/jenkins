package org.talend.designer.codegen.translators.data_quality.standardization;

import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.INode;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import java.util.List;
import java.util.Map;

public class TStandardizeRowBeginJava
{
  protected static String nl;
  public static synchronized TStandardizeRowBeginJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TStandardizeRowBeginJava result = new TStandardizeRowBeginJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "routines.";
  protected final String TEXT_2 = " engine_";
  protected final String TEXT_3 = " = new routines.";
  protected final String TEXT_4 = "();" + NL + "engine_";
  protected final String TEXT_5 = ".getMatcher().setSearchUndefinedFields(";
  protected final String TEXT_6 = ");" + NL + "engine_";
  protected final String TEXT_7 = ".getMatcher().setSlopForPartialMatch(";
  protected final String TEXT_8 = ");" + NL + "engine_";
  protected final String TEXT_9 = ".getMatcher().setMaxEditsForFuzzyMatch(";
  protected final String TEXT_10 = ");" + NL + "engine_";
  protected final String TEXT_11 = ".getMatcher().setFormatXmlOutput(";
  protected final String TEXT_12 = ");" + NL;
  protected final String TEXT_13 = NL + "\tengine_";
  protected final String TEXT_14 = ".addMatchRule(";
  protected final String TEXT_15 = ", org.talend.dataquality.parser.match.Matcher.MatchType.";
  protected final String TEXT_16 = ", ";
  protected final String TEXT_17 = ", \"";
  protected final String TEXT_18 = "\");";
  protected final String TEXT_19 = NL + "engine_";
  protected final String TEXT_20 = ".preprocess();" + NL + "" + NL + "//org.talend.dataquality.parser.util.ParseError error_";
  protected final String TEXT_21 = " = engine_";
  protected final String TEXT_22 = ".error;" + NL + "//org.talend.dataquality.parser.util.ParseError error_";
  protected final String TEXT_23 = " = new org.talend.dataquality.parser.util.ParseError();";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
INode node = (INode)codeGenArgument.getArgument();
String cid = node.getUniqueName();

final String JOB_NAME =  node.getProcess().getName().toLowerCase();
final String COMPONENT_NAME = cid.toLowerCase();

final class StringUtils {
	public String capitalize(String input) {
		int strLen;
		
		if (input == null || (strLen = input.length()) == 0) {
			return input;
		}
		input = input.toLowerCase();
		
		return new StringBuffer(strLen)
			.append(Character.toTitleCase(input.charAt(0)))
			.append(input.substring(1))
			.toString(); 
	} 
}
StringUtils su = new StringUtils();
String javaClassName = su.capitalize(JOB_NAME) + su.capitalize(COMPONENT_NAME);
List<Map<String, String>> rules = (List<Map<String, String>>)ElementParameterParser.getObjectValue(node, "__RULE_TABLE__");

boolean searchUndefinedFields = Boolean.valueOf(ElementParameterParser.getValue(node, "__SEARCH_UNDEFINED_FIELDS__"));
//double minimumSimilarity = Double.valueOf(ElementParameterParser.getValue(node, "__MINIMUM_SIMILARITY__"));
String slop = ElementParameterParser.getValue(node, "__SLOP_FOR_PARTIAL_MATCH__");
String maxEdits = ElementParameterParser.getValue(node, "__MAX_EDITS_FOR_FUZZY_MATCH__");
boolean formatXmlOutput = Boolean.valueOf(ElementParameterParser.getValue(node, "__FORMAT_XML_OUTPUT__"));
        

    stringBuffer.append(TEXT_1);
    stringBuffer.append(javaClassName);
    stringBuffer.append(TEXT_2);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_3);
    stringBuffer.append(javaClassName);
    stringBuffer.append(TEXT_4);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_5);
    stringBuffer.append(searchUndefinedFields);
    stringBuffer.append(TEXT_6);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_7);
    stringBuffer.append(slop);
    stringBuffer.append(TEXT_8);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_9);
    stringBuffer.append(maxEdits);
    stringBuffer.append(TEXT_10);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(formatXmlOutput);
    stringBuffer.append(TEXT_12);
    
for (Map<String, String> rule : rules) {
	String ruleName = rule.get("RULE_NAME");
	String ruleType = rule.get("RULE_TYPE");
	String ruleValue = rule.get("RULE_VALUE");	
	String searchMode = rule.get("SEARCH_MODE");

    stringBuffer.append(TEXT_13);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(ruleName);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(ruleType);
    stringBuffer.append(TEXT_16);
    stringBuffer.append(ruleValue);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(searchMode);
    stringBuffer.append(TEXT_18);
    
}
    stringBuffer.append(TEXT_19);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_22);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_23);
    return stringBuffer.toString();
  }
}
