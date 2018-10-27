package org.talend.designer.codegen.translators.common;

import org.talend.designer.codegen.config.NodesSubTree;
import org.talend.core.model.process.INode;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Vector;
import org.talend.core.model.process.IProcess;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.designer.codegen.config.CodeGeneratorArgument;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IHashableInputConnections;
import org.talend.core.model.process.IHashConfiguration;
import org.talend.core.model.process.IHashableColumn;
import org.talend.core.model.utils.NodeUtil;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.utils.TalendQuoteUtils;

public class Mr_subprocess_headerJava
{
  protected static String nl;
  public static synchronized Mr_subprocess_headerJava create(String lineSeparator)
  {
    nl = lineSeparator;
    Mr_subprocess_headerJava result = new Mr_subprocess_headerJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "\t";
  protected final String TEXT_2 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_3 = "\", 1);" + NL + "\t\t";
  protected final String TEXT_4 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_5 = "\", -1);" + NL + "\t\t";
  protected final String TEXT_6 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_7 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_8 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_10 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_11 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_12 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t";
  protected final String TEXT_13 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_14 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_15 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_16 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_17 = " && !null2_";
  protected final String TEXT_18 = "){" + NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_19 = " && null2_";
  protected final String TEXT_20 = "){" + NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_21 = " && null2_";
  protected final String TEXT_22 = "){" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_23 = "\t\t" + NL + "            \t";
  protected final String TEXT_24 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_25 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_26 = NL + "\t            \tpos1 += length1_";
  protected final String TEXT_27 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_28 = ";" + NL + "\t            ";
  protected final String TEXT_29 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_30 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_31 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_32 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_33 = NL + "\t                pos1 += (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 4; " + NL + "\t                pos2 += (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 4; " + NL + "\t            ";
  protected final String TEXT_34 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_35 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_36 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_37 = NL + "\t            ";
  protected final String TEXT_38 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_39 = NL + "\t            \tpos1 += 4;" + NL + "\t            \tpos1 += readInt(b1, pos1);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tpos2 += readInt(b2, pos2);" + NL + "\t            ";
  protected final String TEXT_40 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_41 = NL + "\t            ";
  protected final String TEXT_42 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_43 = NL + "\t            ";
  protected final String TEXT_44 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_45 = NL + "\t            ";
  protected final String TEXT_46 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_47 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_48 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_49 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t\tboolean null1_";
  protected final String TEXT_50 = " = length1_";
  protected final String TEXT_51 = " == -1;" + NL + "\t\t\t\t\tboolean null2_";
  protected final String TEXT_52 = " = length2_";
  protected final String TEXT_53 = " == -1;" + NL + "\t\t\t\t";
  protected final String TEXT_54 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_55 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_56 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_57 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_58 = " && !null2_";
  protected final String TEXT_59 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_60 = NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_61 = " && null2_";
  protected final String TEXT_62 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_63 = NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_64 = " && null2_";
  protected final String TEXT_65 = "){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_66 = "\t\t" + NL + "            \t";
  protected final String TEXT_67 = NL + "\t        \t\tif(b1[pos1] > 0 && b2[pos2] ==0){" + NL + "\t        \t\t\t";
  protected final String TEXT_68 = NL + "\t        \t\t}else if(b1[pos1] == 0 && b2[pos2] > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_69 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_70 = NL + "\t                if(b1[pos1] > b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_71 = NL + "\t        \t\t}else if(b1[pos1] < b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_72 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_73 = NL + "\t            \tint n_";
  protected final String TEXT_74 = " = Math.min(length1_";
  protected final String TEXT_75 = ", length2_";
  protected final String TEXT_76 = ");" + NL + "\t            \tfor(int i = 0; i < n_";
  protected final String TEXT_77 = "; i++){" + NL + "\t            \t\tif(b1[pos1+i] > b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_78 = NL + "\t            \t\t}else if(b1[pos1+i] < b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_79 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += length1_";
  protected final String TEXT_80 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_81 = ";" + NL + "\t            \tif(length1_";
  protected final String TEXT_82 = " > length2_";
  protected final String TEXT_83 = "){" + NL + "\t            \t\t";
  protected final String TEXT_84 = NL + "\t            \t}else if(length1_";
  protected final String TEXT_85 = " < length2_";
  protected final String TEXT_86 = "){" + NL + "\t            \t\t";
  protected final String TEXT_87 = NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_88 = NL + "\t            \tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) != 0){" + NL + "\t            \t\tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_89 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_90 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_91 = NL + "\t            \tlong v1_";
  protected final String TEXT_92 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_93 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_94 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_95 = " = FormatterUtils.format_DateInUTC(new java.util.Date(v1_";
  protected final String TEXT_96 = "), ";
  protected final String TEXT_97 = ").compareTo(FormatterUtils.format_DateInUTC(new java.util.Date(v2_";
  protected final String TEXT_98 = "), ";
  protected final String TEXT_99 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_100 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_101 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_102 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_103 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_104 = NL + "\t\t                if(v1_";
  protected final String TEXT_105 = " > v2_";
  protected final String TEXT_106 = "){" + NL + "\t\t                \t";
  protected final String TEXT_107 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_108 = " < v2_";
  protected final String TEXT_109 = "){" + NL + "\t\t                \t";
  protected final String TEXT_110 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_111 = NL + "\t                double v1_";
  protected final String TEXT_112 = " = readDouble(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t                double v2_";
  protected final String TEXT_113 = " = readDouble(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_114 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_115 = " = String.valueOf(v1_";
  protected final String TEXT_116 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_117 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_118 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_119 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_120 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_121 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_122 = NL + "\t\t                if(v1_";
  protected final String TEXT_123 = " > v2_";
  protected final String TEXT_124 = "){" + NL + "\t\t                \t";
  protected final String TEXT_125 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_126 = " < v2_";
  protected final String TEXT_127 = "){" + NL + "\t\t                \t";
  protected final String TEXT_128 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_129 = NL + "\t            \tfloat v1_";
  protected final String TEXT_130 = " = readFloat(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t                float v2_";
  protected final String TEXT_131 = " = readFloat(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_132 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_133 = " = String.valueOf(v1_";
  protected final String TEXT_134 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_135 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_136 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_137 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_138 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_139 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_140 = NL + "\t\t                if(v1_";
  protected final String TEXT_141 = " > v2_";
  protected final String TEXT_142 = "){" + NL + "\t\t                \t";
  protected final String TEXT_143 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_144 = " < v2_";
  protected final String TEXT_145 = "){" + NL + "\t\t                \t";
  protected final String TEXT_146 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_147 = NL + "\t                byte[] bs1_";
  protected final String TEXT_148 = " = new byte[(short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff))];" + NL + "\t                pos1 += 2; " + NL + "\t                for(int i = 0; i < bs1_";
  protected final String TEXT_149 = ".length; i++){" + NL + "\t                \tbs1_";
  protected final String TEXT_150 = "[i] = b1[pos1+i];" + NL + "\t                }" + NL + "\t                pos1 += bs1_";
  protected final String TEXT_151 = ".length;" + NL + "\t                int scale1_";
  protected final String TEXT_152 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 2;" + NL + "\t                java.math.BigDecimal bd1_";
  protected final String TEXT_153 = " = new java.math.BigDecimal(new java.math.BigInteger(bs1_";
  protected final String TEXT_154 = "), scale1_";
  protected final String TEXT_155 = ");" + NL + "\t                " + NL + "\t                byte[] bs2_";
  protected final String TEXT_156 = " = new byte[(short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff))];" + NL + "\t                pos2 += 2; " + NL + "\t                for(int i = 0; i < bs2_";
  protected final String TEXT_157 = ".length; i++){" + NL + "\t                \tbs2_";
  protected final String TEXT_158 = "[i] = b2[pos2+i];" + NL + "\t                }" + NL + "\t                pos2 += bs2_";
  protected final String TEXT_159 = ".length;" + NL + "\t                int scale2_";
  protected final String TEXT_160 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 2;" + NL + "\t                java.math.BigDecimal bd2_";
  protected final String TEXT_161 = " = new java.math.BigDecimal(new java.math.BigInteger(bs2_";
  protected final String TEXT_162 = "), scale2_";
  protected final String TEXT_163 = ");" + NL + "\t                " + NL + "\t                ";
  protected final String TEXT_164 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_165 = " = String.valueOf(bd1_";
  protected final String TEXT_166 = ").compareTo(String.valueOf(bd2_";
  protected final String TEXT_167 = "));" + NL + "\t            \t";
  protected final String TEXT_168 = NL + "\t\t                int cmp_";
  protected final String TEXT_169 = " = bd1_";
  protected final String TEXT_170 = ".compareTo(bd2_";
  protected final String TEXT_171 = ");" + NL + "\t\t            ";
  protected final String TEXT_172 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_173 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_174 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_175 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_176 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_177 = NL + "\t            \tint v1_";
  protected final String TEXT_178 = " = readInt(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t            \tint v2_";
  protected final String TEXT_179 = " = readInt(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_180 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_181 = " = String.valueOf(v1_";
  protected final String TEXT_182 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_183 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_184 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_185 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_186 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_187 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_188 = NL + "\t\t                if(v1_";
  protected final String TEXT_189 = " > v2_";
  protected final String TEXT_190 = "){" + NL + "\t\t                \t";
  protected final String TEXT_191 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_192 = " < v2_";
  protected final String TEXT_193 = "){" + NL + "\t\t                \t";
  protected final String TEXT_194 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_195 = NL + "\t           \t\tlong v1_";
  protected final String TEXT_196 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_197 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t               \t";
  protected final String TEXT_198 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_199 = " = String.valueOf(v1_";
  protected final String TEXT_200 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_201 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_202 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_203 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_204 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_205 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_206 = NL + "\t\t                if(v1_";
  protected final String TEXT_207 = " > v2_";
  protected final String TEXT_208 = "){" + NL + "\t\t                \t";
  protected final String TEXT_209 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_210 = " < v2_";
  protected final String TEXT_211 = "){" + NL + "\t\t                \t";
  protected final String TEXT_212 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_213 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_214 = NL + "\t            ";
  protected final String TEXT_215 = NL + "\t            \tshort v1_";
  protected final String TEXT_216 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t            \tshort v2_";
  protected final String TEXT_217 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            \t";
  protected final String TEXT_218 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_219 = " = String.valueOf(v1_";
  protected final String TEXT_220 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_221 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_222 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_223 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_224 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_225 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_226 = NL + "\t\t            \tif(v1_";
  protected final String TEXT_227 = " > v2_";
  protected final String TEXT_228 = "){" + NL + "\t\t                \t";
  protected final String TEXT_229 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_230 = " < v2_";
  protected final String TEXT_231 = "){" + NL + "\t\t                \t";
  protected final String TEXT_232 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_233 = NL + "\t            \tint len1_";
  protected final String TEXT_234 = " = readInt(b1, pos1);" + NL + "\t            \tpos1 += 4;" + NL + "\t            \tbyte[] bs1_";
  protected final String TEXT_235 = " = new byte[len1_";
  protected final String TEXT_236 = "];" + NL + "\t            \tfor(int i = 0; i < bs1_";
  protected final String TEXT_237 = ".length; i++){" + NL + "\t            \t\tbs1_";
  protected final String TEXT_238 = "[i] = b1[pos1 + i];" + NL + "\t            \t}" + NL + "\t            \tpos1 += bs1_";
  protected final String TEXT_239 = ".length;" + NL + "\t            \tString v1_";
  protected final String TEXT_240 = " = new String(bs1_";
  protected final String TEXT_241 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint len2_";
  protected final String TEXT_242 = " = readInt(b2, pos2);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tbyte[] bs2_";
  protected final String TEXT_243 = " = new byte[len2_";
  protected final String TEXT_244 = "];" + NL + "\t            \tfor(int i = 0; i < bs2_";
  protected final String TEXT_245 = ".length; i++){" + NL + "\t            \t\tbs2_";
  protected final String TEXT_246 = "[i] = b2[pos2 + i];" + NL + "\t            \t}" + NL + "\t            \tpos2 += bs2_";
  protected final String TEXT_247 = ".length;" + NL + "\t            \tString v2_";
  protected final String TEXT_248 = " = new String(bs2_";
  protected final String TEXT_249 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint comp_";
  protected final String TEXT_250 = " = v1_";
  protected final String TEXT_251 = ".compareTo(v2_";
  protected final String TEXT_252 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_253 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_254 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_255 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_256 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_257 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_258 = NL + "\t            ";
  protected final String TEXT_259 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_260 = NL + "\t            ";
  protected final String TEXT_261 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_262 = NL + "\t            ";
  protected final String TEXT_263 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_264 = NL + "\t\t\t\t\tjava.util.Map<String, Integer> compareColumns = new java.util.HashMap<String, Integer>();\t" + NL + "\t\t\t\t";
  protected final String TEXT_265 = NL + "                    \t";
  protected final String TEXT_266 = NL + "                \t";
  protected final String TEXT_267 = NL + "                \t\t";
  protected final String TEXT_268 = NL + "                \t";
  protected final String TEXT_269 = NL + "\t\t\t\t\t\tif(compareColumns.get(\"";
  protected final String TEXT_270 = "\") == null){" + NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_271 = "\") > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_272 = NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_273 = "\") < 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_274 = NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_275 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_276 = " extends WritableComparator{" + NL + "\t\t\t\tint pos1;" + NL + "\t\t\t\tint pos2;" + NL + "\t\t\t\tint comp = 0;" + NL + "\t\t\t\t" + NL + "\t\t\t\tprotected ";
  protected final String TEXT_277 = "(){" + NL + "\t\t\t\t\tsuper(";
  protected final String TEXT_278 = ".class, false);" + NL + "\t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2){" + NL + "\t\t\t\t\ttry{" + NL + "\t\t\t\t\t\tpos1 = s1;" + NL + "\t\t\t\t\t\tpos2 = s2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_279 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_280 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn comp;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t}catch(Exception e){" + NL + "\t\t\t\t\t\tthrow new RuntimeException(e);" + NL + "\t\t\t\t\t}\t\t\t\t\t" + NL + "\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_281 = NL + "\t\t\t\t\tpublic int compare(WritableComparable w1, WritableComparable w2){" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_282 = " k1 = (";
  protected final String TEXT_283 = ")w1;" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_284 = " k2 = (";
  protected final String TEXT_285 = ")w2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_286 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn 0;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_287 = NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_288 = NL + "\t\t\t\tif(k1.";
  protected final String TEXT_289 = " == null && k2.";
  protected final String TEXT_290 = " != null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_291 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_292 = " != null && k2.";
  protected final String TEXT_293 = " == null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_294 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_295 = " == null && k2.";
  protected final String TEXT_296 = " == null){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_297 = "\t\t" + NL + "            \t";
  protected final String TEXT_298 = NL + "\t        \t\tif(k1.";
  protected final String TEXT_299 = " != k2.";
  protected final String TEXT_300 = "){" + NL + "\t        \t\t\tif(k1.";
  protected final String TEXT_301 = "){" + NL + "\t\t        \t\t\t";
  protected final String TEXT_302 = NL + "\t        \t\t\t}else{" + NL + "\t\t        \t\t\t";
  protected final String TEXT_303 = NL + "\t        \t\t\t}" + NL + "\t        \t\t}" + NL + "\t        \t";
  protected final String TEXT_304 = NL + "\t                if(k1.";
  protected final String TEXT_305 = " > k2.";
  protected final String TEXT_306 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_307 = NL + "\t        \t\t}else if(k1.";
  protected final String TEXT_308 = " < k2.";
  protected final String TEXT_309 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_310 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_311 = NL + "\t            \tString s1_";
  protected final String TEXT_312 = " = new String(k1.";
  protected final String TEXT_313 = ");" + NL + "\t            \tString s2_";
  protected final String TEXT_314 = " = new String(k2.";
  protected final String TEXT_315 = ");" + NL + "\t            \tif(!s1_";
  protected final String TEXT_316 = ".equals(s2_";
  protected final String TEXT_317 = ")){" + NL + "\t\t\t\t\t\tif(s1_";
  protected final String TEXT_318 = ".compareTo(s2_";
  protected final String TEXT_319 = ") > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_320 = NL + "\t\t\t\t\t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_321 = NL + "\t\t\t\t\t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_322 = NL + "\t            \tif(k1.";
  protected final String TEXT_323 = " - k2.";
  protected final String TEXT_324 = " != 0){" + NL + "\t            \t\tif(k1.";
  protected final String TEXT_325 = " - k2.";
  protected final String TEXT_326 = " > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_327 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_328 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_329 = NL + "\t                ";
  protected final String TEXT_330 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_331 = " = FormatterUtils.format_DateInUTC(k1.";
  protected final String TEXT_332 = ", ";
  protected final String TEXT_333 = ").compareTo(FormatterUtils.format_DateInUTC(k2.";
  protected final String TEXT_334 = ", ";
  protected final String TEXT_335 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_336 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_337 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_338 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_339 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_340 = NL + "\t            \t\tif(!k1.";
  protected final String TEXT_341 = ".equals(k2.";
  protected final String TEXT_342 = ")){" + NL + "\t\t\t                if(k1.";
  protected final String TEXT_343 = ".compareTo(k2.";
  protected final String TEXT_344 = ") > 0){" + NL + "\t\t\t                \t";
  protected final String TEXT_345 = NL + "\t\t\t                }else{" + NL + "\t\t\t                \t";
  protected final String TEXT_346 = NL + "\t\t\t                }" + NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_347 = NL + "\t                ";
  protected final String TEXT_348 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_349 = " = String.valueOf(k1.";
  protected final String TEXT_350 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_351 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_352 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_353 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_354 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_355 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_356 = NL + "\t\t                if(k1.";
  protected final String TEXT_357 = " > k2.";
  protected final String TEXT_358 = "){" + NL + "\t\t                \t";
  protected final String TEXT_359 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_360 = " < k2.";
  protected final String TEXT_361 = "){" + NL + "\t\t                \t";
  protected final String TEXT_362 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_363 = NL + "\t                ";
  protected final String TEXT_364 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_365 = " = String.valueOf(k1.";
  protected final String TEXT_366 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_367 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_368 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_369 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_370 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_371 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_372 = NL + "\t\t                if(k1.";
  protected final String TEXT_373 = " > k2.";
  protected final String TEXT_374 = "){" + NL + "\t\t                \t";
  protected final String TEXT_375 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_376 = " < k2.";
  protected final String TEXT_377 = "){" + NL + "\t\t                \t";
  protected final String TEXT_378 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_379 = NL + "\t                ";
  protected final String TEXT_380 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_381 = " = String.valueOf(k1.";
  protected final String TEXT_382 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_383 = "));" + NL + "\t            \t";
  protected final String TEXT_384 = NL + "\t\t                int cmp_";
  protected final String TEXT_385 = " = k1.";
  protected final String TEXT_386 = ".compareTo(k2.";
  protected final String TEXT_387 = ");" + NL + "\t\t            ";
  protected final String TEXT_388 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_389 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_390 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_391 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_392 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_393 = NL + "\t                ";
  protected final String TEXT_394 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_395 = " = String.valueOf(k1.";
  protected final String TEXT_396 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_397 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_398 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_399 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_400 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_401 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_402 = NL + "\t\t                if(k1.";
  protected final String TEXT_403 = " > k2.";
  protected final String TEXT_404 = "){" + NL + "\t\t                \t";
  protected final String TEXT_405 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_406 = " < k2.";
  protected final String TEXT_407 = "){" + NL + "\t\t                \t";
  protected final String TEXT_408 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_409 = NL + "\t               \t";
  protected final String TEXT_410 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_411 = " = String.valueOf(k1.";
  protected final String TEXT_412 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_413 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_414 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_415 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_416 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_417 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_418 = NL + "\t\t                if(k1.";
  protected final String TEXT_419 = " > k2.";
  protected final String TEXT_420 = "){" + NL + "\t\t                \t";
  protected final String TEXT_421 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_422 = " < k2.";
  protected final String TEXT_423 = "){" + NL + "\t\t                \t";
  protected final String TEXT_424 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_425 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_426 = NL + "\t            ";
  protected final String TEXT_427 = NL + "\t            \t";
  protected final String TEXT_428 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_429 = " = String.valueOf(k1.";
  protected final String TEXT_430 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_431 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_432 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_433 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_434 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_435 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_436 = NL + "\t\t            \tif(k1.";
  protected final String TEXT_437 = " > k2.";
  protected final String TEXT_438 = "){" + NL + "\t\t                \t";
  protected final String TEXT_439 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_440 = " < k2.";
  protected final String TEXT_441 = "){" + NL + "\t\t                \t";
  protected final String TEXT_442 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_443 = NL + "\t            \tint comp_";
  protected final String TEXT_444 = " = k1.";
  protected final String TEXT_445 = ".compareTo(k2.";
  protected final String TEXT_446 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_447 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_448 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_449 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_450 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_451 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_452 = NL + "\t            ";
  protected final String TEXT_453 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_454 = NL + "\t            ";
  protected final String TEXT_455 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_456 = NL + "\t            ";
  protected final String TEXT_457 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_458 = NL + "                        ";
  protected final String TEXT_459 = ".write";
  protected final String TEXT_460 = "(this.";
  protected final String TEXT_461 = ");";
  protected final String TEXT_462 = NL + "                        if(this.";
  protected final String TEXT_463 = " == null){";
  protected final String TEXT_464 = NL + "                                ";
  protected final String TEXT_465 = ".writeInt(-1);";
  protected final String TEXT_466 = NL + "                                ";
  protected final String TEXT_467 = ".writeByte(-1);";
  protected final String TEXT_468 = NL + "                        }else{";
  protected final String TEXT_469 = NL + "                                ";
  protected final String TEXT_470 = ".writeInt(this.";
  protected final String TEXT_471 = ".length);";
  protected final String TEXT_472 = NL + "                                ";
  protected final String TEXT_473 = ".writeByte(0);";
  protected final String TEXT_474 = NL + "                                ";
  protected final String TEXT_475 = ".writeBoolean(this.";
  protected final String TEXT_476 = ");";
  protected final String TEXT_477 = NL + "                                ";
  protected final String TEXT_478 = ".writeByte(this.";
  protected final String TEXT_479 = ");";
  protected final String TEXT_480 = NL + "                                ";
  protected final String TEXT_481 = ".write(this.";
  protected final String TEXT_482 = ");";
  protected final String TEXT_483 = NL + "                                ";
  protected final String TEXT_484 = ".writeChar(this.";
  protected final String TEXT_485 = ");";
  protected final String TEXT_486 = NL + "                                ";
  protected final String TEXT_487 = ".writeLong(this.";
  protected final String TEXT_488 = ".getTime());";
  protected final String TEXT_489 = NL + "                                ";
  protected final String TEXT_490 = ".writeDouble(this.";
  protected final String TEXT_491 = ");";
  protected final String TEXT_492 = NL + "                                ";
  protected final String TEXT_493 = ".writeFloat(this.";
  protected final String TEXT_494 = ");";
  protected final String TEXT_495 = NL + "                                byte[] bytes_";
  protected final String TEXT_496 = " = this.";
  protected final String TEXT_497 = ".unscaledValue().toByteArray();" + NL + "                                short length_";
  protected final String TEXT_498 = " = (short)bytes_";
  protected final String TEXT_499 = ".length;" + NL + "                                short scale_";
  protected final String TEXT_500 = " = (short)this.";
  protected final String TEXT_501 = ".scale();";
  protected final String TEXT_502 = NL + "                                ";
  protected final String TEXT_503 = ".writeShort(length_";
  protected final String TEXT_504 = ");";
  protected final String TEXT_505 = NL + "                                ";
  protected final String TEXT_506 = ".write(bytes_";
  protected final String TEXT_507 = ");";
  protected final String TEXT_508 = NL + "                                ";
  protected final String TEXT_509 = ".writeShort(scale_";
  protected final String TEXT_510 = ");";
  protected final String TEXT_511 = NL + "                                ";
  protected final String TEXT_512 = ".writeInt(this.";
  protected final String TEXT_513 = ");";
  protected final String TEXT_514 = NL + "                                ";
  protected final String TEXT_515 = ".writeLong(this.";
  protected final String TEXT_516 = ");";
  protected final String TEXT_517 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_518 = NL + "                                ";
  protected final String TEXT_519 = ".writeShort(this.";
  protected final String TEXT_520 = ");";
  protected final String TEXT_521 = NL + "                                byte[] bytes_";
  protected final String TEXT_522 = " = this.";
  protected final String TEXT_523 = ".getBytes(\"UTF-8\");";
  protected final String TEXT_524 = NL + "                                ";
  protected final String TEXT_525 = ".writeInt(bytes_";
  protected final String TEXT_526 = ".length);";
  protected final String TEXT_527 = NL + "                                ";
  protected final String TEXT_528 = ".write(bytes_";
  protected final String TEXT_529 = ");";
  protected final String TEXT_530 = NL + "                                Don't support List type: column--";
  protected final String TEXT_531 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_532 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_533 = NL + "                        }";
  protected final String TEXT_534 = NL + "                        this.";
  protected final String TEXT_535 = " = ";
  protected final String TEXT_536 = ".read";
  protected final String TEXT_537 = "();";
  protected final String TEXT_538 = NL + "                        int length_";
  protected final String TEXT_539 = " = ";
  protected final String TEXT_540 = ".readInt();" + NL + "                        if(length_";
  protected final String TEXT_541 = " == -1){";
  protected final String TEXT_542 = NL + "                        if(";
  protected final String TEXT_543 = ".readByte() == -1){";
  protected final String TEXT_544 = NL + "                            this.";
  protected final String TEXT_545 = " = null;" + NL + "                        }else{";
  protected final String TEXT_546 = NL + "                                this.";
  protected final String TEXT_547 = " = ";
  protected final String TEXT_548 = ".readBoolean();";
  protected final String TEXT_549 = NL + "                                this.";
  protected final String TEXT_550 = " = ";
  protected final String TEXT_551 = ".readByte();";
  protected final String TEXT_552 = NL + "                                this.";
  protected final String TEXT_553 = " = new byte[length_";
  protected final String TEXT_554 = "];";
  protected final String TEXT_555 = NL + "                                ";
  protected final String TEXT_556 = ".readFully(this.";
  protected final String TEXT_557 = ");";
  protected final String TEXT_558 = NL + "                                this.";
  protected final String TEXT_559 = " = ";
  protected final String TEXT_560 = ".readChar();";
  protected final String TEXT_561 = NL + "                                this.";
  protected final String TEXT_562 = " = new java.util.Date(";
  protected final String TEXT_563 = ".readLong());";
  protected final String TEXT_564 = NL + "                                this.";
  protected final String TEXT_565 = " = ";
  protected final String TEXT_566 = ".readDouble();";
  protected final String TEXT_567 = NL + "                                this.";
  protected final String TEXT_568 = " = ";
  protected final String TEXT_569 = ".readFloat();";
  protected final String TEXT_570 = NL + "                                int length_";
  protected final String TEXT_571 = " = ";
  protected final String TEXT_572 = ".readShort();" + NL + "                                byte[] bytes_";
  protected final String TEXT_573 = " = new byte[length_";
  protected final String TEXT_574 = "];";
  protected final String TEXT_575 = NL + "                                ";
  protected final String TEXT_576 = ".readFully(bytes_";
  protected final String TEXT_577 = ");" + NL + "                                int scale_";
  protected final String TEXT_578 = " = ";
  protected final String TEXT_579 = ".readShort();" + NL + "                                this.";
  protected final String TEXT_580 = " = new java.math.BigDecimal(new java.math.BigInteger(bytes_";
  protected final String TEXT_581 = "), scale_";
  protected final String TEXT_582 = ");";
  protected final String TEXT_583 = NL + "                                this.";
  protected final String TEXT_584 = " = ";
  protected final String TEXT_585 = ".readInt();";
  protected final String TEXT_586 = NL + "                                this.";
  protected final String TEXT_587 = " = ";
  protected final String TEXT_588 = ".readLong();";
  protected final String TEXT_589 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_590 = NL + "                                this.";
  protected final String TEXT_591 = " = ";
  protected final String TEXT_592 = ".readShort();";
  protected final String TEXT_593 = NL + "                                int length_";
  protected final String TEXT_594 = " = ";
  protected final String TEXT_595 = ".readInt();" + NL + "                                byte[] bytes_";
  protected final String TEXT_596 = " = new byte[length_";
  protected final String TEXT_597 = "];";
  protected final String TEXT_598 = NL + "                                ";
  protected final String TEXT_599 = ".readFully(bytes_";
  protected final String TEXT_600 = ", 0, length_";
  protected final String TEXT_601 = ");" + NL + "                                this.";
  protected final String TEXT_602 = " = new String(bytes_";
  protected final String TEXT_603 = ", 0, length_";
  protected final String TEXT_604 = ", \"UTF-8\");";
  protected final String TEXT_605 = NL + "                                Don't support List type: column--";
  protected final String TEXT_606 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_607 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_608 = NL + "                        }";
  protected final String TEXT_609 = NL + "            final int prime = 31;" + NL + "            int result = 1;";
  protected final String TEXT_610 = NL + "                            result = prime * result + (this.";
  protected final String TEXT_611 = " ? 1231 : 1237);";
  protected final String TEXT_612 = NL + "                            result = prime * result + (int) this.";
  protected final String TEXT_613 = ";";
  protected final String TEXT_614 = NL + "                        result = prime * result + java.util.Arrays.hashCode(this.";
  protected final String TEXT_615 = ");";
  protected final String TEXT_616 = NL + "                        result = prime * result + ((this.";
  protected final String TEXT_617 = " == null) ? 0 : this.";
  protected final String TEXT_618 = ".hashCode());";
  protected final String TEXT_619 = NL + "            return result;";
  protected final String TEXT_620 = NL + "            if (this == ";
  protected final String TEXT_621 = ") return true;" + NL + "            if (";
  protected final String TEXT_622 = " == null) return false;" + NL + "            if (getClass() != ";
  protected final String TEXT_623 = ".getClass()) return false;" + NL + "            final ";
  protected final String TEXT_624 = " other = (";
  protected final String TEXT_625 = ") ";
  protected final String TEXT_626 = ";";
  protected final String TEXT_627 = NL + "                        if (this.";
  protected final String TEXT_628 = " != other.";
  protected final String TEXT_629 = ")" + NL + "                            return false;";
  protected final String TEXT_630 = NL + "                        if(!java.util.Arrays.equals(this.";
  protected final String TEXT_631 = ", other.";
  protected final String TEXT_632 = ")) {" + NL + "                            return false;" + NL + "                        }";
  protected final String TEXT_633 = NL + "                        if (this.";
  protected final String TEXT_634 = " == null) {" + NL + "                            if (other.";
  protected final String TEXT_635 = " != null)" + NL + "                                return false;" + NL + "                        } else if (!this.";
  protected final String TEXT_636 = ".equals(other.";
  protected final String TEXT_637 = "))" + NL + "                            return false;";
  protected final String TEXT_638 = NL + "            return true;";
  protected final String TEXT_639 = NL + "            int returnValue = -1;";
  protected final String TEXT_640 = NL + "                    returnValue = checkNullsAndCompare(this.";
  protected final String TEXT_641 = ", ";
  protected final String TEXT_642 = ".";
  protected final String TEXT_643 = ");" + NL + "                    if(returnValue != 0) {" + NL + "                        return returnValue;" + NL + "                    }";
  protected final String TEXT_644 = NL + "            return returnValue;";
  protected final String TEXT_645 = NL + "            public static class ";
  protected final String TEXT_646 = " implements ";
  protected final String TEXT_647 = " {" + NL;
  protected final String TEXT_648 = NL + "                    public ";
  protected final String TEXT_649 = " ";
  protected final String TEXT_650 = NL + "                        = ' '";
  protected final String TEXT_651 = ";";
  protected final String TEXT_652 = NL;
  protected final String TEXT_653 = NL + NL + "                public int hashCode() {";
  protected final String TEXT_654 = NL + "                }" + NL + "" + NL + "                public boolean equals(Object obj) {";
  protected final String TEXT_655 = NL + "                }" + NL + "" + NL + "                public String toString() {" + NL + "                    StringBuilder sb = new StringBuilder();" + NL + "                    sb.append(super.toString());" + NL + "                    sb.append(\"[\");";
  protected final String TEXT_656 = NL + "                                sb.append(\"";
  protected final String TEXT_657 = "=\"+";
  protected final String TEXT_658 = ");";
  protected final String TEXT_659 = NL + "                                sb.append(\"";
  protected final String TEXT_660 = "=\"+String.valueOf(";
  protected final String TEXT_661 = "));";
  protected final String TEXT_662 = NL + "                    sb.append(\"]\");" + NL + "" + NL + "                    return sb.toString();" + NL + "                }" + NL + "" + NL + "                public void write(DataOutput out) throws IOException {";
  protected final String TEXT_663 = NL + "                }" + NL + "" + NL + "                public void readFields(DataInput in) throws IOException {";
  protected final String TEXT_664 = NL + "                }" + NL + "" + NL + "                public int compareTo(";
  protected final String TEXT_665 = " other) {";
  protected final String TEXT_666 = NL + "                }" + NL + "" + NL + "                private int checkNullsAndCompare(Object object1, Object object2) {" + NL + "                    int returnValue = 0;" + NL + "                    if (object1 instanceof Comparable && object2 instanceof Comparable) {" + NL + "                        returnValue = ((Comparable) object1).compareTo(object2);" + NL + "                    } else if (object1 != null && object2 != null) {" + NL + "                        returnValue = compareStrings(object1.toString(), object2.toString());" + NL + "                    } else if (object1 == null && object2 != null) {" + NL + "                        returnValue = 1;" + NL + "                    } else if (object1 != null && object2 == null) {" + NL + "                        returnValue = -1;" + NL + "                    } else {" + NL + "                        returnValue = 0;" + NL + "                    }" + NL + "" + NL + "                    return returnValue;" + NL + "                }" + NL + "" + NL + "                private int compareStrings(String string1, String string2) {" + NL + "                    return string1.compareTo(string2);" + NL + "                }" + NL;
  protected final String TEXT_667 = NL + "            }";
  protected final String TEXT_668 = NL + "                static{" + NL + "                    WritableComparator.define(";
  protected final String TEXT_669 = ".class, new ";
  protected final String TEXT_670 = "());" + NL + "                }";
  protected final String TEXT_671 = NL + "    public void ";
  protected final String TEXT_672 = "Process(final GlobalVar globalMap) throws Exception{" + NL + "" + NL + "        try {" + NL + "" + NL + "            String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();" + NL + "            boolean resumeIt = currentMethodName.equals(resumeEntryMethodName);" + NL + "            if( resumeEntryMethodName == null || resumeIt || globalResumeTicket) { //start the resume" + NL + "                globalResumeTicket = true;" + NL;
  protected final String TEXT_673 = NL + "                ";
  protected final String TEXT_674 = "Process(globalMap);";
  protected final String TEXT_675 = NL + "                        if(true){" + NL + "                            throw new Exception(\"No processing components defined in the subprocess ";
  protected final String TEXT_676 = "Process!\");" + NL + "                        }";
  protected final String TEXT_677 = NL + "                    final String mr_temp_dir = this.mr_temp_dir;" + NL + "                    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(";
  protected final String TEXT_678 = ");" + NL + "                    ugi.doAs(new PrivilegedExceptionAction<Void>() {" + NL + "                        public Void run() throws Exception {";
  protected final String TEXT_679 = NL + "                            FileSystem.setDefaultUri(getConf(), new java.net.URI(";
  protected final String TEXT_680 = "));";
  protected final String TEXT_681 = NL + "                            " + NL + "                            final FileSystem fs = FileSystem.get(getConf());" + NL + "                            final JobConf job = new JobConf(getConf());" + NL + "                            job.setJobName(projectName + \"_\" + jobName + \"_\" + jobVersion + \"_\" + \"";
  protected final String TEXT_682 = "\");" + NL + "                            job.setJarByClass(";
  protected final String TEXT_683 = ".class);" + NL + "                            ChainMapper chainMapper = new ChainMapper(job);" + NL + "                            MultipleInputs.setInputMapperClass(job);";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append(TEXT_1);
    
	class ComparatorHelper{
		String className;
		List<IMetadataColumn> columns;
		String structName;
		Map<String, Boolean> criterias;
		List<String> orders;
		boolean unorder = false;
		Map<String, Boolean> sortTypes; // false means sort by alpha
		boolean genObjectCompare = false;
		
		public void init(String className, List<IMetadataColumn> columns, String structName){
			this.className = className;
			this.columns = columns;
			this.structName = structName;
			orders = new java.util.ArrayList<String>();
			criterias = new java.util.HashMap<String, Boolean>();
			sortTypes = new java.util.HashMap<String, Boolean>();
			for(IMetadataColumn column : columns){
				orders.add(column.getLabel());
				criterias.put(column.getLabel(), true);
				sortTypes.put(column.getLabel(), true);
			}
		}
		public void setGenObjectCompare(Boolean genObjectCompare){
			this.genObjectCompare = genObjectCompare;
		}
		//true is asc
		//false is desc
		public void setCriterias(Map<String,Boolean> criterias){
			this.criterias = criterias;
		}
		
		public void setOrders(List<String> orders){
			this.orders = orders;
			int index = 0;
			for(String order : orders){
				for(int i = 0; i < columns.size(); i++){
					if(order.equals(columns.get(i).getLabel())){
						if(i < index){
							unorder = true;
						}else{
							index = i;
						}
						break;
					}
				}
				if(unorder){
					break;
				}
			}
		}
		
		public void setSortTypes(Map<String, Boolean> sortTypes){
			this.sortTypes = sortTypes;
		}
		
		private void greater(String columnName){
			if(unorder){
				addGreater(columnName);
			}else{
				genGreater(columnName);
			}
		}
		private void lesser(String columnName){
			if(unorder){
				addLesser(columnName);
			}else{
				genLesser(columnName);
			}			
		}
		private void addGreater(String columnName){
		
    stringBuffer.append(TEXT_2);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_3);
    
		}
		private void addLesser(String columnName){
		
    stringBuffer.append(TEXT_4);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_5);
    	
		}
		private void genGreater(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_6);
    	
			}else{
			
    stringBuffer.append(TEXT_7);
    
			}
		}
		private void genLesser(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_8);
    	
			}else{
			
    stringBuffer.append(TEXT_9);
    
			}
		}
		
		private void skipColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
    stringBuffer.append(TEXT_10);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_11);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_12);
    
				}else{
	       		
    stringBuffer.append(TEXT_13);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_14);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_15);
    
	        	}
				
    stringBuffer.append(TEXT_16);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_17);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_18);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_19);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_20);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_21);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_22);
    }
    stringBuffer.append(TEXT_23);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_24);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_25);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_26);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_27);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_28);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_29);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	            
    stringBuffer.append(TEXT_30);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_31);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_32);
    
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_33);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_34);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_35);
    
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_36);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_37);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_38);
    
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_39);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_40);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_41);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_42);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_43);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_44);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_45);
    
	            }
				
    if(nullable){
    stringBuffer.append(TEXT_46);
    
        	}
		}
		
		private void compareColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
    stringBuffer.append(TEXT_47);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_48);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_49);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_50);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_51);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_52);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_53);
    
				}else{
	       		
    stringBuffer.append(TEXT_54);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_55);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_56);
    
	        	}
				
    stringBuffer.append(TEXT_57);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_58);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_59);
    lesser(columnName);
    stringBuffer.append(TEXT_60);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_62);
    greater(columnName);
    stringBuffer.append(TEXT_63);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_65);
    }
    stringBuffer.append(TEXT_66);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_67);
    greater(columnName);
    stringBuffer.append(TEXT_68);
    lesser(columnName);
    stringBuffer.append(TEXT_69);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_70);
    greater(columnName);
    stringBuffer.append(TEXT_71);
    lesser(columnName);
    stringBuffer.append(TEXT_72);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_73);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_74);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_77);
    greater(columnName);
    stringBuffer.append(TEXT_78);
    lesser(columnName);
    stringBuffer.append(TEXT_79);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_83);
    greater(columnName);
    stringBuffer.append(TEXT_84);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_86);
    lesser(columnName);
    stringBuffer.append(TEXT_87);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_88);
    greater(columnName);
    stringBuffer.append(TEXT_89);
    lesser(columnName);
    stringBuffer.append(TEXT_90);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_91);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_92);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_93);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_94);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_95);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_100);
    greater(columnName);
    stringBuffer.append(TEXT_101);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_102);
    lesser(columnName);
    stringBuffer.append(TEXT_103);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_104);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_105);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_106);
    greater(columnName);
    stringBuffer.append(TEXT_107);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_109);
    lesser(columnName);
    stringBuffer.append(TEXT_110);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_111);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_112);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_113);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_114);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_115);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_118);
    greater(columnName);
    stringBuffer.append(TEXT_119);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_120);
    lesser(columnName);
    stringBuffer.append(TEXT_121);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_122);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_123);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_124);
    greater(columnName);
    stringBuffer.append(TEXT_125);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_127);
    lesser(columnName);
    stringBuffer.append(TEXT_128);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_129);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_130);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_131);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_132);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_133);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_136);
    greater(columnName);
    stringBuffer.append(TEXT_137);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_138);
    lesser(columnName);
    stringBuffer.append(TEXT_139);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_140);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_141);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_142);
    greater(columnName);
    stringBuffer.append(TEXT_143);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_145);
    lesser(columnName);
    stringBuffer.append(TEXT_146);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_147);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_148);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_149);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_150);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_151);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_152);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_153);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_154);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_155);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_156);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_157);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_158);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_159);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_160);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_161);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_162);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_163);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_164);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_165);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_167);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_168);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_169);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_171);
    }
    stringBuffer.append(TEXT_172);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_173);
    greater(columnName);
    stringBuffer.append(TEXT_174);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_175);
    lesser(columnName);
    stringBuffer.append(TEXT_176);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_177);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_178);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_179);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_180);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_181);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_184);
    greater(columnName);
    stringBuffer.append(TEXT_185);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_186);
    lesser(columnName);
    stringBuffer.append(TEXT_187);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_188);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_189);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_190);
    greater(columnName);
    stringBuffer.append(TEXT_191);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_193);
    lesser(columnName);
    stringBuffer.append(TEXT_194);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_195);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_196);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_197);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_198);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_199);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_202);
    greater(columnName);
    stringBuffer.append(TEXT_203);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_204);
    lesser(columnName);
    stringBuffer.append(TEXT_205);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_206);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_207);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_208);
    greater(columnName);
    stringBuffer.append(TEXT_209);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_211);
    lesser(columnName);
    stringBuffer.append(TEXT_212);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_213);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_214);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_215);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_216);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_217);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_218);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_219);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_222);
    greater(columnName);
    stringBuffer.append(TEXT_223);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_224);
    lesser(columnName);
    stringBuffer.append(TEXT_225);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_226);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_227);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_228);
    greater(columnName);
    stringBuffer.append(TEXT_229);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_231);
    lesser(columnName);
    stringBuffer.append(TEXT_232);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_233);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_234);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_235);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_236);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_237);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_238);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_239);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_240);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_241);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_242);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_243);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_244);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_245);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_246);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_247);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_248);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_249);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_250);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_251);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_252);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_253);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_254);
    greater(columnName);
    stringBuffer.append(TEXT_255);
    lesser(columnName);
    stringBuffer.append(TEXT_256);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_257);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_258);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_259);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_260);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_261);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_262);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_263);
    
        	}
		}
		
		public void compareColumns(){
			if(columns != null){
				if(unorder){
				
    stringBuffer.append(TEXT_264);
    
				}
	            for(IMetadataColumn column : columns){
	            	String columnName = column.getLabel();
                	if(orders.contains(columnName)){
                	
    stringBuffer.append(TEXT_265);
    compareColumn(column);
    stringBuffer.append(TEXT_266);
    
                	}else{
                	
    stringBuffer.append(TEXT_267);
    skipColumn(column);
    stringBuffer.append(TEXT_268);
    	
	               	}
				}
				if(unorder){
					for(String columnName : orders){
					
    stringBuffer.append(TEXT_269);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_270);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_271);
    genGreater(columnName);
    stringBuffer.append(TEXT_272);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_273);
    genLesser(columnName);
    stringBuffer.append(TEXT_274);
    	
					}
				}
			}
		}
		
		public void compareAfterColumns(){
		}
		
		public void generateCode(){
		
    stringBuffer.append(TEXT_275);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_276);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_278);
    compareColumns();
    stringBuffer.append(TEXT_279);
    compareAfterColumns();
    stringBuffer.append(TEXT_280);
    if(genObjectCompare){
    stringBuffer.append(TEXT_281);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_282);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_285);
    
						for(IMetadataColumn column : columns){
			            	String columnName = column.getLabel();
		                	compareObjectColumn(column);
						}
						
    stringBuffer.append(TEXT_286);
    }
    stringBuffer.append(TEXT_287);
    
		}
		
		private void compareObjectColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
			
    stringBuffer.append(TEXT_288);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_289);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_290);
    lesser(columnName);
    stringBuffer.append(TEXT_291);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_293);
    greater(columnName);
    stringBuffer.append(TEXT_294);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_295);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_296);
    }
    stringBuffer.append(TEXT_297);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_298);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_299);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_300);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_301);
    greater(columnName);
    stringBuffer.append(TEXT_302);
    lesser(columnName);
    stringBuffer.append(TEXT_303);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_304);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_305);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_306);
    greater(columnName);
    stringBuffer.append(TEXT_307);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_309);
    lesser(columnName);
    stringBuffer.append(TEXT_310);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_311);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_312);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_313);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_314);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_315);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_316);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_317);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_318);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_319);
    greater(columnName);
    stringBuffer.append(TEXT_320);
    lesser(columnName);
    stringBuffer.append(TEXT_321);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_322);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_323);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_324);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_326);
    greater(columnName);
    stringBuffer.append(TEXT_327);
    lesser(columnName);
    stringBuffer.append(TEXT_328);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_329);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_330);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_331);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_332);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_333);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_336);
    greater(columnName);
    stringBuffer.append(TEXT_337);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_338);
    lesser(columnName);
    stringBuffer.append(TEXT_339);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_340);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_341);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_343);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_344);
    greater(columnName);
    stringBuffer.append(TEXT_345);
    lesser(columnName);
    stringBuffer.append(TEXT_346);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_347);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_348);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_349);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_350);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_351);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_352);
    greater(columnName);
    stringBuffer.append(TEXT_353);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_354);
    lesser(columnName);
    stringBuffer.append(TEXT_355);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_356);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_357);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_358);
    greater(columnName);
    stringBuffer.append(TEXT_359);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_361);
    lesser(columnName);
    stringBuffer.append(TEXT_362);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_363);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_364);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_365);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_366);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_368);
    greater(columnName);
    stringBuffer.append(TEXT_369);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_370);
    lesser(columnName);
    stringBuffer.append(TEXT_371);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_372);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_373);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_374);
    greater(columnName);
    stringBuffer.append(TEXT_375);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_376);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_377);
    lesser(columnName);
    stringBuffer.append(TEXT_378);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_379);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_380);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_381);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_382);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_383);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_384);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_385);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_386);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_387);
    }
    stringBuffer.append(TEXT_388);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_389);
    greater(columnName);
    stringBuffer.append(TEXT_390);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_391);
    lesser(columnName);
    stringBuffer.append(TEXT_392);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_393);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_394);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_395);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_396);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_397);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_398);
    greater(columnName);
    stringBuffer.append(TEXT_399);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_400);
    lesser(columnName);
    stringBuffer.append(TEXT_401);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_402);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_403);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_404);
    greater(columnName);
    stringBuffer.append(TEXT_405);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_406);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_407);
    lesser(columnName);
    stringBuffer.append(TEXT_408);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_409);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_410);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_411);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_412);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_413);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_414);
    greater(columnName);
    stringBuffer.append(TEXT_415);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_416);
    lesser(columnName);
    stringBuffer.append(TEXT_417);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_418);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_419);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_420);
    greater(columnName);
    stringBuffer.append(TEXT_421);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_422);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_423);
    lesser(columnName);
    stringBuffer.append(TEXT_424);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_425);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_426);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_427);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_428);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_429);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_430);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_431);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_432);
    greater(columnName);
    stringBuffer.append(TEXT_433);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_434);
    lesser(columnName);
    stringBuffer.append(TEXT_435);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_436);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_437);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_438);
    greater(columnName);
    stringBuffer.append(TEXT_439);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_440);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_441);
    lesser(columnName);
    stringBuffer.append(TEXT_442);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_443);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_444);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_445);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_446);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_447);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_448);
    greater(columnName);
    stringBuffer.append(TEXT_449);
    lesser(columnName);
    stringBuffer.append(TEXT_450);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_451);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_452);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_453);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_454);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_455);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_456);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_457);
    
        	}
		}
	}
	
    
    class StructHelper{
        String structName;
        List<IMetadataColumn> columns;
        java.util.Set<IMetadataColumn> columnsNonSerialized;
        String implementsClasses;
        final boolean genComparator;

        /**
         * The comparator should usually only be generated if the structure is
         * being used as an input KEY into a reduce step.
         */
        public StructHelper(String structName, List<IMetadataColumn> columns, boolean genComparator, java.util.Set<IMetadataColumn> columnsNonSerialized) {
            this.structName = structName + "Struct";
            this.columns = columns;
            this.columnsNonSerialized = columnsNonSerialized;
            this.implementsClasses = "WritableComparable<"+this.structName+">";
            this.genComparator = genComparator;
        }

        public StructHelper(String structName, List<IMetadataColumn> columns, boolean genComparator) {
            this.structName = structName + "Struct";
            this.columns = columns;
            this.implementsClasses = "WritableComparable<"+this.structName+">";
            this.genComparator = genComparator;
        }

        public String getStructName(){
            return structName;
        }

        public void appendImplClasses(String className){
            this.implementsClasses += ",";
            this.implementsClasses += className;
        }

        public void overrideWrite(String dataOutput){
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    if (columnsNonSerialized != null && columnsNonSerialized.contains(columns))
                        continue;
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());

                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                        typeToGenerate=typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
                        
    stringBuffer.append(TEXT_458);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_459);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_460);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_461);
    
                    } else{
                        
    stringBuffer.append(TEXT_462);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_463);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_464);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_465);
    
                            }else{
                            
    stringBuffer.append(TEXT_466);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_467);
    
                            }
                            
    stringBuffer.append(TEXT_468);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_469);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_470);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_471);
    
                            }else{
                            
    stringBuffer.append(TEXT_472);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_473);
    
                            }
                            
    
                            if(typeToGenerate.equals("Boolean")) {
                            
    stringBuffer.append(TEXT_474);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_475);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_476);
    
                            } else if(typeToGenerate.equals("Byte")) {
                            
    stringBuffer.append(TEXT_477);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_478);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_479);
    
                            } else if(typeToGenerate.equals("byte[]")) {
                            
    stringBuffer.append(TEXT_480);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_481);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_482);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")) {
                            
    stringBuffer.append(TEXT_483);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_484);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_485);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_486);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_487);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_488);
    
                            } else if(typeToGenerate.equals("Double")) {
                            
    stringBuffer.append(TEXT_489);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_490);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_491);
    
                            } else if(typeToGenerate.equals("Float")) {
                            
    stringBuffer.append(TEXT_492);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_493);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_494);
    
                            } else if(typeToGenerate.equals("BigDecimal")) {
                            
    stringBuffer.append(TEXT_495);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_496);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_497);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_498);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_499);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_500);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_501);
    stringBuffer.append(TEXT_502);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_503);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_504);
    stringBuffer.append(TEXT_505);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_506);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_507);
    stringBuffer.append(TEXT_508);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_509);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_510);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_511);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_512);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_513);
    
                            } else if(typeToGenerate.equals("Long")) {
                            
    stringBuffer.append(TEXT_514);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_515);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_516);
    
                            } else if(typeToGenerate.equals("Object")) {
                            
    stringBuffer.append(TEXT_517);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")) {
                            
    stringBuffer.append(TEXT_518);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_519);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_520);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_521);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_522);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_523);
    stringBuffer.append(TEXT_524);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_525);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_526);
    stringBuffer.append(TEXT_527);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_528);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_529);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_530);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Document")) {
                            
    stringBuffer.append(TEXT_531);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")) {
                            
    stringBuffer.append(TEXT_532);
    stringBuffer.append(column.getLabel());
    
                            }
                               
    stringBuffer.append(TEXT_533);
    
                    }
                }
            }
        }

        public void overrideReadFields(String dataInput){
            if (columns != null) {
                for (IMetadataColumn column: columns) {
                    if (columnsNonSerialized != null && columnsNonSerialized.contains(columns))
                        continue;
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());

                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                        typeToGenerate=typeToGenerate.substring(0,1).toUpperCase()+typeToGenerate.substring(1);
                        
    stringBuffer.append(TEXT_534);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_535);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_536);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_537);
    
                    }else{
                        if(typeToGenerate.equals("byte[]")){
                        
    stringBuffer.append(TEXT_538);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_539);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_540);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_541);
    
                        }else{
                           
    stringBuffer.append(TEXT_542);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_543);
    
                        }
                        
    stringBuffer.append(TEXT_544);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_545);
    
                            if(typeToGenerate.equals("Boolean")){
                            
    stringBuffer.append(TEXT_546);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_547);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_548);
    
                            } else if(typeToGenerate.equals("Byte")){
                            
    stringBuffer.append(TEXT_549);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_550);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_551);
    
                            } else if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_552);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_553);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_554);
    stringBuffer.append(TEXT_555);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_556);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_557);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")){
                            
    stringBuffer.append(TEXT_558);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_559);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_560);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_561);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_562);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_563);
    
                            } else if(typeToGenerate.equals("Double")){
                            
    stringBuffer.append(TEXT_564);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_565);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_566);
    
                            } else if(typeToGenerate.equals("Float")){
                            
    stringBuffer.append(TEXT_567);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_568);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_569);
    
                            } else if(typeToGenerate.equals("BigDecimal")){
                            
    stringBuffer.append(TEXT_570);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_571);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_572);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_573);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_574);
    stringBuffer.append(TEXT_575);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_576);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_577);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_578);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_579);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_580);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_581);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_582);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_583);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_584);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_585);
    
                            } else if(typeToGenerate.equals("Long")){
                            
    stringBuffer.append(TEXT_586);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_587);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_588);
    
                            } else if(typeToGenerate.equals("Object")){
                            
    stringBuffer.append(TEXT_589);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")){
                            
    stringBuffer.append(TEXT_590);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_591);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_592);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_593);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_594);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_595);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_596);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_597);
    stringBuffer.append(TEXT_598);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_599);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_600);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_601);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_602);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_603);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_604);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_605);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Doucument")) {
                            
    stringBuffer.append(TEXT_606);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")){
                            
    stringBuffer.append(TEXT_607);
    stringBuffer.append(column.getLabel());
    
                            }
                            
    stringBuffer.append(TEXT_608);
    
                    }
                }
            }
        }

        public void overrideHashCode(){
        
    stringBuffer.append(TEXT_609);
    
            if (columns != null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                         String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                         if(javaType == JavaTypesManager.BOOLEAN) {
                        
    stringBuffer.append(TEXT_610);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_611);
    
                        } else {
                        
    stringBuffer.append(TEXT_612);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_613);
    
                        }
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                        
    stringBuffer.append(TEXT_614);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_615);
    
                    } else {
                    
    stringBuffer.append(TEXT_616);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_617);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_618);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_619);
    
        }

        public void overrideEquals(String objName){
        
    stringBuffer.append(TEXT_620);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_621);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_622);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_623);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_624);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_625);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_626);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                    
    stringBuffer.append(TEXT_627);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_628);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_629);
    
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                    
    stringBuffer.append(TEXT_630);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_631);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_632);
    
                    } else {
                    
    stringBuffer.append(TEXT_633);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_634);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_635);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_636);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_637);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_638);
    
        }

        public void overrideCompareTo(String otherName){
            //int returnValue = 0; ?
            
    stringBuffer.append(TEXT_639);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                
    stringBuffer.append(TEXT_640);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_641);
    stringBuffer.append(otherName);
    stringBuffer.append(TEXT_642);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_643);
    
                }
            }
            
    stringBuffer.append(TEXT_644);
    
        }

        public void addMethods(){
        }

        public void declareVars(){
        }

        public void generateCode(){
        
    stringBuffer.append(TEXT_645);
    stringBuffer.append(structName );
    stringBuffer.append(TEXT_646);
    stringBuffer.append(implementsClasses);
    stringBuffer.append(TEXT_647);
    
                if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    
    stringBuffer.append(TEXT_648);
    stringBuffer.append( JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()) );
    stringBuffer.append(TEXT_649);
    stringBuffer.append(column.getLabel() );
    
                        if(javaType == JavaTypesManager.CHARACTER && !column.isNullable()) {
                        
    stringBuffer.append(TEXT_650);
    
                        }
                        
    stringBuffer.append(TEXT_651);
    
                    }
                }
                
    stringBuffer.append(TEXT_652);
    declareVars();
    stringBuffer.append(TEXT_653);
    overrideHashCode();
    stringBuffer.append(TEXT_654);
    overrideEquals("obj");
    stringBuffer.append(TEXT_655);
    
                    if (columns !=null) {
                        for(int i=0; i< columns.size(); i++) {
                            IMetadataColumn column = columns.get(i);
                            String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                            if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_656);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_657);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_658);
    
                            }else{
                            
    stringBuffer.append(TEXT_659);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_660);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_661);
    
                            }
                        }
                    }
                    
    stringBuffer.append(TEXT_662);
    overrideWrite("out");
    stringBuffer.append(TEXT_663);
    overrideReadFields("in");
    stringBuffer.append(TEXT_664);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_665);
    overrideCompareTo("other");
    stringBuffer.append(TEXT_666);
    addMethods();
    stringBuffer.append(TEXT_667);
    
            if(genComparator){
                ComparatorHelper StructComparator = new ComparatorHelper();
                StructComparator.init(structName + "_Comparator", columns, structName);
                StructComparator.generateCode();

                
    stringBuffer.append(TEXT_668);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_669);
    stringBuffer.append(structName + "_Comparator");
    stringBuffer.append(TEXT_670);
    
            }
        }
    }
    
    
    CodeGeneratorArgument codeGenArgument = (CodeGeneratorArgument) argument;
    Vector v = (Vector) codeGenArgument.getArgument();
    INode mrconn = (INode)v.get(0);
    NodesSubTree subTree = (NodesSubTree)v.get(1);

    for(INode node : subTree.getNodes()) {
        List<IMetadataTable> metadatas = node.getMetadataList();
        List< ? extends IConnection> conns = node.getOutgoingConnections();
        for(IConnection conn : conns) {
            if(conn.getLineStyle().equals(EConnectionType.FLOW_MAIN) || conn.getLineStyle().equals(EConnectionType.FLOW_REF)){
                IMetadataTable metadata = conn.getMetadataTable();
                List<IMetadataColumn> columns = metadata.getListColumns();

                StructHelper rowStruct = new StructHelper(conn.getName(), columns, false);
                rowStruct.generateCode();
            }
        }
    }
    
    stringBuffer.append(TEXT_671);
    stringBuffer.append(subTree.getName());
    stringBuffer.append(TEXT_672);
    
            // Call for RUN AFTER links
            for(String after : subTree.getAfterSubProcesses()){
            
    stringBuffer.append(TEXT_673);
    stringBuffer.append(after);
    stringBuffer.append(TEXT_674);
    
            }

            INode firstNode = subTree.getNode(subTree.getName());
            List<? extends IConnection> conns = firstNode.getOutgoingSortedConnections();
            String firstConnName = "";
            if(conns != null && conns.size()>0){
                IConnection conn = conns.get(0);
                if (conn.getLineStyle().hasConnectionCategory(IConnectionCategory.DATA)) {
                    firstConnName = conn.getName();
                }
            }
            //if only one component and GUI without warning,
            //if the component is tMRConfiguration, ignore
            //if the component is in lookup connector, actually we should use merge connector instead of lookup connector, but now for work, we need transfer the necessary resource
            if(!"".equals(firstConnName)){
                //when there only have tMR*Input and tMR*Output, no warning on the GUI
                if(subTree.getNodes().size() > 1){
                    if(subTree.getNodes().size() == 2){
                    
    stringBuffer.append(TEXT_675);
    stringBuffer.append(subTree.getName());
    stringBuffer.append(TEXT_676);
    
                    }
                    String username = ElementParameterParser.getValue(mrconn,"__USERNAME__");
                    String mrDistribution = ElementParameterParser.getValue(mrconn,"__DISTRIBUTION__");
                    String mrVersion = ElementParameterParser.getValue(mrconn,"__MR_VERSION__");
                    org.talend.hadoop.distribution.component.MRComponent mrDistrib = null;
                    try {
                        mrDistrib = (org.talend.hadoop.distribution.component.MRComponent) org.talend.hadoop.distribution.DistributionFactory.buildDistribution(mrDistribution, mrVersion);
                    } catch (java.lang.Exception e) {
                        e.printStackTrace();
                        return "";
                    }
                    boolean isCustom = mrDistrib instanceof org.talend.hadoop.distribution.custom.CustomDistribution;

                    boolean mrUseKrb = false;
                    if(isCustom || mrDistrib.doSupportKerberos()){
                        mrUseKrb = "true".equals(ElementParameterParser.getValue(mrconn,"__USE_KRB__"));
                    }

                    if(!(username == null || "".equals(username) || "\"\"".equals(username) || mrUseKrb)){
                    
    stringBuffer.append(TEXT_677);
    stringBuffer.append(username);
    stringBuffer.append(TEXT_678);
    
                    } // (!(username == null || "".equals(username) || "\"\"".equals(username) || mrUseKrb))
                    
                    // Dataproc distribution
                     
                    boolean isGoogleDataprocDistribution = !isCustom && mrDistrib.isGoogleDataprocDistribution();
                    if(isGoogleDataprocDistribution){
                    
    stringBuffer.append(TEXT_679);
    stringBuffer.append(ElementParameterParser.getValue(mrconn, "__GOOGLE_STORAGE_IO_BUCKET__"));
    stringBuffer.append(TEXT_680);
    
                    }
                    
    stringBuffer.append(TEXT_681);
    stringBuffer.append(subTree.getName());
    stringBuffer.append(TEXT_682);
    stringBuffer.append(codeGenArgument.getJobName());
    stringBuffer.append(TEXT_683);
    
                }
            }
            
    return stringBuffer.toString();
  }
}
