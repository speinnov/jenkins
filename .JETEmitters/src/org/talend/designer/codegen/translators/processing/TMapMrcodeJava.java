package org.talend.designer.codegen.translators.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.talend.core.model.metadata.IMetadataColumn;
import org.talend.core.model.metadata.IMetadataTable;
import org.talend.core.model.metadata.MetadataTalendType;
import org.talend.core.model.metadata.types.JavaType;
import org.talend.core.model.metadata.types.JavaTypesManager;
import org.talend.core.model.process.EConnectionType;
import org.talend.core.model.process.ElementParameterParser;
import org.talend.core.model.process.IConnection;
import org.talend.core.model.process.IConnectionCategory;
import org.talend.core.model.process.INode;
import org.talend.core.model.utils.TalendTextUtils;
import org.talend.designer.common.BigDataCodeGeneratorArgument;
import org.talend.designer.common.tmap.TMapUtilLegacy;
import org.talend.designer.mapper.MapperComponent;
import org.talend.designer.mapper.MapperMain;
import org.talend.designer.mapper.external.data.ExternalMapperData;
import org.talend.designer.mapper.external.data.ExternalMapperTable;
import org.talend.designer.mapper.external.data.ExternalMapperTableEntry;
import org.talend.designer.mapper.language.ILanguage;
import org.talend.designer.mapper.language.LanguageProvider;
import org.talend.designer.mapper.language.generation.GenerationManagerFactory;
import org.talend.designer.mapper.language.generation.JavaGenerationManager;

public class TMapMrcodeJava
{
  protected static String nl;
  public static synchronized TMapMrcodeJava create(String lineSeparator)
  {
    nl = lineSeparator;
    TMapMrcodeJava result = new TMapMrcodeJava();
    nl = null;
    return result;
  }

  public final String NL = nl == null ? (System.getProperties().getProperty("line.separator")) : nl;
  protected final String TEXT_1 = "";
  protected final String TEXT_2 = NL + "\t";
  protected final String TEXT_3 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_4 = "\", 1);" + NL + "\t\t";
  protected final String TEXT_5 = NL + "\t\t\tcompareColumns.put(\"";
  protected final String TEXT_6 = "\", -1);" + NL + "\t\t";
  protected final String TEXT_7 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_8 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_9 = NL + "\t\t\t\treturn -1;" + NL + "\t\t\t";
  protected final String TEXT_10 = NL + "\t\t\t\treturn 1;" + NL + "\t\t\t";
  protected final String TEXT_11 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_12 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_13 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t";
  protected final String TEXT_14 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_15 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_16 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_17 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_18 = " && !null2_";
  protected final String TEXT_19 = "){" + NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_20 = " && null2_";
  protected final String TEXT_21 = "){" + NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_22 = " && null2_";
  protected final String TEXT_23 = "){" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_24 = "\t\t" + NL + "            \t";
  protected final String TEXT_25 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_26 = NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_27 = NL + "\t            \tpos1 += length1_";
  protected final String TEXT_28 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_29 = ";" + NL + "\t            ";
  protected final String TEXT_30 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_31 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_32 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_33 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_34 = NL + "\t                pos1 += (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 4; " + NL + "\t                pos2 += (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 4; " + NL + "\t            ";
  protected final String TEXT_35 = NL + "\t                pos1 += 4;" + NL + "\t                pos2 += 4;" + NL + "\t            ";
  protected final String TEXT_36 = NL + "\t                pos1 += 8;" + NL + "\t                pos2 += 8;" + NL + "\t            ";
  protected final String TEXT_37 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_38 = NL + "\t            ";
  protected final String TEXT_39 = NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_40 = NL + "\t            \tpos1 += 4;" + NL + "\t            \tpos1 += readInt(b1, pos1);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tpos2 += readInt(b2, pos2);" + NL + "\t            ";
  protected final String TEXT_41 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_42 = NL + "\t            ";
  protected final String TEXT_43 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_44 = NL + "\t            ";
  protected final String TEXT_45 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_46 = NL + "\t            ";
  protected final String TEXT_47 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_48 = NL + "\t\t\t\t\tint length1_";
  protected final String TEXT_49 = " = readInt(b1, pos1);" + NL + "\t\t\t\t\tpos1 += 4;" + NL + "\t\t\t\t\tint length2_";
  protected final String TEXT_50 = " = readInt(b2, pos2);" + NL + "\t\t\t\t\tpos2 += 4;" + NL + "\t\t\t\t\tboolean null1_";
  protected final String TEXT_51 = " = length1_";
  protected final String TEXT_52 = " == -1;" + NL + "\t\t\t\t\tboolean null2_";
  protected final String TEXT_53 = " = length2_";
  protected final String TEXT_54 = " == -1;" + NL + "\t\t\t\t";
  protected final String TEXT_55 = NL + "\t        \t\tboolean null1_";
  protected final String TEXT_56 = " = b1[pos1] == -1;" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tboolean null2_";
  protected final String TEXT_57 = " = b2[pos2] == -1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_58 = NL + "\t\t\t\tif(null1_";
  protected final String TEXT_59 = " && !null2_";
  protected final String TEXT_60 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_61 = NL + "\t\t\t\t}else if(!null1_";
  protected final String TEXT_62 = " && null2_";
  protected final String TEXT_63 = "){" + NL + "\t\t\t\t\t";
  protected final String TEXT_64 = NL + "\t\t\t\t}else if(null1_";
  protected final String TEXT_65 = " && null2_";
  protected final String TEXT_66 = "){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_67 = "\t\t" + NL + "            \t";
  protected final String TEXT_68 = NL + "\t        \t\tif(b1[pos1] > 0 && b2[pos2] ==0){" + NL + "\t        \t\t\t";
  protected final String TEXT_69 = NL + "\t        \t\t}else if(b1[pos1] == 0 && b2[pos2] > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_70 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t        \t";
  protected final String TEXT_71 = NL + "\t                if(b1[pos1] > b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_72 = NL + "\t        \t\t}else if(b1[pos1] < b2[pos2]){" + NL + "\t        \t\t\t";
  protected final String TEXT_73 = NL + "\t        \t\t}" + NL + "\t        \t\tpos1 += 1;" + NL + "\t        \t\tpos2 += 1;" + NL + "\t            ";
  protected final String TEXT_74 = NL + "\t            \tint n_";
  protected final String TEXT_75 = " = Math.min(length1_";
  protected final String TEXT_76 = ", length2_";
  protected final String TEXT_77 = ");" + NL + "\t            \tfor(int i = 0; i < n_";
  protected final String TEXT_78 = "; i++){" + NL + "\t            \t\tif(b1[pos1+i] > b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_79 = NL + "\t            \t\t}else if(b1[pos1+i] < b2[pos2+i]){" + NL + "\t            \t\t\t";
  protected final String TEXT_80 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += length1_";
  protected final String TEXT_81 = ";" + NL + "\t            \tpos2 += length2_";
  protected final String TEXT_82 = ";" + NL + "\t            \tif(length1_";
  protected final String TEXT_83 = " > length2_";
  protected final String TEXT_84 = "){" + NL + "\t            \t\t";
  protected final String TEXT_85 = NL + "\t            \t}else if(length1_";
  protected final String TEXT_86 = " < length2_";
  protected final String TEXT_87 = "){" + NL + "\t            \t\t";
  protected final String TEXT_88 = NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_89 = NL + "\t            \tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) != 0){" + NL + "\t            \t\tif((char)((b1[pos1] << 8) | (b1[pos1+1] & 0xff)) - (char)((b2[pos2] << 8) | (b2[pos2+1] & 0xff)) > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_90 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_91 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            ";
  protected final String TEXT_92 = NL + "\t            \tlong v1_";
  protected final String TEXT_93 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_94 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_95 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_96 = " = FormatterUtils.format_DateInUTC(new java.util.Date(v1_";
  protected final String TEXT_97 = "), ";
  protected final String TEXT_98 = ").compareTo(FormatterUtils.format_DateInUTC(new java.util.Date(v2_";
  protected final String TEXT_99 = "), ";
  protected final String TEXT_100 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_101 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_102 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_103 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_104 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_105 = NL + "\t\t                if(v1_";
  protected final String TEXT_106 = " > v2_";
  protected final String TEXT_107 = "){" + NL + "\t\t                \t";
  protected final String TEXT_108 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_109 = " < v2_";
  protected final String TEXT_110 = "){" + NL + "\t\t                \t";
  protected final String TEXT_111 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_112 = NL + "\t                double v1_";
  protected final String TEXT_113 = " = readDouble(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t                double v2_";
  protected final String TEXT_114 = " = readDouble(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t                ";
  protected final String TEXT_115 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_116 = " = String.valueOf(v1_";
  protected final String TEXT_117 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_118 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_119 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_120 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_121 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_122 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_123 = NL + "\t\t                if(v1_";
  protected final String TEXT_124 = " > v2_";
  protected final String TEXT_125 = "){" + NL + "\t\t                \t";
  protected final String TEXT_126 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_127 = " < v2_";
  protected final String TEXT_128 = "){" + NL + "\t\t                \t";
  protected final String TEXT_129 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_130 = NL + "\t            \tfloat v1_";
  protected final String TEXT_131 = " = readFloat(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t                float v2_";
  protected final String TEXT_132 = " = readFloat(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_133 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_134 = " = String.valueOf(v1_";
  protected final String TEXT_135 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_136 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_137 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_138 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_139 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_140 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_141 = NL + "\t\t                if(v1_";
  protected final String TEXT_142 = " > v2_";
  protected final String TEXT_143 = "){" + NL + "\t\t                \t";
  protected final String TEXT_144 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_145 = " < v2_";
  protected final String TEXT_146 = "){" + NL + "\t\t                \t";
  protected final String TEXT_147 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_148 = NL + "\t                byte[] bs1_";
  protected final String TEXT_149 = " = new byte[(short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff))];" + NL + "\t                pos1 += 2; " + NL + "\t                for(int i = 0; i < bs1_";
  protected final String TEXT_150 = ".length; i++){" + NL + "\t                \tbs1_";
  protected final String TEXT_151 = "[i] = b1[pos1+i];" + NL + "\t                }" + NL + "\t                pos1 += bs1_";
  protected final String TEXT_152 = ".length;" + NL + "\t                int scale1_";
  protected final String TEXT_153 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t                pos1 += 2;" + NL + "\t                java.math.BigDecimal bd1_";
  protected final String TEXT_154 = " = new java.math.BigDecimal(new java.math.BigInteger(bs1_";
  protected final String TEXT_155 = "), scale1_";
  protected final String TEXT_156 = ");" + NL + "\t                " + NL + "\t                byte[] bs2_";
  protected final String TEXT_157 = " = new byte[(short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff))];" + NL + "\t                pos2 += 2; " + NL + "\t                for(int i = 0; i < bs2_";
  protected final String TEXT_158 = ".length; i++){" + NL + "\t                \tbs2_";
  protected final String TEXT_159 = "[i] = b2[pos2+i];" + NL + "\t                }" + NL + "\t                pos2 += bs2_";
  protected final String TEXT_160 = ".length;" + NL + "\t                int scale2_";
  protected final String TEXT_161 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t                pos2 += 2;" + NL + "\t                java.math.BigDecimal bd2_";
  protected final String TEXT_162 = " = new java.math.BigDecimal(new java.math.BigInteger(bs2_";
  protected final String TEXT_163 = "), scale2_";
  protected final String TEXT_164 = ");" + NL + "\t                " + NL + "\t                ";
  protected final String TEXT_165 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_166 = " = String.valueOf(bd1_";
  protected final String TEXT_167 = ").compareTo(String.valueOf(bd2_";
  protected final String TEXT_168 = "));" + NL + "\t            \t";
  protected final String TEXT_169 = NL + "\t\t                int cmp_";
  protected final String TEXT_170 = " = bd1_";
  protected final String TEXT_171 = ".compareTo(bd2_";
  protected final String TEXT_172 = ");" + NL + "\t\t            ";
  protected final String TEXT_173 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_174 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_175 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_176 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_177 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_178 = NL + "\t            \tint v1_";
  protected final String TEXT_179 = " = readInt(b1, pos1);" + NL + "\t                pos1 += 4;" + NL + "\t            \tint v2_";
  protected final String TEXT_180 = " = readInt(b2, pos2);" + NL + "\t                pos2 += 4;" + NL + "\t                ";
  protected final String TEXT_181 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_182 = " = String.valueOf(v1_";
  protected final String TEXT_183 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_184 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_185 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_186 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_187 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_188 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_189 = NL + "\t\t                if(v1_";
  protected final String TEXT_190 = " > v2_";
  protected final String TEXT_191 = "){" + NL + "\t\t                \t";
  protected final String TEXT_192 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_193 = " < v2_";
  protected final String TEXT_194 = "){" + NL + "\t\t                \t";
  protected final String TEXT_195 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_196 = NL + "\t           \t\tlong v1_";
  protected final String TEXT_197 = " = readLong(b1, pos1);" + NL + "\t                pos1 += 8;" + NL + "\t            \tlong v2_";
  protected final String TEXT_198 = " = readLong(b2, pos2);" + NL + "\t                pos2 += 8;" + NL + "\t               \t";
  protected final String TEXT_199 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_200 = " = String.valueOf(v1_";
  protected final String TEXT_201 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_202 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_203 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_204 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_205 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_206 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_207 = NL + "\t\t                if(v1_";
  protected final String TEXT_208 = " > v2_";
  protected final String TEXT_209 = "){" + NL + "\t\t                \t";
  protected final String TEXT_210 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_211 = " < v2_";
  protected final String TEXT_212 = "){" + NL + "\t\t                \t";
  protected final String TEXT_213 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_214 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_215 = NL + "\t            ";
  protected final String TEXT_216 = NL + "\t            \tshort v1_";
  protected final String TEXT_217 = " = (short)((b1[pos1] << 8) | (b1[pos1+1] & 0xff));" + NL + "\t            \tshort v2_";
  protected final String TEXT_218 = " = (short)((b2[pos2] << 8) | (b2[pos2+1] & 0xff));" + NL + "\t            \tpos1 += 2;" + NL + "\t            \tpos2 += 2;" + NL + "\t            \t";
  protected final String TEXT_219 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_220 = " = String.valueOf(v1_";
  protected final String TEXT_221 = ").compareTo(String.valueOf(v2_";
  protected final String TEXT_222 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_223 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_224 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_225 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_226 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_227 = NL + "\t\t            \tif(v1_";
  protected final String TEXT_228 = " > v2_";
  protected final String TEXT_229 = "){" + NL + "\t\t                \t";
  protected final String TEXT_230 = NL + "\t\t                }else if(v1_";
  protected final String TEXT_231 = " < v2_";
  protected final String TEXT_232 = "){" + NL + "\t\t                \t";
  protected final String TEXT_233 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_234 = NL + "\t            \tint len1_";
  protected final String TEXT_235 = " = readInt(b1, pos1);" + NL + "\t            \tpos1 += 4;" + NL + "\t            \tbyte[] bs1_";
  protected final String TEXT_236 = " = new byte[len1_";
  protected final String TEXT_237 = "];" + NL + "\t            \tfor(int i = 0; i < bs1_";
  protected final String TEXT_238 = ".length; i++){" + NL + "\t            \t\tbs1_";
  protected final String TEXT_239 = "[i] = b1[pos1 + i];" + NL + "\t            \t}" + NL + "\t            \tpos1 += bs1_";
  protected final String TEXT_240 = ".length;" + NL + "\t            \tString v1_";
  protected final String TEXT_241 = " = new String(bs1_";
  protected final String TEXT_242 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint len2_";
  protected final String TEXT_243 = " = readInt(b2, pos2);" + NL + "\t            \tpos2 += 4;" + NL + "\t            \tbyte[] bs2_";
  protected final String TEXT_244 = " = new byte[len2_";
  protected final String TEXT_245 = "];" + NL + "\t            \tfor(int i = 0; i < bs2_";
  protected final String TEXT_246 = ".length; i++){" + NL + "\t            \t\tbs2_";
  protected final String TEXT_247 = "[i] = b2[pos2 + i];" + NL + "\t            \t}" + NL + "\t            \tpos2 += bs2_";
  protected final String TEXT_248 = ".length;" + NL + "\t            \tString v2_";
  protected final String TEXT_249 = " = new String(bs2_";
  protected final String TEXT_250 = ", \"UTF-8\");" + NL + "\t            \t" + NL + "\t            \tint comp_";
  protected final String TEXT_251 = " = v1_";
  protected final String TEXT_252 = ".compareTo(v2_";
  protected final String TEXT_253 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_254 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_255 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_256 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_257 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_258 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_259 = NL + "\t            ";
  protected final String TEXT_260 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_261 = NL + "\t            ";
  protected final String TEXT_262 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_263 = NL + "\t            ";
  protected final String TEXT_264 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_265 = NL + "\t\t\t\t\tjava.util.Map<String, Integer> compareColumns = new java.util.HashMap<String, Integer>();\t" + NL + "\t\t\t\t";
  protected final String TEXT_266 = NL + "                    \t";
  protected final String TEXT_267 = NL + "                \t";
  protected final String TEXT_268 = NL + "                \t\t";
  protected final String TEXT_269 = NL + "                \t";
  protected final String TEXT_270 = NL + "\t\t\t\t\t\tif(compareColumns.get(\"";
  protected final String TEXT_271 = "\") == null){" + NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_272 = "\") > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_273 = NL + "\t\t\t\t\t\t}else if(compareColumns.get(\"";
  protected final String TEXT_274 = "\") < 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_275 = NL + "\t\t\t\t\t\t}" + NL + "\t\t\t\t\t";
  protected final String TEXT_276 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_277 = " extends WritableComparator{" + NL + "\t\t\t\tint pos1;" + NL + "\t\t\t\tint pos2;" + NL + "\t\t\t\tint comp = 0;" + NL + "\t\t\t\t" + NL + "\t\t\t\tprotected ";
  protected final String TEXT_278 = "(){" + NL + "\t\t\t\t\tsuper(";
  protected final String TEXT_279 = ".class, false);" + NL + "\t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2){" + NL + "\t\t\t\t\ttry{" + NL + "\t\t\t\t\t\tpos1 = s1;" + NL + "\t\t\t\t\t\tpos2 = s2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_280 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_281 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn comp;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t}catch(Exception e){" + NL + "\t\t\t\t\t\tthrow new RuntimeException(e);" + NL + "\t\t\t\t\t}\t\t\t\t\t" + NL + "\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_282 = NL + "\t\t\t\t\tpublic int compare(WritableComparable w1, WritableComparable w2){" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_283 = " k1 = (";
  protected final String TEXT_284 = ")w1;" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_285 = " k2 = (";
  protected final String TEXT_286 = ")w2;" + NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_287 = NL + "\t\t\t\t\t\t" + NL + "\t\t\t\t\t\treturn 0;" + NL + "\t\t\t\t\t}" + NL + "\t\t\t\t";
  protected final String TEXT_288 = NL + "\t\t\t}" + NL + "\t\t";
  protected final String TEXT_289 = NL + "\t\t\t\tif(k1.";
  protected final String TEXT_290 = " == null && k2.";
  protected final String TEXT_291 = " != null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_292 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_293 = " != null && k2.";
  protected final String TEXT_294 = " == null){" + NL + "\t\t\t\t\t";
  protected final String TEXT_295 = NL + "\t\t\t\t}else if(k1.";
  protected final String TEXT_296 = " == null && k2.";
  protected final String TEXT_297 = " == null){" + NL + "\t\t\t\t\t//ignore" + NL + "\t\t\t\t}else{\t" + NL + "\t\t\t";
  protected final String TEXT_298 = "\t\t" + NL + "            \t";
  protected final String TEXT_299 = NL + "\t        \t\tif(k1.";
  protected final String TEXT_300 = " != k2.";
  protected final String TEXT_301 = "){" + NL + "\t        \t\t\tif(k1.";
  protected final String TEXT_302 = "){" + NL + "\t\t        \t\t\t";
  protected final String TEXT_303 = NL + "\t        \t\t\t}else{" + NL + "\t\t        \t\t\t";
  protected final String TEXT_304 = NL + "\t        \t\t\t}" + NL + "\t        \t\t}" + NL + "\t        \t";
  protected final String TEXT_305 = NL + "\t                if(k1.";
  protected final String TEXT_306 = " > k2.";
  protected final String TEXT_307 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_308 = NL + "\t        \t\t}else if(k1.";
  protected final String TEXT_309 = " < k2.";
  protected final String TEXT_310 = "){" + NL + "\t        \t\t\t";
  protected final String TEXT_311 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_312 = NL + "\t            \tString s1_";
  protected final String TEXT_313 = " = new String(k1.";
  protected final String TEXT_314 = ");" + NL + "\t            \tString s2_";
  protected final String TEXT_315 = " = new String(k2.";
  protected final String TEXT_316 = ");" + NL + "\t            \tif(!s1_";
  protected final String TEXT_317 = ".equals(s2_";
  protected final String TEXT_318 = ")){" + NL + "\t\t\t\t\t\tif(s1_";
  protected final String TEXT_319 = ".compareTo(s2_";
  protected final String TEXT_320 = ") > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_321 = NL + "\t\t\t\t\t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_322 = NL + "\t\t\t\t\t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_323 = NL + "\t            \tif(k1.";
  protected final String TEXT_324 = " - k2.";
  protected final String TEXT_325 = " != 0){" + NL + "\t            \t\tif(k1.";
  protected final String TEXT_326 = " - k2.";
  protected final String TEXT_327 = " > 0){" + NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_328 = "            \t\t\t" + NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_329 = NL + "\t            \t\t}" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_330 = NL + "\t                ";
  protected final String TEXT_331 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_332 = " = FormatterUtils.format_DateInUTC(k1.";
  protected final String TEXT_333 = ", ";
  protected final String TEXT_334 = ").compareTo(FormatterUtils.format_DateInUTC(k2.";
  protected final String TEXT_335 = ", ";
  protected final String TEXT_336 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_337 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_338 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_339 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_340 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_341 = NL + "\t            \t\tif(!k1.";
  protected final String TEXT_342 = ".equals(k2.";
  protected final String TEXT_343 = ")){" + NL + "\t\t\t                if(k1.";
  protected final String TEXT_344 = ".compareTo(k2.";
  protected final String TEXT_345 = ") > 0){" + NL + "\t\t\t                \t";
  protected final String TEXT_346 = NL + "\t\t\t                }else{" + NL + "\t\t\t                \t";
  protected final String TEXT_347 = NL + "\t\t\t                }" + NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_348 = NL + "\t                ";
  protected final String TEXT_349 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_350 = " = String.valueOf(k1.";
  protected final String TEXT_351 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_352 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_353 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_354 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_355 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_356 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_357 = NL + "\t\t                if(k1.";
  protected final String TEXT_358 = " > k2.";
  protected final String TEXT_359 = "){" + NL + "\t\t                \t";
  protected final String TEXT_360 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_361 = " < k2.";
  protected final String TEXT_362 = "){" + NL + "\t\t                \t";
  protected final String TEXT_363 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_364 = NL + "\t                ";
  protected final String TEXT_365 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_366 = " = String.valueOf(k1.";
  protected final String TEXT_367 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_368 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_369 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_370 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_371 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_372 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_373 = NL + "\t\t                if(k1.";
  protected final String TEXT_374 = " > k2.";
  protected final String TEXT_375 = "){" + NL + "\t\t                \t";
  protected final String TEXT_376 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_377 = " < k2.";
  protected final String TEXT_378 = "){" + NL + "\t\t                \t";
  protected final String TEXT_379 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_380 = NL + "\t                ";
  protected final String TEXT_381 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_382 = " = String.valueOf(k1.";
  protected final String TEXT_383 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_384 = "));" + NL + "\t            \t";
  protected final String TEXT_385 = NL + "\t\t                int cmp_";
  protected final String TEXT_386 = " = k1.";
  protected final String TEXT_387 = ".compareTo(k2.";
  protected final String TEXT_388 = ");" + NL + "\t\t            ";
  protected final String TEXT_389 = NL + "\t        \t\tif(cmp_";
  protected final String TEXT_390 = " > 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_391 = NL + "\t        \t\t}else if(cmp_";
  protected final String TEXT_392 = " < 0){" + NL + "\t        \t\t\t";
  protected final String TEXT_393 = NL + "\t        \t\t}" + NL + "\t            ";
  protected final String TEXT_394 = NL + "\t                ";
  protected final String TEXT_395 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_396 = " = String.valueOf(k1.";
  protected final String TEXT_397 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_398 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_399 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_400 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_401 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_402 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_403 = NL + "\t\t                if(k1.";
  protected final String TEXT_404 = " > k2.";
  protected final String TEXT_405 = "){" + NL + "\t\t                \t";
  protected final String TEXT_406 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_407 = " < k2.";
  protected final String TEXT_408 = "){" + NL + "\t\t                \t";
  protected final String TEXT_409 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_410 = NL + "\t               \t";
  protected final String TEXT_411 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_412 = " = String.valueOf(k1.";
  protected final String TEXT_413 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_414 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_415 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_416 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_417 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_418 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_419 = NL + "\t\t                if(k1.";
  protected final String TEXT_420 = " > k2.";
  protected final String TEXT_421 = "){" + NL + "\t\t                \t";
  protected final String TEXT_422 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_423 = " < k2.";
  protected final String TEXT_424 = "){" + NL + "\t\t                \t";
  protected final String TEXT_425 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_426 = NL + "\t            \tDon't support Object type: column--";
  protected final String TEXT_427 = NL + "\t            ";
  protected final String TEXT_428 = NL + "\t            \t";
  protected final String TEXT_429 = NL + "\t            \t\tint cmp_";
  protected final String TEXT_430 = " = String.valueOf(k1.";
  protected final String TEXT_431 = ").compareTo(String.valueOf(k2.";
  protected final String TEXT_432 = "));" + NL + "\t            \t\tif(cmp_";
  protected final String TEXT_433 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_434 = NL + "\t            \t\t}else if(cmp_";
  protected final String TEXT_435 = " < 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_436 = NL + "\t            \t\t}" + NL + "\t            \t";
  protected final String TEXT_437 = NL + "\t\t            \tif(k1.";
  protected final String TEXT_438 = " > k2.";
  protected final String TEXT_439 = "){" + NL + "\t\t                \t";
  protected final String TEXT_440 = NL + "\t\t                }else if(k1.";
  protected final String TEXT_441 = " < k2.";
  protected final String TEXT_442 = "){" + NL + "\t\t                \t";
  protected final String TEXT_443 = NL + "\t\t                }" + NL + "\t            \t";
  protected final String TEXT_444 = NL + "\t            \tint comp_";
  protected final String TEXT_445 = " = k1.";
  protected final String TEXT_446 = ".compareTo(k2.";
  protected final String TEXT_447 = ");" + NL + "\t            \tif(comp_";
  protected final String TEXT_448 = " != 0){" + NL + "\t            \t\tif(comp_";
  protected final String TEXT_449 = " > 0){" + NL + "\t            \t\t\t";
  protected final String TEXT_450 = NL + "\t            \t\t}else{" + NL + "\t            \t\t\t";
  protected final String TEXT_451 = NL + "\t            \t\t}" + NL + "\t            \t\t" + NL + "\t            \t}" + NL + "\t            ";
  protected final String TEXT_452 = NL + "\t            \tDon't support List type: column--";
  protected final String TEXT_453 = NL + "\t            ";
  protected final String TEXT_454 = NL + "\t            \tDon't support Document type: column--";
  protected final String TEXT_455 = NL + "\t            ";
  protected final String TEXT_456 = NL + "\t            \tDon't support Dynamic type: column--";
  protected final String TEXT_457 = NL + "\t            ";
  protected final String TEXT_458 = NL + "        \t\t}        " + NL + "        \t";
  protected final String TEXT_459 = NL + "                        ";
  protected final String TEXT_460 = ".write";
  protected final String TEXT_461 = "(this.";
  protected final String TEXT_462 = ");";
  protected final String TEXT_463 = NL + "                        if(this.";
  protected final String TEXT_464 = " == null){";
  protected final String TEXT_465 = NL + "                                ";
  protected final String TEXT_466 = ".writeInt(-1);";
  protected final String TEXT_467 = NL + "                                ";
  protected final String TEXT_468 = ".writeByte(-1);";
  protected final String TEXT_469 = NL + "                        }else{";
  protected final String TEXT_470 = NL + "                                ";
  protected final String TEXT_471 = ".writeInt(this.";
  protected final String TEXT_472 = ".length);";
  protected final String TEXT_473 = NL + "                                ";
  protected final String TEXT_474 = ".writeByte(0);";
  protected final String TEXT_475 = NL + "                                ";
  protected final String TEXT_476 = ".writeBoolean(this.";
  protected final String TEXT_477 = ");";
  protected final String TEXT_478 = NL + "                                ";
  protected final String TEXT_479 = ".writeByte(this.";
  protected final String TEXT_480 = ");";
  protected final String TEXT_481 = NL + "                                ";
  protected final String TEXT_482 = ".write(this.";
  protected final String TEXT_483 = ");";
  protected final String TEXT_484 = NL + "                                ";
  protected final String TEXT_485 = ".writeChar(this.";
  protected final String TEXT_486 = ");";
  protected final String TEXT_487 = NL + "                                ";
  protected final String TEXT_488 = ".writeLong(this.";
  protected final String TEXT_489 = ".getTime());";
  protected final String TEXT_490 = NL + "                                ";
  protected final String TEXT_491 = ".writeDouble(this.";
  protected final String TEXT_492 = ");";
  protected final String TEXT_493 = NL + "                                ";
  protected final String TEXT_494 = ".writeFloat(this.";
  protected final String TEXT_495 = ");";
  protected final String TEXT_496 = NL + "                                byte[] bytes_";
  protected final String TEXT_497 = " = this.";
  protected final String TEXT_498 = ".unscaledValue().toByteArray();" + NL + "                                short length_";
  protected final String TEXT_499 = " = (short)bytes_";
  protected final String TEXT_500 = ".length;" + NL + "                                short scale_";
  protected final String TEXT_501 = " = (short)this.";
  protected final String TEXT_502 = ".scale();";
  protected final String TEXT_503 = NL + "                                ";
  protected final String TEXT_504 = ".writeShort(length_";
  protected final String TEXT_505 = ");";
  protected final String TEXT_506 = NL + "                                ";
  protected final String TEXT_507 = ".write(bytes_";
  protected final String TEXT_508 = ");";
  protected final String TEXT_509 = NL + "                                ";
  protected final String TEXT_510 = ".writeShort(scale_";
  protected final String TEXT_511 = ");";
  protected final String TEXT_512 = NL + "                                ";
  protected final String TEXT_513 = ".writeInt(this.";
  protected final String TEXT_514 = ");";
  protected final String TEXT_515 = NL + "                                ";
  protected final String TEXT_516 = ".writeLong(this.";
  protected final String TEXT_517 = ");";
  protected final String TEXT_518 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_519 = NL + "                                ";
  protected final String TEXT_520 = ".writeShort(this.";
  protected final String TEXT_521 = ");";
  protected final String TEXT_522 = NL + "                                byte[] bytes_";
  protected final String TEXT_523 = " = this.";
  protected final String TEXT_524 = ".getBytes(\"UTF-8\");";
  protected final String TEXT_525 = NL + "                                ";
  protected final String TEXT_526 = ".writeInt(bytes_";
  protected final String TEXT_527 = ".length);";
  protected final String TEXT_528 = NL + "                                ";
  protected final String TEXT_529 = ".write(bytes_";
  protected final String TEXT_530 = ");";
  protected final String TEXT_531 = NL + "                                Don't support List type: column--";
  protected final String TEXT_532 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_533 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_534 = NL + "                        }";
  protected final String TEXT_535 = NL + "                        this.";
  protected final String TEXT_536 = " = ";
  protected final String TEXT_537 = ".read";
  protected final String TEXT_538 = "();";
  protected final String TEXT_539 = NL + "                        int length_";
  protected final String TEXT_540 = " = ";
  protected final String TEXT_541 = ".readInt();" + NL + "                        if(length_";
  protected final String TEXT_542 = " == -1){";
  protected final String TEXT_543 = NL + "                        if(";
  protected final String TEXT_544 = ".readByte() == -1){";
  protected final String TEXT_545 = NL + "                            this.";
  protected final String TEXT_546 = " = null;" + NL + "                        }else{";
  protected final String TEXT_547 = NL + "                                this.";
  protected final String TEXT_548 = " = ";
  protected final String TEXT_549 = ".readBoolean();";
  protected final String TEXT_550 = NL + "                                this.";
  protected final String TEXT_551 = " = ";
  protected final String TEXT_552 = ".readByte();";
  protected final String TEXT_553 = NL + "                                this.";
  protected final String TEXT_554 = " = new byte[length_";
  protected final String TEXT_555 = "];";
  protected final String TEXT_556 = NL + "                                ";
  protected final String TEXT_557 = ".readFully(this.";
  protected final String TEXT_558 = ");";
  protected final String TEXT_559 = NL + "                                this.";
  protected final String TEXT_560 = " = ";
  protected final String TEXT_561 = ".readChar();";
  protected final String TEXT_562 = NL + "                                this.";
  protected final String TEXT_563 = " = new java.util.Date(";
  protected final String TEXT_564 = ".readLong());";
  protected final String TEXT_565 = NL + "                                this.";
  protected final String TEXT_566 = " = ";
  protected final String TEXT_567 = ".readDouble();";
  protected final String TEXT_568 = NL + "                                this.";
  protected final String TEXT_569 = " = ";
  protected final String TEXT_570 = ".readFloat();";
  protected final String TEXT_571 = NL + "                                int length_";
  protected final String TEXT_572 = " = ";
  protected final String TEXT_573 = ".readShort();" + NL + "                                byte[] bytes_";
  protected final String TEXT_574 = " = new byte[length_";
  protected final String TEXT_575 = "];";
  protected final String TEXT_576 = NL + "                                ";
  protected final String TEXT_577 = ".readFully(bytes_";
  protected final String TEXT_578 = ");" + NL + "                                int scale_";
  protected final String TEXT_579 = " = ";
  protected final String TEXT_580 = ".readShort();" + NL + "                                this.";
  protected final String TEXT_581 = " = new java.math.BigDecimal(new java.math.BigInteger(bytes_";
  protected final String TEXT_582 = "), scale_";
  protected final String TEXT_583 = ");";
  protected final String TEXT_584 = NL + "                                this.";
  protected final String TEXT_585 = " = ";
  protected final String TEXT_586 = ".readInt();";
  protected final String TEXT_587 = NL + "                                this.";
  protected final String TEXT_588 = " = ";
  protected final String TEXT_589 = ".readLong();";
  protected final String TEXT_590 = NL + "                                Don't support Object type: column--";
  protected final String TEXT_591 = NL + "                                this.";
  protected final String TEXT_592 = " = ";
  protected final String TEXT_593 = ".readShort();";
  protected final String TEXT_594 = NL + "                                int length_";
  protected final String TEXT_595 = " = ";
  protected final String TEXT_596 = ".readInt();" + NL + "                                byte[] bytes_";
  protected final String TEXT_597 = " = new byte[length_";
  protected final String TEXT_598 = "];";
  protected final String TEXT_599 = NL + "                                ";
  protected final String TEXT_600 = ".readFully(bytes_";
  protected final String TEXT_601 = ", 0, length_";
  protected final String TEXT_602 = ");" + NL + "                                this.";
  protected final String TEXT_603 = " = new String(bytes_";
  protected final String TEXT_604 = ", 0, length_";
  protected final String TEXT_605 = ", \"UTF-8\");";
  protected final String TEXT_606 = NL + "                                Don't support List type: column--";
  protected final String TEXT_607 = NL + "                                Don't support Document type: column--";
  protected final String TEXT_608 = NL + "                                Don't support Dynamic type: column--";
  protected final String TEXT_609 = NL + "                        }";
  protected final String TEXT_610 = NL + "            final int prime = 31;" + NL + "            int result = 1;";
  protected final String TEXT_611 = NL + "                            result = prime * result + (this.";
  protected final String TEXT_612 = " ? 1231 : 1237);";
  protected final String TEXT_613 = NL + "                            result = prime * result + (int) this.";
  protected final String TEXT_614 = ";";
  protected final String TEXT_615 = NL + "                        result = prime * result + java.util.Arrays.hashCode(this.";
  protected final String TEXT_616 = ");";
  protected final String TEXT_617 = NL + "                        result = prime * result + ((this.";
  protected final String TEXT_618 = " == null) ? 0 : this.";
  protected final String TEXT_619 = ".hashCode());";
  protected final String TEXT_620 = NL + "            return result;";
  protected final String TEXT_621 = NL + "            if (this == ";
  protected final String TEXT_622 = ") return true;" + NL + "            if (";
  protected final String TEXT_623 = " == null) return false;" + NL + "            if (getClass() != ";
  protected final String TEXT_624 = ".getClass()) return false;" + NL + "            final ";
  protected final String TEXT_625 = " other = (";
  protected final String TEXT_626 = ") ";
  protected final String TEXT_627 = ";";
  protected final String TEXT_628 = NL + "                        if (this.";
  protected final String TEXT_629 = " != other.";
  protected final String TEXT_630 = ")" + NL + "                            return false;";
  protected final String TEXT_631 = NL + "                        if(!java.util.Arrays.equals(this.";
  protected final String TEXT_632 = ", other.";
  protected final String TEXT_633 = ")) {" + NL + "                            return false;" + NL + "                        }";
  protected final String TEXT_634 = NL + "                        if (this.";
  protected final String TEXT_635 = " == null) {" + NL + "                            if (other.";
  protected final String TEXT_636 = " != null)" + NL + "                                return false;" + NL + "                        } else if (!this.";
  protected final String TEXT_637 = ".equals(other.";
  protected final String TEXT_638 = "))" + NL + "                            return false;";
  protected final String TEXT_639 = NL + "            return true;";
  protected final String TEXT_640 = NL + "            int returnValue = -1;";
  protected final String TEXT_641 = NL + "                    returnValue = checkNullsAndCompare(this.";
  protected final String TEXT_642 = ", ";
  protected final String TEXT_643 = ".";
  protected final String TEXT_644 = ");" + NL + "                    if(returnValue != 0) {" + NL + "                        return returnValue;" + NL + "                    }";
  protected final String TEXT_645 = NL + "            return returnValue;";
  protected final String TEXT_646 = NL + "            public static class ";
  protected final String TEXT_647 = " implements ";
  protected final String TEXT_648 = " {" + NL;
  protected final String TEXT_649 = NL + "                    public ";
  protected final String TEXT_650 = " ";
  protected final String TEXT_651 = NL + "                        = ' '";
  protected final String TEXT_652 = ";";
  protected final String TEXT_653 = NL;
  protected final String TEXT_654 = NL + NL + "                public int hashCode() {";
  protected final String TEXT_655 = NL + "                }" + NL + "" + NL + "                public boolean equals(Object obj) {";
  protected final String TEXT_656 = NL + "                }" + NL + "" + NL + "                public String toString() {" + NL + "                    StringBuilder sb = new StringBuilder();" + NL + "                    sb.append(super.toString());" + NL + "                    sb.append(\"[\");";
  protected final String TEXT_657 = NL + "                                sb.append(\"";
  protected final String TEXT_658 = "=\"+";
  protected final String TEXT_659 = ");";
  protected final String TEXT_660 = NL + "                                sb.append(\"";
  protected final String TEXT_661 = "=\"+String.valueOf(";
  protected final String TEXT_662 = "));";
  protected final String TEXT_663 = NL + "                    sb.append(\"]\");" + NL + "" + NL + "                    return sb.toString();" + NL + "                }" + NL + "" + NL + "                public void write(DataOutput out) throws IOException {";
  protected final String TEXT_664 = NL + "                }" + NL + "" + NL + "                public void readFields(DataInput in) throws IOException {";
  protected final String TEXT_665 = NL + "                }" + NL + "" + NL + "                public int compareTo(";
  protected final String TEXT_666 = " other) {";
  protected final String TEXT_667 = NL + "                }" + NL + "" + NL + "                private int checkNullsAndCompare(Object object1, Object object2) {" + NL + "                    int returnValue = 0;" + NL + "                    if (object1 instanceof Comparable && object2 instanceof Comparable) {" + NL + "                        returnValue = ((Comparable) object1).compareTo(object2);" + NL + "                    } else if (object1 != null && object2 != null) {" + NL + "                        returnValue = compareStrings(object1.toString(), object2.toString());" + NL + "                    } else if (object1 == null && object2 != null) {" + NL + "                        returnValue = 1;" + NL + "                    } else if (object1 != null && object2 == null) {" + NL + "                        returnValue = -1;" + NL + "                    } else {" + NL + "                        returnValue = 0;" + NL + "                    }" + NL + "" + NL + "                    return returnValue;" + NL + "                }" + NL + "" + NL + "                private int compareStrings(String string1, String string2) {" + NL + "                    return string1.compareTo(string2);" + NL + "                }" + NL;
  protected final String TEXT_668 = NL + "            }";
  protected final String TEXT_669 = NL + "                static{" + NL + "                    WritableComparator.define(";
  protected final String TEXT_670 = ".class, new ";
  protected final String TEXT_671 = "());" + NL + "                }";
  protected final String TEXT_672 = NL;
  protected final String TEXT_673 = "\t";
  protected final String TEXT_674 = NL + "\t\t\toutput_";
  protected final String TEXT_675 = ".collect(";
  protected final String TEXT_676 = ", ";
  protected final String TEXT_677 = ");" + NL + "\t\t";
  protected final String TEXT_678 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_679 = " extends MapReduceBase" + NL + "\t\t\t\timplements Mapper<";
  protected final String TEXT_680 = ", ";
  protected final String TEXT_681 = ", ";
  protected final String TEXT_682 = ", ";
  protected final String TEXT_683 = ">{" + NL + "" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\t";
  protected final String TEXT_684 = " ";
  protected final String TEXT_685 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_686 = NL + "\t\t\t\t\t";
  protected final String TEXT_687 = " ";
  protected final String TEXT_688 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_689 = NL + "\t\t\t\t";
  protected final String TEXT_690 = NL + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_691 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_692 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_693 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_694 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_695 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_696 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_697 = " = new ";
  protected final String TEXT_698 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_699 = NL + "\t\t\t\t\t";
  protected final String TEXT_700 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_701 = " = new ";
  protected final String TEXT_702 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_703 = NL + "\t\t\t\t\t";
  protected final String TEXT_704 = NL + "  \t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void map(";
  protected final String TEXT_705 = " key_";
  protected final String TEXT_706 = ", ";
  protected final String TEXT_707 = " value_";
  protected final String TEXT_708 = "," + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_709 = ", ";
  protected final String TEXT_710 = "> output_";
  protected final String TEXT_711 = ", Reporter reporter_";
  protected final String TEXT_712 = ") throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_713 = NL + "\t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_714 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_715 = NL + "\t\t\t\tChainReducer.addMapper(job, ";
  protected final String TEXT_716 = ".class, ";
  protected final String TEXT_717 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_718 = ".class, ";
  protected final String TEXT_719 = ".class, ";
  protected final String TEXT_720 = ".class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_721 = NL + "\t\t\t\tchainMapper.addMapper(job, ";
  protected final String TEXT_722 = ".class, ";
  protected final String TEXT_723 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_724 = ".class, ";
  protected final String TEXT_725 = ".class, ";
  protected final String TEXT_726 = ".class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_727 = NL + "\t\t\t\tchainMapper.setCid(\"";
  protected final String TEXT_728 = "\");" + NL + "\t\t\t";
  protected final String TEXT_729 = NL + "\t\t\t\tmos_";
  protected final String TEXT_730 = ".getCollector(\"";
  protected final String TEXT_731 = "\", reporter_";
  protected final String TEXT_732 = ").collect(";
  protected final String TEXT_733 = ", ";
  protected final String TEXT_734 = ");" + NL + "\t\t\t\t";
  protected final String TEXT_735 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_736 = " extends MapReduceBase" + NL + "\t\t\t\timplements Mapper<";
  protected final String TEXT_737 = ", ";
  protected final String TEXT_738 = ", ";
  protected final String TEXT_739 = ", WritableComparable>{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\tpublic MultipleOutputs mos_";
  protected final String TEXT_740 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_741 = " ";
  protected final String TEXT_742 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_743 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_744 = " ";
  protected final String TEXT_745 = " = null;" + NL + "\t\t\t\t\t";
  protected final String TEXT_746 = NL + "\t\t\t\t";
  protected final String TEXT_747 = NL + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_748 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_749 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_750 = ");" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_751 = " = new MultipleOutputs(job_";
  protected final String TEXT_752 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_753 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_754 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_755 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_756 = " = new ";
  protected final String TEXT_757 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_758 = NL + "\t\t\t\t\t";
  protected final String TEXT_759 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_760 = " = new ";
  protected final String TEXT_761 = "();" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_762 = NL + "\t\t\t\t\t";
  protected final String TEXT_763 = NL + "  \t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void map(";
  protected final String TEXT_764 = " key_";
  protected final String TEXT_765 = ", ";
  protected final String TEXT_766 = " value_";
  protected final String TEXT_767 = "," + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_768 = ", WritableComparable> output_";
  protected final String TEXT_769 = ", Reporter reporter_";
  protected final String TEXT_770 = ") throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_771 = NL + "\t\t\t\t}" + NL + "" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_772 = ".close();" + NL + "\t\t\t\t\t";
  protected final String TEXT_773 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_774 = NL + "\t\t\t\tChainReducer.addMapper(job, ";
  protected final String TEXT_775 = ".class, ";
  protected final String TEXT_776 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_777 = ".class, ";
  protected final String TEXT_778 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_779 = NL + "\t\t\t\tchainMapper.addMapper(job, ";
  protected final String TEXT_780 = ".class, ";
  protected final String TEXT_781 = ".class," + NL + "\t        \t\t";
  protected final String TEXT_782 = ".class, ";
  protected final String TEXT_783 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "\t\t\t";
  protected final String TEXT_784 = NL + "        \tMultipleOutputs.setWorkDir(job, genTempFolderForComponent(\"MultipleOutputs_";
  protected final String TEXT_785 = "\"));" + NL + "        \t";
  protected final String TEXT_786 = NL + "\t\t\t\tMultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_787 = "\", ";
  protected final String TEXT_788 = ".class, ";
  protected final String TEXT_789 = ".class);" + NL + "        \t";
  protected final String TEXT_790 = NL;
  protected final String TEXT_791 = "\t";
  protected final String TEXT_792 = NL + "\t\t\toutput_";
  protected final String TEXT_793 = ".collect(";
  protected final String TEXT_794 = ", ";
  protected final String TEXT_795 = ");" + NL + "\t\t";
  protected final String TEXT_796 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_797 = " extends MapReduceBase " + NL + "\t\t\t\timplements Reducer<";
  protected final String TEXT_798 = ", ";
  protected final String TEXT_799 = ", ";
  protected final String TEXT_800 = ", ";
  protected final String TEXT_801 = ">{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\t";
  protected final String TEXT_802 = " ";
  protected final String TEXT_803 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_804 = " ";
  protected final String TEXT_805 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_806 = NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_807 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_808 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_809 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_810 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_811 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_812 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_813 = " = new ";
  protected final String TEXT_814 = "(); " + NL + "\t\t\t\t\t";
  protected final String TEXT_815 = NL + "\t\t\t\t\t";
  protected final String TEXT_816 = " = new ";
  protected final String TEXT_817 = "();" + NL + "\t\t\t\t\t";
  protected final String TEXT_818 = NL + "  \t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void reduce(";
  protected final String TEXT_819 = " key_";
  protected final String TEXT_820 = ", Iterator<";
  protected final String TEXT_821 = "> values_";
  protected final String TEXT_822 = ", " + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_823 = ", ";
  protected final String TEXT_824 = "> output_";
  protected final String TEXT_825 = ", Reporter reporter_";
  protected final String TEXT_826 = ") throws IOException{" + NL + "\t\t\t\t    final OutputCollector<";
  protected final String TEXT_827 = ", ";
  protected final String TEXT_828 = "> outputCollect_";
  protected final String TEXT_829 = "=output_";
  protected final String TEXT_830 = ";" + NL + "\t\t\t\t\t";
  protected final String TEXT_831 = NL + "\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\t";
  protected final String TEXT_832 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_833 = NL + "\t\t\tChainReducer.setReducer(job, ";
  protected final String TEXT_834 = ".class, ";
  protected final String TEXT_835 = ".class," + NL + "        \t\t";
  protected final String TEXT_836 = ".class, ";
  protected final String TEXT_837 = ".class, ";
  protected final String TEXT_838 = ".class, true, new JobConf(false));" + NL + "\t\t";
  protected final String TEXT_839 = NL + "\t\t\tmos_";
  protected final String TEXT_840 = ".getCollector(\"";
  protected final String TEXT_841 = "\", reporter_";
  protected final String TEXT_842 = ").collect(";
  protected final String TEXT_843 = ", ";
  protected final String TEXT_844 = ");" + NL + "\t\t";
  protected final String TEXT_845 = NL + "\t\t\tpublic static class ";
  protected final String TEXT_846 = " extends MapReduceBase " + NL + "\t\t\t\timplements Reducer<";
  protected final String TEXT_847 = ", ";
  protected final String TEXT_848 = ", ";
  protected final String TEXT_849 = ", WritableComparable>{" + NL + "\t\t\t\tContextProperties context;" + NL + "\t\t\t\tGlobalVar globalMap;" + NL + "\t\t\t\tpublic MultipleOutputs mos_";
  protected final String TEXT_850 = ";" + NL + "\t\t\t\t";
  protected final String TEXT_851 = " ";
  protected final String TEXT_852 = " = null;" + NL + "\t\t\t\t";
  protected final String TEXT_853 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_854 = " ";
  protected final String TEXT_855 = " = null;\t" + NL + "\t\t\t\t\t";
  protected final String TEXT_856 = NL + "\t\t\t\t";
  protected final String TEXT_857 = NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void configure(JobConf job_";
  protected final String TEXT_858 = "){" + NL + "\t\t\t\t\tcontext = new ContextProperties(job_";
  protected final String TEXT_859 = ");" + NL + "\t\t\t\t\tglobalMap = new GlobalVar(job_";
  protected final String TEXT_860 = ");" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_861 = " = new MultipleOutputs(job_";
  protected final String TEXT_862 = ");" + NL + "\t\t\t\t\t";
  protected final String TEXT_863 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_864 = " = NullWritable.get();" + NL + "\t\t\t\t\t";
  protected final String TEXT_865 = NL + "\t\t\t\t\t\t";
  protected final String TEXT_866 = " = new ";
  protected final String TEXT_867 = "(); " + NL + "\t\t\t\t\t";
  protected final String TEXT_868 = NL + "\t\t\t\t\t";
  protected final String TEXT_869 = NL + "\t\t\t\t\t\t\t";
  protected final String TEXT_870 = " = new ";
  protected final String TEXT_871 = "();\t" + NL + "\t\t\t\t\t\t";
  protected final String TEXT_872 = NL + "\t\t\t\t\t";
  protected final String TEXT_873 = NL + "  \t\t\t\t}" + NL + "\t\t\t\t" + NL + "\t\t\t\tpublic void reduce(";
  protected final String TEXT_874 = " key_";
  protected final String TEXT_875 = ", Iterator<";
  protected final String TEXT_876 = "> values_";
  protected final String TEXT_877 = ", " + NL + "\t\t\t\t\tOutputCollector<";
  protected final String TEXT_878 = ", WritableComparable> output_";
  protected final String TEXT_879 = ", Reporter reporter_";
  protected final String TEXT_880 = ") throws IOException{" + NL + "\t\t\t\t    final Reporter reporterFinal_";
  protected final String TEXT_881 = "= reporter_";
  protected final String TEXT_882 = ";" + NL + "\t\t\t\t\t";
  protected final String TEXT_883 = NL + "\t\t\t\t}" + NL + "\t\t\t\t\t" + NL + "\t\t\t\tpublic void close() throws IOException{" + NL + "\t\t\t\t\tmos_";
  protected final String TEXT_884 = ".close();" + NL + "\t\t\t\t\t";
  protected final String TEXT_885 = NL + "  \t\t\t\t}" + NL + "    \t\t}" + NL + "    \t";
  protected final String TEXT_886 = NL + "\t\t\tChainReducer.setReducer(job, ";
  protected final String TEXT_887 = ".class, ";
  protected final String TEXT_888 = ".class," + NL + "        \t\t";
  protected final String TEXT_889 = ".class, ";
  protected final String TEXT_890 = ".class, WritableComparable.class, true, new JobConf(false));" + NL + "        \tMultipleOutputs.setWorkDir(job, genTempFolderForComponent(\"MultipleOutputs_";
  protected final String TEXT_891 = "\"));" + NL + "\t\t\t";
  protected final String TEXT_892 = NL + "\t\t\t\tMultipleOutputs.setKeyValue(job, \"";
  protected final String TEXT_893 = "\", ";
  protected final String TEXT_894 = ".class, ";
  protected final String TEXT_895 = ".class);" + NL + "        \t";
  protected final String TEXT_896 = NL + "\t\t\tjob.setCombinerClass(";
  protected final String TEXT_897 = ".class);" + NL + "\t\t";
  protected final String TEXT_898 = NL;
  protected final String TEXT_899 = NL;
  protected final String TEXT_900 = NL + "                ";
  protected final String TEXT_901 = ".";
  protected final String TEXT_902 = " = ";
  protected final String TEXT_903 = ";";
  protected final String TEXT_904 = NL + "                ";
  protected final String TEXT_905 = ".";
  protected final String TEXT_906 = " = ";
  protected final String TEXT_907 = ".";
  protected final String TEXT_908 = ";";
  protected final String TEXT_909 = NL + "                    boolean innerRejected_";
  protected final String TEXT_910 = " = false;";
  protected final String TEXT_911 = NL + "                        boolean forceLoop";
  protected final String TEXT_912 = " = false;" + NL + "                        if(innerRejected_";
  protected final String TEXT_913 = " || lookup";
  protected final String TEXT_914 = "Values.size() == 0){";
  protected final String TEXT_915 = NL + "                                innerRejected_";
  protected final String TEXT_916 = " = true;";
  protected final String TEXT_917 = NL + "                            forceLoop";
  protected final String TEXT_918 = " = true;" + NL + "                        }";
  protected final String TEXT_919 = NL + "                        ";
  protected final String TEXT_920 = "_filter_";
  protected final String TEXT_921 = "Struct ";
  protected final String TEXT_922 = " = null;" + NL + "                        for(int i_";
  protected final String TEXT_923 = " = 0; i_";
  protected final String TEXT_924 = " < lookup";
  protected final String TEXT_925 = "Values.size() || forceLoop";
  protected final String TEXT_926 = "; i_";
  protected final String TEXT_927 = "++){" + NL + "                            if(forceLoop";
  protected final String TEXT_928 = "){";
  protected final String TEXT_929 = NL + "                                ";
  protected final String TEXT_930 = " = ";
  protected final String TEXT_931 = "Default;" + NL + "                                i_";
  protected final String TEXT_932 = " = lookup";
  protected final String TEXT_933 = "Values.size();" + NL + "                                forceLoop";
  protected final String TEXT_934 = " = false;" + NL + "                            }else{";
  protected final String TEXT_935 = NL + "                                ";
  protected final String TEXT_936 = " = lookup";
  protected final String TEXT_937 = "Values.get(i_";
  protected final String TEXT_938 = ");";
  protected final String TEXT_939 = NL + "                                        if(!(";
  protected final String TEXT_940 = ")){";
  protected final String TEXT_941 = NL + "                                            ";
  protected final String TEXT_942 = " = ";
  protected final String TEXT_943 = "Default;";
  protected final String TEXT_944 = NL + "                                                innerRejected_";
  protected final String TEXT_945 = " = true;";
  protected final String TEXT_946 = NL + "                                        }";
  protected final String TEXT_947 = NL + "                            }";
  protected final String TEXT_948 = NL + "                                if(!innerRejected_";
  protected final String TEXT_949 = "){";
  protected final String TEXT_950 = NL + "                                        boolean rejected_";
  protected final String TEXT_951 = " = true;";
  protected final String TEXT_952 = NL + "                                                if(";
  protected final String TEXT_953 = "){";
  protected final String TEXT_954 = NL + "                                                        rejected_";
  protected final String TEXT_955 = " = false;";
  protected final String TEXT_956 = NL + "                                                    \t";
  protected final String TEXT_957 = ".";
  protected final String TEXT_958 = " = ";
  protected final String TEXT_959 = ";";
  protected final String TEXT_960 = NL + "                                                }";
  protected final String TEXT_961 = NL + "                                            if(rejected_";
  protected final String TEXT_962 = " ";
  protected final String TEXT_963 = "){";
  protected final String TEXT_964 = NL + "                                                        \t";
  protected final String TEXT_965 = ".";
  protected final String TEXT_966 = " = ";
  protected final String TEXT_967 = ";";
  protected final String TEXT_968 = NL + "                                            }";
  protected final String TEXT_969 = NL + "                                }";
  protected final String TEXT_970 = NL + "                                if(innerRejected_";
  protected final String TEXT_971 = "){";
  protected final String TEXT_972 = NL + "                                            if(";
  protected final String TEXT_973 = "){";
  protected final String TEXT_974 = NL + "                                                \t";
  protected final String TEXT_975 = ".";
  protected final String TEXT_976 = " = ";
  protected final String TEXT_977 = ";";
  protected final String TEXT_978 = NL + "                                            }";
  protected final String TEXT_979 = NL + "                                }";
  protected final String TEXT_980 = NL + "                            innerRejected_";
  protected final String TEXT_981 = " = false;";
  protected final String TEXT_982 = NL + "                        }";
  protected final String TEXT_983 = NL + "                class ";
  protected final String TEXT_984 = "{";
  protected final String TEXT_985 = NL + "                        ";
  protected final String TEXT_986 = " ";
  protected final String TEXT_987 = ";";
  protected final String TEXT_988 = NL + "                }";
  protected final String TEXT_989 = NL + "                ";
  protected final String TEXT_990 = NL + "                ";
  protected final String TEXT_991 = ".";
  protected final String TEXT_992 = " = ";
  protected final String TEXT_993 = ";";
  protected final String TEXT_994 = NL;
  protected final String TEXT_995 = NL + "                        class ";
  protected final String TEXT_996 = "{";
  protected final String TEXT_997 = NL + "                                ";
  protected final String TEXT_998 = " ";
  protected final String TEXT_999 = ";";
  protected final String TEXT_1000 = NL + "                        }";
  protected final String TEXT_1001 = NL + "                        ";
  protected final String TEXT_1002 = NL + "                ";
  protected final String TEXT_1003 = " ";
  protected final String TEXT_1004 = " = value_";
  protected final String TEXT_1005 = ";";
  protected final String TEXT_1006 = NL + "                    if(";
  protected final String TEXT_1007 = "){";
  protected final String TEXT_1008 = NL + "                        ";
  protected final String TEXT_1009 = ".";
  protected final String TEXT_1010 = " = ";
  protected final String TEXT_1011 = ";";
  protected final String TEXT_1012 = NL + "                    boolean rejected_";
  protected final String TEXT_1013 = " = true;";
  protected final String TEXT_1014 = NL + "                            if(";
  protected final String TEXT_1015 = "){";
  protected final String TEXT_1016 = NL + "                            rejected_";
  protected final String TEXT_1017 = " = false;";
  protected final String TEXT_1018 = NL + "\t                                ";
  protected final String TEXT_1019 = ".";
  protected final String TEXT_1020 = " = ";
  protected final String TEXT_1021 = ";";
  protected final String TEXT_1022 = NL + "                            }";
  protected final String TEXT_1023 = NL + "                        if(rejected_";
  protected final String TEXT_1024 = " ";
  protected final String TEXT_1025 = "){";
  protected final String TEXT_1026 = NL + "                                    \t";
  protected final String TEXT_1027 = ".";
  protected final String TEXT_1028 = " = ";
  protected final String TEXT_1029 = ";";
  protected final String TEXT_1030 = NL + "                        }";
  protected final String TEXT_1031 = NL + "                    }";
  protected final String TEXT_1032 = NL + "        ";
  protected final String TEXT_1033 = " ";
  protected final String TEXT_1034 = " = new ";
  protected final String TEXT_1035 = "();";
  protected final String TEXT_1036 = NL + "            ";
  protected final String TEXT_1037 = ".";
  protected final String TEXT_1038 = " = ";
  protected final String TEXT_1039 = ".";
  protected final String TEXT_1040 = ";";
  protected final String TEXT_1041 = NL + "        ";
  protected final String TEXT_1042 = " = null;";
  protected final String TEXT_1043 = NL + "            private java.util.Map<";
  protected final String TEXT_1044 = ", List<";
  protected final String TEXT_1045 = ">> ";
  protected final String TEXT_1046 = " = null;";
  protected final String TEXT_1047 = NL + "            ";
  protected final String TEXT_1048 = " = new java.util.HashMap<";
  protected final String TEXT_1049 = ", List<";
  protected final String TEXT_1050 = ">>();";
  protected final String TEXT_1051 = NL + "            List<";
  protected final String TEXT_1052 = "> lookupValues = ";
  protected final String TEXT_1053 = ".get(lookupKey);" + NL + "            if (null == lookupValues) {" + NL + "                lookupValues = new java.util.ArrayList<";
  protected final String TEXT_1054 = ">(1);";
  protected final String TEXT_1055 = NL + "                ";
  protected final String TEXT_1056 = ".put(lookupKey, lookupValues);" + NL + "            }";
  protected final String TEXT_1057 = NL + "            lookupValues.add(lookupValue);";
  protected final String TEXT_1058 = NL + "            private java.util.Map<";
  protected final String TEXT_1059 = ", ";
  protected final String TEXT_1060 = "> ";
  protected final String TEXT_1061 = " = null;";
  protected final String TEXT_1062 = NL + "            ";
  protected final String TEXT_1063 = " = new java.util.HashMap<";
  protected final String TEXT_1064 = ", ";
  protected final String TEXT_1065 = ">();";
  protected final String TEXT_1066 = NL + "            if (!";
  protected final String TEXT_1067 = ".containsKey(lookupKey)) {";
  protected final String TEXT_1068 = NL + "                ";
  protected final String TEXT_1069 = ".put(lookupKey, lookupValue);" + NL + "            }";
  protected final String TEXT_1070 = NL + "            private java.util.BitSet ";
  protected final String TEXT_1071 = " = null;" + NL + "            private List<";
  protected final String TEXT_1072 = "> ";
  protected final String TEXT_1073 = "DefaultSingle =" + NL + "                    java.util.Arrays.asList(new ";
  protected final String TEXT_1074 = "());";
  protected final String TEXT_1075 = NL + "            ";
  protected final String TEXT_1076 = " = new java.util.BitSet();";
  protected final String TEXT_1077 = NL + "            ";
  protected final String TEXT_1078 = ".set(lookupKey.";
  protected final String TEXT_1079 = ");";
  protected final String TEXT_1080 = NL + "            ";
  protected final String TEXT_1081 = "_filter_";
  protected final String TEXT_1082 = "Struct ";
  protected final String TEXT_1083 = "Default = new ";
  protected final String TEXT_1084 = "_filter_";
  protected final String TEXT_1085 = "Struct();";
  protected final String TEXT_1086 = NL + "            try {" + NL + "                NullWritable ignoredKey = NullWritable.get();" + NL + "" + NL + "                // Get the locally cached lookup inputs." + NL + "                FileSystem fs = FileSystem.getLocal(job_";
  protected final String TEXT_1087 = ");" + NL + "                Path lookupSource = fs.makeQualified(new Path(\"./\" + ";
  protected final String TEXT_1088 = "));" + NL + "" + NL + "                // Reuse the input format to read the records into the cache" + NL + "                // by overriding the input dir in a temporary JobConf.";
  protected final String TEXT_1089 = NL + "                ";
  protected final String TEXT_1090 = "StructInputFormat inputFormat = ReflectionUtils.newInstance(";
  protected final String TEXT_1091 = "StructInputFormat.class, job_";
  protected final String TEXT_1092 = ");" + NL + "                JobConf tmpJob = new JobConf();" + NL + "                tmpJob.setBoolean(\"mapred.input.dir.recursive\", true);" + NL + "                FileInputFormat.addInputPath(tmpJob, lookupSource);" + NL;
  protected final String TEXT_1093 = NL + "                    inputFormat.setIgnoreInputPath(true);";
  protected final String TEXT_1094 = NL + NL + "                for (InputSplit split : inputFormat.getSplits(tmpJob, 0)) {" + NL + "                    RecordReader<NullWritable, ";
  protected final String TEXT_1095 = "Struct> reader = inputFormat.getRecordReader(split, tmpJob, null);" + NL;
  protected final String TEXT_1096 = NL + "                    ";
  protected final String TEXT_1097 = "Struct ";
  protected final String TEXT_1098 = " = new ";
  protected final String TEXT_1099 = "Struct();" + NL + "                    while (reader.next(ignoredKey, ";
  protected final String TEXT_1100 = ")) {";
  protected final String TEXT_1101 = NL + "                        ";
  protected final String TEXT_1102 = " lookupKey = new ";
  protected final String TEXT_1103 = "();" + NL;
  protected final String TEXT_1104 = NL + "                                if(!(";
  protected final String TEXT_1105 = ")){" + NL + "                                    continue;" + NL + "                                }";
  protected final String TEXT_1106 = NL + "                    }" + NL + "                }" + NL + "            } catch (IOException e) {" + NL + "                throw new RuntimeException(e);" + NL + "            }";
  protected final String TEXT_1107 = NL + "        ";
  protected final String TEXT_1108 = " ";
  protected final String TEXT_1109 = " = value_";
  protected final String TEXT_1110 = ";";
  protected final String TEXT_1111 = NL + "            if (!(";
  protected final String TEXT_1112 = "))" + NL + "                return;";
  protected final String TEXT_1113 = NL;
  protected final String TEXT_1114 = NL + "        ";
  protected final String TEXT_1115 = " lookupKey = new ";
  protected final String TEXT_1116 = "();" + NL;
  protected final String TEXT_1117 = NL + "            List<";
  protected final String TEXT_1118 = "_filter_";
  protected final String TEXT_1119 = "Struct> lookup";
  protected final String TEXT_1120 = "Values =";
  protected final String TEXT_1121 = NL + "                    ";
  protected final String TEXT_1122 = ";" + NL + "            if (null == lookup";
  protected final String TEXT_1123 = "Values)" + NL + "                lookup";
  protected final String TEXT_1124 = "Values = java.util.Collections.emptyList();";
  protected final String TEXT_1125 = NL + "                int ";
  protected final String TEXT_1126 = ";";
  protected final String TEXT_1127 = NL + "                ";
  protected final String TEXT_1128 = ".writeInt(";
  protected final String TEXT_1129 = ");";
  protected final String TEXT_1130 = NL + "                ";
  protected final String TEXT_1131 = " = ";
  protected final String TEXT_1132 = ".readInt();";
  protected final String TEXT_1133 = NL + "                public void set";
  protected final String TEXT_1134 = "(int tagIndex){" + NL + "                    this.";
  protected final String TEXT_1135 = " = tagIndex;" + NL + "                }" + NL + "                public int get";
  protected final String TEXT_1136 = "(){" + NL + "                    return ";
  protected final String TEXT_1137 = ";" + NL + "                }" + NL + "                public int compareToWithTagged(";
  protected final String TEXT_1138 = " other){" + NL + "                    int cmp = this.compareTo(other);" + NL + "                    if (cmp != 0) {" + NL + "                        return cmp;" + NL + "                    } else {" + NL + "                        Integer thisTag = ";
  protected final String TEXT_1139 = ";" + NL + "                        Integer otherTag = other.get";
  protected final String TEXT_1140 = "();" + NL + "                        return thisTag.compareTo(otherTag);" + NL + "                    }" + NL + "                }";
  protected final String TEXT_1141 = NL + "                public static class ";
  protected final String TEXT_1142 = " implements Writable {" + NL + "                    int tagIndex;" + NL + "                    Writable mapOutputStruct;" + NL + "                    public Writable getData(){" + NL + "                        return mapOutputStruct;" + NL + "                    }" + NL + "                    public void setTagIndex(int tagIndex){" + NL + "                        this.tagIndex = tagIndex;" + NL + "                    }" + NL + "                    public int getTagIndex(){" + NL + "                        return tagIndex;" + NL + "                    }" + NL + "                    public void setData(Writable mapOutputStruct){" + NL + "                        this.mapOutputStruct = mapOutputStruct;" + NL + "                    }" + NL + "                    public void write(DataOutput out) throws IOException{" + NL + "                        out.writeInt(tagIndex);" + NL + "                        mapOutputStruct.write(out);" + NL + "                    }" + NL + "                    public void readFields(DataInput in) throws IOException{" + NL + "                        tagIndex = in.readInt();" + NL + "                        if (tagIndex == ";
  protected final String TEXT_1143 = ") {" + NL + "                            this.mapOutputStruct = new ";
  protected final String TEXT_1144 = "_filter_";
  protected final String TEXT_1145 = "Struct();";
  protected final String TEXT_1146 = NL + "                            } else if (tagIndex == ";
  protected final String TEXT_1147 = ") {" + NL + "                                this.mapOutputStruct = new ";
  protected final String TEXT_1148 = "_filter_";
  protected final String TEXT_1149 = "Struct();";
  protected final String TEXT_1150 = NL + "                        }" + NL + "                        this.mapOutputStruct.readFields(in);" + NL + "                    }" + NL + "                    public ";
  protected final String TEXT_1151 = " clone(Configuration conf){" + NL + "                        return (";
  protected final String TEXT_1152 = ")WritableUtils.clone(this, conf);" + NL + "                    }" + NL + "                }";
  protected final String TEXT_1153 = NL + "                ";
  protected final String TEXT_1154 = " mainTableFilter_";
  protected final String TEXT_1155 = " = null;";
  protected final String TEXT_1156 = NL + "                mainTableFilter_";
  protected final String TEXT_1157 = " = new ";
  protected final String TEXT_1158 = "();";
  protected final String TEXT_1159 = NL + "                ";
  protected final String TEXT_1160 = " ";
  protected final String TEXT_1161 = " = value_";
  protected final String TEXT_1162 = ";";
  protected final String TEXT_1163 = NL + "                    mainTableFilter_";
  protected final String TEXT_1164 = ".";
  protected final String TEXT_1165 = " = ";
  protected final String TEXT_1166 = ".";
  protected final String TEXT_1167 = ";";
  protected final String TEXT_1168 = NL + "                int tagIndex_";
  protected final String TEXT_1169 = " = ";
  protected final String TEXT_1170 = ";";
  protected final String TEXT_1171 = NL + "                    if(";
  protected final String TEXT_1172 = "){";
  protected final String TEXT_1173 = NL + "                ";
  protected final String TEXT_1174 = ".set";
  protected final String TEXT_1175 = "(tagIndex_";
  protected final String TEXT_1176 = ");";
  protected final String TEXT_1177 = NL + "                ";
  protected final String TEXT_1178 = ".setTagIndex(tagIndex_";
  protected final String TEXT_1179 = ");";
  protected final String TEXT_1180 = NL + "                ";
  protected final String TEXT_1181 = ".setData(mainTableFilter_";
  protected final String TEXT_1182 = ");";
  protected final String TEXT_1183 = NL + "                    }";
  protected final String TEXT_1184 = NL + "                    ";
  protected final String TEXT_1185 = " lookupTableFilter_";
  protected final String TEXT_1186 = " = null;";
  protected final String TEXT_1187 = NL + "                    lookupTableFilter_";
  protected final String TEXT_1188 = " = new ";
  protected final String TEXT_1189 = "();";
  protected final String TEXT_1190 = NL + "                    ";
  protected final String TEXT_1191 = " ";
  protected final String TEXT_1192 = " = value_";
  protected final String TEXT_1193 = ";";
  protected final String TEXT_1194 = NL + "                            if(!(";
  protected final String TEXT_1195 = ")){" + NL + "                                return;" + NL + "                            }";
  protected final String TEXT_1196 = NL + "                        lookupTableFilter_";
  protected final String TEXT_1197 = ".";
  protected final String TEXT_1198 = " = ";
  protected final String TEXT_1199 = ".";
  protected final String TEXT_1200 = ";";
  protected final String TEXT_1201 = NL + "                    int tagIndex_";
  protected final String TEXT_1202 = " = ";
  protected final String TEXT_1203 = ";";
  protected final String TEXT_1204 = NL + "                    ";
  protected final String TEXT_1205 = ".set";
  protected final String TEXT_1206 = "(tagIndex_";
  protected final String TEXT_1207 = ");";
  protected final String TEXT_1208 = NL + "                    ";
  protected final String TEXT_1209 = ".setTagIndex(tagIndex_";
  protected final String TEXT_1210 = ");";
  protected final String TEXT_1211 = NL + "                    ";
  protected final String TEXT_1212 = ".setData(lookupTableFilter_";
  protected final String TEXT_1213 = ");";
  protected final String TEXT_1214 = NL + "        static class JoinKeyPartitioner_";
  protected final String TEXT_1215 = " implements org.apache.hadoop.mapred.Partitioner<";
  protected final String TEXT_1216 = ", Writable>{" + NL + "            public int getPartition(";
  protected final String TEXT_1217 = " key, Writable value, int numPartitions) {" + NL + "                return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;" + NL + "            }" + NL + "            public void configure(JobConf arg0) {" + NL + "            }" + NL + "        }";
  protected final String TEXT_1218 = NL + "                Integer tagIndex1_";
  protected final String TEXT_1219 = " = readInt(b1, pos1);" + NL + "                Integer tagIndex2_";
  protected final String TEXT_1220 = " = readInt(b2, pos2);" + NL + "                comp = tagIndex1_";
  protected final String TEXT_1221 = ".compareTo(tagIndex2_";
  protected final String TEXT_1222 = ");";
  protected final String TEXT_1223 = NL + "                ";
  protected final String TEXT_1224 = " ";
  protected final String TEXT_1225 = " = key_";
  protected final String TEXT_1226 = ";" + NL + "                while(values_";
  protected final String TEXT_1227 = ".hasNext()){";
  protected final String TEXT_1228 = NL + "                    ";
  protected final String TEXT_1229 = " ";
  protected final String TEXT_1230 = " = values_";
  protected final String TEXT_1231 = ".next();";
  protected final String TEXT_1232 = NL + "                            boolean find";
  protected final String TEXT_1233 = " = false;" + NL + "                            if(";
  protected final String TEXT_1234 = " == ";
  protected final String TEXT_1235 = ".getTagIndex()){" + NL + "                                if(!find";
  protected final String TEXT_1236 = "){";
  protected final String TEXT_1237 = NL + "                                    find";
  protected final String TEXT_1238 = " = true;" + NL + "                                }" + NL + "                                continue;" + NL + "                            }";
  protected final String TEXT_1239 = NL + "                }";
  protected final String TEXT_1240 = NL + "                private Configuration conf_";
  protected final String TEXT_1241 = ";";
  protected final String TEXT_1242 = NL + "                conf_";
  protected final String TEXT_1243 = " = job_";
  protected final String TEXT_1244 = ";";
  protected final String TEXT_1245 = NL + "                    List<";
  protected final String TEXT_1246 = "_filter_";
  protected final String TEXT_1247 = "Struct> lookup";
  protected final String TEXT_1248 = "Values = new java.util.ArrayList<";
  protected final String TEXT_1249 = "_filter_";
  protected final String TEXT_1250 = "Struct>();";
  protected final String TEXT_1251 = NL + "                    ";
  protected final String TEXT_1252 = "_filter_";
  protected final String TEXT_1253 = "Struct ";
  protected final String TEXT_1254 = "Default = new ";
  protected final String TEXT_1255 = "_filter_";
  protected final String TEXT_1256 = "Struct();";
  protected final String TEXT_1257 = NL + "                ";
  protected final String TEXT_1258 = " lookupValue_";
  protected final String TEXT_1259 = " = null;" + NL + "                while (values_";
  protected final String TEXT_1260 = ".hasNext()) {" + NL + "                    lookupValue_";
  protected final String TEXT_1261 = " = values_";
  protected final String TEXT_1262 = ".next();";
  protected final String TEXT_1263 = NL + "                        if(";
  protected final String TEXT_1264 = " == lookupValue_";
  protected final String TEXT_1265 = ".getTagIndex()) {";
  protected final String TEXT_1266 = NL + "                                        if(lookup";
  protected final String TEXT_1267 = "Values.size() == 1){" + NL + "                                            continue;" + NL + "                                        }";
  protected final String TEXT_1268 = NL + "                            ";
  protected final String TEXT_1269 = " copyValue_";
  protected final String TEXT_1270 = " = ((";
  protected final String TEXT_1271 = ")lookupValue_";
  protected final String TEXT_1272 = ").clone(conf_";
  protected final String TEXT_1273 = ");" + NL + "                            lookup";
  protected final String TEXT_1274 = "Values.add((";
  protected final String TEXT_1275 = "_filter_";
  protected final String TEXT_1276 = "Struct)copyValue_";
  protected final String TEXT_1277 = ".getData());" + NL + "                            continue;" + NL + "                        }";
  protected final String TEXT_1278 = NL + "                   if(";
  protected final String TEXT_1279 = " == lookupValue_";
  protected final String TEXT_1280 = ".getTagIndex()) {" + NL + "                       break;" + NL + "                   }" + NL + "                }" + NL;
  protected final String TEXT_1281 = NL + "                while (lookupValue_";
  protected final String TEXT_1282 = " != null && ";
  protected final String TEXT_1283 = " == lookupValue_";
  protected final String TEXT_1284 = ".getTagIndex()" + NL + "                        || values_";
  protected final String TEXT_1285 = ".hasNext()) {";
  protected final String TEXT_1286 = NL + "                    ";
  protected final String TEXT_1287 = " value_";
  protected final String TEXT_1288 = " = lookupValue_";
  protected final String TEXT_1289 = " != null" + NL + "                            ? lookupValue_";
  protected final String TEXT_1290 = " : values_";
  protected final String TEXT_1291 = ".next();" + NL + "                    lookupValue_";
  protected final String TEXT_1292 = " = null;" + NL;
  protected final String TEXT_1293 = NL + "                    ";
  protected final String TEXT_1294 = "_filter_";
  protected final String TEXT_1295 = "Struct ";
  protected final String TEXT_1296 = " = (";
  protected final String TEXT_1297 = "_filter_";
  protected final String TEXT_1298 = "Struct)value_";
  protected final String TEXT_1299 = ".getData();" + NL;
  protected final String TEXT_1300 = NL + "                }";

  public String generate(Object argument)
  {
    final StringBuffer stringBuffer = new StringBuffer();
    
final BigDataCodeGeneratorArgument codeGenArgument = (BigDataCodeGeneratorArgument)argument;
final MapperComponent node = (MapperComponent)codeGenArgument.getArgument();
final String cid = node.getUniqueName();

    stringBuffer.append(TEXT_1);
    stringBuffer.append(TEXT_2);
    
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
		
    stringBuffer.append(TEXT_3);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_4);
    
		}
		private void addLesser(String columnName){
		
    stringBuffer.append(TEXT_5);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_6);
    	
		}
		private void genGreater(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_7);
    	
			}else{
			
    stringBuffer.append(TEXT_8);
    
			}
		}
		private void genLesser(String columnName){
			if(criterias.get(columnName)){
			
    stringBuffer.append(TEXT_9);
    	
			}else{
			
    stringBuffer.append(TEXT_10);
    
			}
		}
		
		private void skipColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
    stringBuffer.append(TEXT_11);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_12);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_13);
    
				}else{
	       		
    stringBuffer.append(TEXT_14);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_15);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_16);
    
	        	}
				
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
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_23);
    }
    stringBuffer.append(TEXT_24);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_25);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_26);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_27);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_28);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_29);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_30);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	            
    stringBuffer.append(TEXT_31);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_32);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_33);
    
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_34);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_35);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_36);
    
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_37);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_38);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_39);
    
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_40);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_41);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_42);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_43);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_44);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_45);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_46);
    
	            }
				
    if(nullable){
    stringBuffer.append(TEXT_47);
    
        	}
		}
		
		private void compareColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
				if(typeToGenerate.equals("byte[]")){
				
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
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_54);
    
				}else{
	       		
    stringBuffer.append(TEXT_55);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_56);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_57);
    
	        	}
				
    stringBuffer.append(TEXT_58);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_59);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_60);
    lesser(columnName);
    stringBuffer.append(TEXT_61);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_62);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_63);
    greater(columnName);
    stringBuffer.append(TEXT_64);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_65);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_66);
    }
    stringBuffer.append(TEXT_67);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_68);
    greater(columnName);
    stringBuffer.append(TEXT_69);
    lesser(columnName);
    stringBuffer.append(TEXT_70);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_71);
    greater(columnName);
    stringBuffer.append(TEXT_72);
    lesser(columnName);
    stringBuffer.append(TEXT_73);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
    stringBuffer.append(TEXT_74);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_75);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_76);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_77);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_78);
    greater(columnName);
    stringBuffer.append(TEXT_79);
    lesser(columnName);
    stringBuffer.append(TEXT_80);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_81);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_82);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_83);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_84);
    greater(columnName);
    stringBuffer.append(TEXT_85);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_86);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_87);
    lesser(columnName);
    stringBuffer.append(TEXT_88);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_89);
    greater(columnName);
    stringBuffer.append(TEXT_90);
    lesser(columnName);
    stringBuffer.append(TEXT_91);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_92);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_93);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_94);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_95);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_96);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_97);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_98);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_99);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_100);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_101);
    greater(columnName);
    stringBuffer.append(TEXT_102);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_103);
    lesser(columnName);
    stringBuffer.append(TEXT_104);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_105);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_106);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_107);
    greater(columnName);
    stringBuffer.append(TEXT_108);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_109);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_110);
    lesser(columnName);
    stringBuffer.append(TEXT_111);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_112);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_113);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_114);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_115);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_116);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_117);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_118);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_119);
    greater(columnName);
    stringBuffer.append(TEXT_120);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_121);
    lesser(columnName);
    stringBuffer.append(TEXT_122);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_123);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_124);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_125);
    greater(columnName);
    stringBuffer.append(TEXT_126);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_127);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_128);
    lesser(columnName);
    stringBuffer.append(TEXT_129);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_130);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_131);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_132);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_133);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_134);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_135);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_136);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_137);
    greater(columnName);
    stringBuffer.append(TEXT_138);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_139);
    lesser(columnName);
    stringBuffer.append(TEXT_140);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_141);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_142);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_143);
    greater(columnName);
    stringBuffer.append(TEXT_144);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_145);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_146);
    lesser(columnName);
    stringBuffer.append(TEXT_147);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
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
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_164);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_165);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_166);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_167);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_168);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_169);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_170);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_171);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_172);
    }
    stringBuffer.append(TEXT_173);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_174);
    greater(columnName);
    stringBuffer.append(TEXT_175);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_176);
    lesser(columnName);
    stringBuffer.append(TEXT_177);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_178);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_179);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_180);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_181);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_182);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_183);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_184);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_185);
    greater(columnName);
    stringBuffer.append(TEXT_186);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_187);
    lesser(columnName);
    stringBuffer.append(TEXT_188);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_189);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_190);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_191);
    greater(columnName);
    stringBuffer.append(TEXT_192);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_193);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_194);
    lesser(columnName);
    stringBuffer.append(TEXT_195);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_196);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_197);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_198);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_199);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_200);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_201);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_202);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_203);
    greater(columnName);
    stringBuffer.append(TEXT_204);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_205);
    lesser(columnName);
    stringBuffer.append(TEXT_206);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_207);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_208);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_209);
    greater(columnName);
    stringBuffer.append(TEXT_210);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_211);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_212);
    lesser(columnName);
    stringBuffer.append(TEXT_213);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_214);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_215);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_216);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_217);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_218);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_219);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_220);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_221);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_222);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_223);
    greater(columnName);
    stringBuffer.append(TEXT_224);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_225);
    lesser(columnName);
    stringBuffer.append(TEXT_226);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_227);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_228);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_229);
    greater(columnName);
    stringBuffer.append(TEXT_230);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_231);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_232);
    lesser(columnName);
    stringBuffer.append(TEXT_233);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
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
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_255);
    greater(columnName);
    stringBuffer.append(TEXT_256);
    lesser(columnName);
    stringBuffer.append(TEXT_257);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_258);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_259);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_260);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_261);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_262);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_263);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_264);
    
        	}
		}
		
		public void compareColumns(){
			if(columns != null){
				if(unorder){
				
    stringBuffer.append(TEXT_265);
    
				}
	            for(IMetadataColumn column : columns){
	            	String columnName = column.getLabel();
                	if(orders.contains(columnName)){
                	
    stringBuffer.append(TEXT_266);
    compareColumn(column);
    stringBuffer.append(TEXT_267);
    
                	}else{
                	
    stringBuffer.append(TEXT_268);
    skipColumn(column);
    stringBuffer.append(TEXT_269);
    	
	               	}
				}
				if(unorder){
					for(String columnName : orders){
					
    stringBuffer.append(TEXT_270);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_271);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_272);
    genGreater(columnName);
    stringBuffer.append(TEXT_273);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_274);
    genLesser(columnName);
    stringBuffer.append(TEXT_275);
    	
					}
				}
			}
		}
		
		public void compareAfterColumns(){
		}
		
		public void generateCode(){
		
    stringBuffer.append(TEXT_276);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_277);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_278);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_279);
    compareColumns();
    stringBuffer.append(TEXT_280);
    compareAfterColumns();
    stringBuffer.append(TEXT_281);
    if(genObjectCompare){
    stringBuffer.append(TEXT_282);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_283);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_284);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_285);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_286);
    
						for(IMetadataColumn column : columns){
			            	String columnName = column.getLabel();
		                	compareObjectColumn(column);
						}
						
    stringBuffer.append(TEXT_287);
    }
    stringBuffer.append(TEXT_288);
    
		}
		
		private void compareObjectColumn(IMetadataColumn column){
			boolean nullable = !JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable());
			String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
			String pattern = column.getPattern() == null || column.getPattern().trim().length() == 0 ? null : column.getPattern();
			String columnName = column.getLabel();
			if(nullable){
			
    stringBuffer.append(TEXT_289);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_290);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_291);
    lesser(columnName);
    stringBuffer.append(TEXT_292);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_293);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_294);
    greater(columnName);
    stringBuffer.append(TEXT_295);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_296);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_297);
    }
    stringBuffer.append(TEXT_298);
    
				if(typeToGenerate.equalsIgnoreCase("Boolean")){
	        	
    stringBuffer.append(TEXT_299);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_300);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_301);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_302);
    greater(columnName);
    stringBuffer.append(TEXT_303);
    lesser(columnName);
    stringBuffer.append(TEXT_304);
    
	        	}else if(typeToGenerate.equalsIgnoreCase("Byte")){
	            
    stringBuffer.append(TEXT_305);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_306);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_307);
    greater(columnName);
    stringBuffer.append(TEXT_308);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_309);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_310);
    lesser(columnName);
    stringBuffer.append(TEXT_311);
    
	            }else if(typeToGenerate.equals("byte[]")){
	            
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
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_320);
    greater(columnName);
    stringBuffer.append(TEXT_321);
    lesser(columnName);
    stringBuffer.append(TEXT_322);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Char") || typeToGenerate.equalsIgnoreCase("Character")){
	            
    stringBuffer.append(TEXT_323);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_324);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_325);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_326);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_327);
    greater(columnName);
    stringBuffer.append(TEXT_328);
    lesser(columnName);
    stringBuffer.append(TEXT_329);
    
	            }else if(typeToGenerate.equals("java.util.Date")){
	        	
    stringBuffer.append(TEXT_330);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_331);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_332);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_333);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_334);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_335);
    stringBuffer.append(pattern);
    stringBuffer.append(TEXT_336);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_337);
    greater(columnName);
    stringBuffer.append(TEXT_338);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_339);
    lesser(columnName);
    stringBuffer.append(TEXT_340);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_341);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_342);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_343);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_344);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_345);
    greater(columnName);
    stringBuffer.append(TEXT_346);
    lesser(columnName);
    stringBuffer.append(TEXT_347);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Double")){
	            
    stringBuffer.append(TEXT_348);
    
	            	if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_349);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_350);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_351);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_352);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_353);
    greater(columnName);
    stringBuffer.append(TEXT_354);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_355);
    lesser(columnName);
    stringBuffer.append(TEXT_356);
    	
	            	}else{
	            	
    stringBuffer.append(TEXT_357);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_358);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_359);
    greater(columnName);
    stringBuffer.append(TEXT_360);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_361);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_362);
    lesser(columnName);
    stringBuffer.append(TEXT_363);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Float")){
	            
    stringBuffer.append(TEXT_364);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_365);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_366);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_367);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_368);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_369);
    greater(columnName);
    stringBuffer.append(TEXT_370);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_371);
    lesser(columnName);
    stringBuffer.append(TEXT_372);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_373);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_374);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_375);
    greater(columnName);
    stringBuffer.append(TEXT_376);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_377);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_378);
    lesser(columnName);
    stringBuffer.append(TEXT_379);
    
	            	}
	            }else if(typeToGenerate.equals("BigDecimal")){
	            
    stringBuffer.append(TEXT_380);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_381);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_382);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_383);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_384);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_385);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_386);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_387);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_388);
    }
    stringBuffer.append(TEXT_389);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_390);
    greater(columnName);
    stringBuffer.append(TEXT_391);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_392);
    lesser(columnName);
    stringBuffer.append(TEXT_393);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Integer") || typeToGenerate.equalsIgnoreCase("int")){
	            
    stringBuffer.append(TEXT_394);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_395);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_396);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_397);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_398);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_399);
    greater(columnName);
    stringBuffer.append(TEXT_400);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_401);
    lesser(columnName);
    stringBuffer.append(TEXT_402);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_403);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_404);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_405);
    greater(columnName);
    stringBuffer.append(TEXT_406);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_407);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_408);
    lesser(columnName);
    stringBuffer.append(TEXT_409);
    
	            	}
	            }else if(typeToGenerate.equalsIgnoreCase("Long")){
	            
    stringBuffer.append(TEXT_410);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_411);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_412);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_413);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_414);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_415);
    greater(columnName);
    stringBuffer.append(TEXT_416);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_417);
    lesser(columnName);
    stringBuffer.append(TEXT_418);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_419);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_420);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_421);
    greater(columnName);
    stringBuffer.append(TEXT_422);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_423);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_424);
    lesser(columnName);
    stringBuffer.append(TEXT_425);
    
	            	}
	            }else if(typeToGenerate.equals("Object")){
	            
    stringBuffer.append(TEXT_426);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_427);
    
	            }else if(typeToGenerate.equalsIgnoreCase("Short")){
	            
    stringBuffer.append(TEXT_428);
    
	                if(!sortTypes.get(columnName)){
	            	
    stringBuffer.append(TEXT_429);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_430);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_431);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_432);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_433);
    greater(columnName);
    stringBuffer.append(TEXT_434);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_435);
    lesser(columnName);
    stringBuffer.append(TEXT_436);
    	
	            	}else{
	                
    stringBuffer.append(TEXT_437);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_438);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_439);
    greater(columnName);
    stringBuffer.append(TEXT_440);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_441);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_442);
    lesser(columnName);
    stringBuffer.append(TEXT_443);
    
	            	}
	            }else if(typeToGenerate.equals("String")){
	            
    stringBuffer.append(TEXT_444);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_445);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_446);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_447);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_448);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_449);
    greater(columnName);
    stringBuffer.append(TEXT_450);
    lesser(columnName);
    stringBuffer.append(TEXT_451);
    
	            }else if(typeToGenerate.equals("List")){
	            
    stringBuffer.append(TEXT_452);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_453);
    
	            }else if(typeToGenerate.equals("Doucument")){
	            
    stringBuffer.append(TEXT_454);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_455);
    
	            }else if(typeToGenerate.equals("Dynamic")){
	            
    stringBuffer.append(TEXT_456);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_457);
    
	            }            	
            	
    if(nullable){
    stringBuffer.append(TEXT_458);
    
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
                        
    stringBuffer.append(TEXT_459);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_460);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_461);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_462);
    
                    } else{
                        
    stringBuffer.append(TEXT_463);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_464);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_465);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_466);
    
                            }else{
                            
    stringBuffer.append(TEXT_467);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_468);
    
                            }
                            
    stringBuffer.append(TEXT_469);
    
                            if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_470);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_471);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_472);
    
                            }else{
                            
    stringBuffer.append(TEXT_473);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_474);
    
                            }
                            
    
                            if(typeToGenerate.equals("Boolean")) {
                            
    stringBuffer.append(TEXT_475);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_476);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_477);
    
                            } else if(typeToGenerate.equals("Byte")) {
                            
    stringBuffer.append(TEXT_478);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_479);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_480);
    
                            } else if(typeToGenerate.equals("byte[]")) {
                            
    stringBuffer.append(TEXT_481);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_482);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_483);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")) {
                            
    stringBuffer.append(TEXT_484);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_485);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_486);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_487);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_488);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_489);
    
                            } else if(typeToGenerate.equals("Double")) {
                            
    stringBuffer.append(TEXT_490);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_491);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_492);
    
                            } else if(typeToGenerate.equals("Float")) {
                            
    stringBuffer.append(TEXT_493);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_494);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_495);
    
                            } else if(typeToGenerate.equals("BigDecimal")) {
                            
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
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_502);
    stringBuffer.append(TEXT_503);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_504);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_505);
    stringBuffer.append(TEXT_506);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_507);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_508);
    stringBuffer.append(TEXT_509);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_510);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_511);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_512);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_513);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_514);
    
                            } else if(typeToGenerate.equals("Long")) {
                            
    stringBuffer.append(TEXT_515);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_516);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_517);
    
                            } else if(typeToGenerate.equals("Object")) {
                            
    stringBuffer.append(TEXT_518);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")) {
                            
    stringBuffer.append(TEXT_519);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_520);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_521);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_522);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_523);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_524);
    stringBuffer.append(TEXT_525);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_526);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_527);
    stringBuffer.append(TEXT_528);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_529);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_530);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_531);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Document")) {
                            
    stringBuffer.append(TEXT_532);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")) {
                            
    stringBuffer.append(TEXT_533);
    stringBuffer.append(column.getLabel());
    
                            }
                               
    stringBuffer.append(TEXT_534);
    
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
                        
    stringBuffer.append(TEXT_535);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_536);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_537);
    stringBuffer.append(typeToGenerate);
    stringBuffer.append(TEXT_538);
    
                    }else{
                        if(typeToGenerate.equals("byte[]")){
                        
    stringBuffer.append(TEXT_539);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_540);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_541);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_542);
    
                        }else{
                           
    stringBuffer.append(TEXT_543);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_544);
    
                        }
                        
    stringBuffer.append(TEXT_545);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_546);
    
                            if(typeToGenerate.equals("Boolean")){
                            
    stringBuffer.append(TEXT_547);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_548);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_549);
    
                            } else if(typeToGenerate.equals("Byte")){
                            
    stringBuffer.append(TEXT_550);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_551);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_552);
    
                            } else if(typeToGenerate.equals("byte[]")){
                            
    stringBuffer.append(TEXT_553);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_554);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_555);
    stringBuffer.append(TEXT_556);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_557);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_558);
    
                            } else if(typeToGenerate.equals("Char") || typeToGenerate.equals("Character")){
                            
    stringBuffer.append(TEXT_559);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_560);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_561);
    
                            } else if(typeToGenerate.equals("java.util.Date")) {
                            
    stringBuffer.append(TEXT_562);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_563);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_564);
    
                            } else if(typeToGenerate.equals("Double")){
                            
    stringBuffer.append(TEXT_565);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_566);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_567);
    
                            } else if(typeToGenerate.equals("Float")){
                            
    stringBuffer.append(TEXT_568);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_569);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_570);
    
                            } else if(typeToGenerate.equals("BigDecimal")){
                            
    stringBuffer.append(TEXT_571);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_572);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_573);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_574);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_575);
    stringBuffer.append(TEXT_576);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_577);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_578);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_579);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_580);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_581);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_582);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_583);
    
                            } else if(typeToGenerate.equals("Integer")){
                            
    stringBuffer.append(TEXT_584);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_585);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_586);
    
                            } else if(typeToGenerate.equals("Long")){
                            
    stringBuffer.append(TEXT_587);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_588);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_589);
    
                            } else if(typeToGenerate.equals("Object")){
                            
    stringBuffer.append(TEXT_590);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Short")){
                            
    stringBuffer.append(TEXT_591);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_592);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_593);
    
                            } else if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_594);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_595);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_596);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_597);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_598);
    stringBuffer.append(TEXT_599);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_600);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_601);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_602);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_603);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_604);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_605);
    
                            } else if(typeToGenerate.equals("List")) {
                            
    stringBuffer.append(TEXT_606);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Doucument")) {
                            
    stringBuffer.append(TEXT_607);
    stringBuffer.append(column.getLabel());
    
                            } else if(typeToGenerate.equals("Dynamic")){
                            
    stringBuffer.append(TEXT_608);
    stringBuffer.append(column.getLabel());
    
                            }
                            
    stringBuffer.append(TEXT_609);
    
                    }
                }
            }
        }

        public void overrideHashCode(){
        
    stringBuffer.append(TEXT_610);
    
            if (columns != null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                         String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                         if(javaType == JavaTypesManager.BOOLEAN) {
                        
    stringBuffer.append(TEXT_611);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_612);
    
                        } else {
                        
    stringBuffer.append(TEXT_613);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_614);
    
                        }
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                        
    stringBuffer.append(TEXT_615);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_616);
    
                    } else {
                    
    stringBuffer.append(TEXT_617);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_618);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_619);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_620);
    
        }

        public void overrideEquals(String objName){
        
    stringBuffer.append(TEXT_621);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_622);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_623);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_624);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_625);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_626);
    stringBuffer.append(objName);
    stringBuffer.append(TEXT_627);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    if (JavaTypesManager.isJavaPrimitiveType(column.getTalendType(), column.isNullable())) {
                    
    stringBuffer.append(TEXT_628);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_629);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_630);
    
                    } else if(javaType == JavaTypesManager.BYTE_ARRAY) {
                    
    stringBuffer.append(TEXT_631);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_632);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_633);
    
                    } else {
                    
    stringBuffer.append(TEXT_634);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_635);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_636);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_637);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_638);
    
                    }
                }
            }
            
    stringBuffer.append(TEXT_639);
    
        }

        public void overrideCompareTo(String otherName){
            //int returnValue = 0; ?
            
    stringBuffer.append(TEXT_640);
    
            if (columns !=null) {
                for (IMetadataColumn column: columns) {
                
    stringBuffer.append(TEXT_641);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_642);
    stringBuffer.append(otherName);
    stringBuffer.append(TEXT_643);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_644);
    
                }
            }
            
    stringBuffer.append(TEXT_645);
    
        }

        public void addMethods(){
        }

        public void declareVars(){
        }

        public void generateCode(){
        
    stringBuffer.append(TEXT_646);
    stringBuffer.append(structName );
    stringBuffer.append(TEXT_647);
    stringBuffer.append(implementsClasses);
    stringBuffer.append(TEXT_648);
    
                if (columns !=null) {
                for (IMetadataColumn column: columns) {
                    JavaType javaType = JavaTypesManager.getJavaTypeFromId(column.getTalendType());
                    
    stringBuffer.append(TEXT_649);
    stringBuffer.append( JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable()) );
    stringBuffer.append(TEXT_650);
    stringBuffer.append(column.getLabel() );
    
                        if(javaType == JavaTypesManager.CHARACTER && !column.isNullable()) {
                        
    stringBuffer.append(TEXT_651);
    
                        }
                        
    stringBuffer.append(TEXT_652);
    
                    }
                }
                
    stringBuffer.append(TEXT_653);
    declareVars();
    stringBuffer.append(TEXT_654);
    overrideHashCode();
    stringBuffer.append(TEXT_655);
    overrideEquals("obj");
    stringBuffer.append(TEXT_656);
    
                    if (columns !=null) {
                        for(int i=0; i< columns.size(); i++) {
                            IMetadataColumn column = columns.get(i);
                            String typeToGenerate = JavaTypesManager.getTypeToGenerate(column.getTalendType(), column.isNullable());
                            if(typeToGenerate.equals("String")) {
                            
    stringBuffer.append(TEXT_657);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_658);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_659);
    
                            }else{
                            
    stringBuffer.append(TEXT_660);
    stringBuffer.append(i==0?"":"," );
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_661);
    stringBuffer.append(column.getLabel() );
    stringBuffer.append(TEXT_662);
    
                            }
                        }
                    }
                    
    stringBuffer.append(TEXT_663);
    overrideWrite("out");
    stringBuffer.append(TEXT_664);
    overrideReadFields("in");
    stringBuffer.append(TEXT_665);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_666);
    overrideCompareTo("other");
    stringBuffer.append(TEXT_667);
    addMethods();
    stringBuffer.append(TEXT_668);
    
            if(genComparator){
                ComparatorHelper StructComparator = new ComparatorHelper();
                StructComparator.init(structName + "_Comparator", columns, structName);
                StructComparator.generateCode();

                
    stringBuffer.append(TEXT_669);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_670);
    stringBuffer.append(structName + "_Comparator");
    stringBuffer.append(TEXT_671);
    
            }
        }
    }
    
    stringBuffer.append(TEXT_672);
    stringBuffer.append(TEXT_673);
    
	//need to define MapperHelper before MapperGenerator, so use this trick
	class MapperHelperBase{
		public void map(){
		}

		public void prepare(){
		}

		public void configure(){
		}

		public void close(){
		}
	}

	class MapperGenerator{
		MapperHelperBase mapper;

		org.talend.core.model.process.AbstractNode node = null;
		String cid = null;
		String mapperClass = null;

		Object inKey = null;
		Object inKeyClass = null;

		Object inValue = null;
		Object inValueClass = null;

		Object outKey = null;
		Object outKeyClass = null;

		Object outValue = null;
		Object outValueClass = null;

		public MapperGenerator(MapperHelperBase mapper){
			this.mapper = mapper;
		}

		public void init(INode node, String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			this.node = (org.talend.core.model.process.AbstractNode)node;
			this.cid = cid;
			this.inKey = (inKey == null ? "key_"+cid : inKey);
			this.inValue = (inValue == null ? "value_"+cid : inValue);
			this.outKey = (outKey == null ? "outputKey_"+cid : outKey);
			this.outValue = (outValue == null ? "outputValue_"+cid : outValue);
			this.mapperClass = buildClassName(cid, "m");
			this.inKeyClass = buildClassName(inKey, "row");
			this.inValueClass = buildClassName(inValue, "row");
			this.outKeyClass = buildClassName(outKey, "row");
			this.outValueClass = buildClassName(outValue, "row");
		}

		private String buildClassName(String name, String type){
			if(type.equals("m")){
				return name + "Mapper";
			}else if(type.equals("r")){
				return name + "Reducer";
			}else if(type.equals("row")){
				return name + "Struct";
			}else{
				return null;
			}
		}

		private Object buildClassName(Object name, String type){
			if(type.equals("row")){
				if(name instanceof java.util.Map){
					java.util.Map<String, String> classes = new java.util.HashMap<String, String>();
					java.util.Map<String, String> names = (java.util.Map<String, String>)name;
					for(String key : names.keySet()){
						classes.put(key, buildClassName(names.get(key), "row"));
					}
					return classes;
				}else if(name instanceof String){
					return buildClassName(name.toString(), "row");
				}else if(name == null){
					return "NullWritable";
				}
			}
			return null;
		}

		public String getInKeyClass(String name){
			if(inKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKeyClass).get(name);
			}
			return getInKeyClass();
		}

		public String getInKeyClass(){
			if(inKeyClass instanceof String){
				return inKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInKey(String name){
			if(inKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKey).get(name);
			}
			return getInKey();
		}

		public String getInKey(){
			if(inKey instanceof String){
				return inKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutKeyClass(String name){
			if(outKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKeyClass).get(name);
			}
			return getOutKeyClass();
		}

		public String getOutKeyClass(){
			if(outKeyClass instanceof String){
				return outKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutKey(String name){
			if(outKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKey).get(name);
			}
			return getOutKey();
		}

		public String getOutKey(){
			if(outKey instanceof String){
				return outKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInValueClass(String name){
			if(inValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValueClass).get(name);
			}
			return getInValueClass();
		}

		public String getInValueClass(){
			if(inValueClass instanceof String){
				return inValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getInValue(String name){
			if(inValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValue).get(name);
			}
			return getInValue();
		}

		public String getInValue(){
			if(inValue instanceof String){
				return inValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutValueClass(String name){
			if(outValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValueClass).get(name);
			}
			return getOutValueClass();
		}

		public String getOutValueClass(){
			if(outValueClass instanceof String){
				return outValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public String getOutValue(String name){
			if(outValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValue).get(name);
			}
			return getOutValue();
		}

		public String getOutValue(){
			if(outValue instanceof String){
				return outValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}

		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			
    stringBuffer.append(TEXT_674);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_675);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_676);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_677);
    
		}

		public String getCodeEmit(String outKey, String outValue) {
			return "output_" + cid + ".collect("
					+ (outKey == null ? "outputKey_"+cid : outKey) + ","
					+ outValue + ");";
		}

		public void generate(){
		
    stringBuffer.append(TEXT_678);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_679);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_680);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_681);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_682);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_683);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_684);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_685);
    if(!outKey.equals(outValue)){//for tFindQuantiles, if outKey same as outValue, assume the write want to reuse same object
    stringBuffer.append(TEXT_686);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_687);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_688);
    }
    stringBuffer.append(TEXT_689);
    mapper.prepare();
    stringBuffer.append(TEXT_690);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_691);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_692);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_693);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_694);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_695);
    }else{
    stringBuffer.append(TEXT_696);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_697);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_698);
    }
    stringBuffer.append(TEXT_699);
    if(!outKey.equals(outValue)){
    stringBuffer.append(TEXT_700);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_701);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_702);
    }
    stringBuffer.append(TEXT_703);
    mapper.configure();
    stringBuffer.append(TEXT_704);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_705);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_706);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_707);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_708);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_709);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_710);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_711);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_712);
    mapper.map();
    stringBuffer.append(TEXT_713);
    mapper.close();
    stringBuffer.append(TEXT_714);
    
		}
		public void generateConf(){
			if(node.isMapOnlyAfterReduce()){
			
    stringBuffer.append(TEXT_715);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_716);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_717);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_718);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_719);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_720);
    
			}else{
			
    stringBuffer.append(TEXT_721);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_722);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_723);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_724);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_725);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_726);
    
			}
		}

		public void generateConf(org.talend.core.model.process.IConnection inConn){
			String startNodeCid = inConn.getSource().getSubProcessStartNode(false).getUniqueName();
			
    stringBuffer.append(TEXT_727);
    stringBuffer.append(startNodeCid);
    stringBuffer.append(TEXT_728);
    
			generateConf();
		}
	}

	class MOMapperGenerator extends MapperGenerator{

		/** The single connection to pass along the output chain of Mappers
		 *  instead of sending to a dedicated collector via MultipleOutputs. */
		String connectionToChain = null;

		public MOMapperGenerator(MapperHelperBase mapper){
			super(mapper);
		}

		public void sendOutConnectionToChain(String name) {
			connectionToChain = getOutValue(name);
		}

		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			if (outValue != null && outValue.equals(connectionToChain))
				super.output(outKey, outValue);
			else {
				
    stringBuffer.append(TEXT_729);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_730);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_731);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_732);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_733);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_734);
    
			}
		}

		public String getCodeEmit(String outKey, String outValue) {
			if (outValue != null && outValue.equals(connectionToChain))
				return super.getCodeEmit(outKey, outValue);
			else
				return "mos_" + cid + ".getCollector(\"" + outValue
						+ "\", reporter_"+ cid + ")"+ ".collect("
						+ (outKey == null ? "outputKey_"+cid : outKey) + ","
						+ outValue + ");";
		}

		public void generate(){
		
    stringBuffer.append(TEXT_735);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_736);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_737);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_738);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_739);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_740);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_741);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_742);
    
				if(outValueClass instanceof java.util.Map){
					for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
					
    stringBuffer.append(TEXT_743);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_744);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_745);
    
					}
				}
				
    stringBuffer.append(TEXT_746);
    mapper.prepare();
    stringBuffer.append(TEXT_747);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_748);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_749);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_750);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_751);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_752);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_753);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_754);
    }else{
    stringBuffer.append(TEXT_755);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_756);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_757);
    }
    stringBuffer.append(TEXT_758);
    
					if(outValueClass instanceof java.util.Map){
						for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
						
    stringBuffer.append(TEXT_759);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_760);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_761);
    
						}
					}
					
    stringBuffer.append(TEXT_762);
    mapper.configure();
    stringBuffer.append(TEXT_763);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_764);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_765);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_766);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_767);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_768);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_769);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_770);
    mapper.map();
    stringBuffer.append(TEXT_771);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_772);
    mapper.close();
    stringBuffer.append(TEXT_773);
    
		}
		public void generateConf(){
			if(node.isMapOnlyAfterReduce()){
			
    stringBuffer.append(TEXT_774);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_775);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_776);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_777);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_778);
    
			}else{
			
    stringBuffer.append(TEXT_779);
    stringBuffer.append(mapperClass);
    stringBuffer.append(TEXT_780);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_781);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_782);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_783);
    
			}
			
    stringBuffer.append(TEXT_784);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_785);
    
        	java.util.Map<String, String> values = (java.util.Map<String, String>)outValue;
        	for(String key : values.keySet()){
        	
    stringBuffer.append(TEXT_786);
    stringBuffer.append(values.get(key));
    stringBuffer.append(TEXT_787);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_788);
    stringBuffer.append(getOutValueClass(key));
    stringBuffer.append(TEXT_789);
    
        	}
		}
	}

	final String M_TYPE_BASE = "base";
	final String M_TYPE_MO = "mo";

	class MapperHelper extends MapperHelperBase{

		MapperGenerator mapperGen;

		String cid = null;

		public void setType(String type){
			if(type.equals(M_TYPE_BASE)){
				mapperGen = new MapperGenerator(this);
			}else if(type.equals(M_TYPE_MO)){
				mapperGen = new MOMapperGenerator(this);
			}
		}

		public void init(INode node, String cid, String inKey, String inValue, String outKey, Object outValue){
			if(mapperGen == null){
				mapperGen = new MapperGenerator(this);
			}
			mapperGen.init(node, cid, inKey, inValue, outKey, outValue);
			this.cid = mapperGen.cid;
		}

		public String getInKeyClass(String name){
			return mapperGen.getInKeyClass(name);
		}

		public String getInKeyClass(){
			return mapperGen.getInKeyClass();
		}

		public String getInKey(String name){
			return mapperGen.getInKey(name);
		}

		public String getInKey(){
			return mapperGen.getInKey();
		}

		public String getOutKeyClass(String name){
			return mapperGen.getOutKeyClass(name);
		}

		public String getOutKeyClass(){
			return mapperGen.getOutKeyClass();
		}

		public String getOutKey(String name){
			return mapperGen.getOutKey(name);
		}

		public String getOutKey(){
			return mapperGen.getOutKey();
		}

		public String getInValueClass(String name){
			return mapperGen.getInValueClass(name);
		}

		public String getInValueClass(){
			return mapperGen.getInValueClass();
		}

		public String getInValue(String name){
			return mapperGen.getInValue(name);
		}

		public String getInValue(){
			return mapperGen.getInValue();
		}

		public String getOutValueClass(String name){
			return mapperGen.getOutValueClass(name);
		}

		public String getOutValueClass(){
			return mapperGen.getOutValueClass();
		}

		public String getOutValue(String name){
			return mapperGen.getOutValue(name);
		}

		public String getOutValue(){
			return mapperGen.getOutValue();
		}

		/**
		 * In the case where the underlying implementation supports multiple
		 * outputs, this causes the named output to be passed along the chain
		 * of mapper tasks instead of using the MultipleOutputs object.
		 */
		public void sendOutConnectionToChain(String name) {
			if (mapperGen instanceof MOMapperGenerator)
				((MOMapperGenerator)mapperGen).sendOutConnectionToChain(name);
		}

		public void output(String outKey, String outValue){
			mapperGen.output(outKey, outValue);
		}

		public String getCodeEmit(String outKey, String outValue){
			return mapperGen.getCodeEmit(outKey, outValue);
		}

		public void generate(){
			mapperGen.generate();
		}

		public void generateConf(){
			mapperGen.generateConf();
		}

		public void generateConf(org.talend.core.model.process.IConnection inConn){
			mapperGen.generateConf(inConn);
		}

		public void map(){
		}

		public void prepare(){
		}

		public void configure(){
		}

		public void close(){
		}
	}
	
    stringBuffer.append(TEXT_790);
    stringBuffer.append(TEXT_791);
    
	//need to define MapperHelper before MapperGenerator, so use this trick
	class ReducerHelperBase{
		public void reduce(){
		}
		
		public void prepare(){
		}
		
		public void configure(){
		}
		
		public void close(){
		}
	}
	
	class ReducerGenerator{
		ReducerHelperBase reducer;
		
		String cid = null;
		String reducerClass = null;

		Object inKey = null;
		Object inKeyClass = null;

		Object inValue = null;
		Object inValueClass = null;

		Object outKey = null;
		Object outKeyClass = null;

		Object outValue = null;
		Object outValueClass = null;
		
		public ReducerGenerator(ReducerHelperBase reducer){
			this.reducer = reducer; 
		}
		
		public void init(String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			this.cid = cid;
			this.inKey = (inKey == null ? "key_"+cid : inKey);
			this.inValue = (inValue == null ? "values_"+cid : inValue);
			this.outKey = (outKey == null ? "outputKey_"+cid : outKey);
			this.outValue = (outValue == null ? "outputValue_"+cid : outValue);
			this.reducerClass = buildClassName(cid, "r");
			this.inKeyClass = buildClassName(inKey, "row");
			this.inValueClass = buildClassName(inValue, "row");
			this.outKeyClass = buildClassName(outKey, "row");
			this.outValueClass = buildClassName(outValue, "row");
		}
		
		protected String buildClassName(String name, String type){
			if(type.equals("m")){
				return name + "Mapper";
			}else if(type.equals("r")){
				return name + "Reducer";
			}else if(type.equals("c")){
				return name + "Combiner";
			}else if(type.equals("row")){
				return name + "Struct";
			}else{
				return null;
			}
		}
		
		protected Object buildClassName(Object name, String type){
			if(type.equals("row")){
				if(name instanceof java.util.Map){
					java.util.Map<String, String> classes = new java.util.HashMap<String, String>();
					java.util.Map<String, String> names = (java.util.Map<String, String>)name;
					for(String key : names.keySet()){
						classes.put(key, buildClassName(names.get(key), "row"));
					}
					return classes;
				}else if(name instanceof String){
					return buildClassName(name.toString(), "row");
				}else if(name == null){
					return "NullWritable";
				}
			}
			return null;
		}
		
		public String getInKeyClass(String name){
			if(inKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKeyClass).get(name);
			}
			return getInKeyClass();
		}
		
		public String getInKeyClass(){
			if(inKeyClass instanceof String){
				return inKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getInKey(String name){
			if(inKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)inKey).get(name);
			}
			return getInKey();
		}
		
		public String getInKey(){
			if(inKey instanceof String){
				return inKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getOutKeyClass(String name){
			if(outKeyClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKeyClass).get(name);
			}
			return getOutKeyClass();
		}
		
		public String getOutKeyClass(){
			if(outKeyClass instanceof String){
				return outKeyClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getOutKey(String name){
			if(outKey instanceof java.util.Map){
				return ((java.util.Map<String, String>)outKey).get(name);
			}
			return getOutKey();
		}
		
		public String getOutKey(){
			if(outKey instanceof String){
				return outKey.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getInValueClass(String name){
			if(inValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValueClass).get(name);
			}
			return getInValueClass();
		}
		
		public String getInValueClass(){
			if(inValueClass instanceof String){
				return inValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getInValue(String name){
			if(inValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)inValue).get(name);
			}
			return getInValue();
		}
		
		public String getInValue(){
			if(inValue instanceof String){
				return inValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getOutValueClass(String name){
			if(outValueClass instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValueClass).get(name);
			}
			return getOutValueClass();
		}
		
		public String getOutValueClass(){
			if(outValueClass instanceof String){
				return outValueClass.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public String getOutValue(String name){
			if(outValue instanceof java.util.Map){
				return ((java.util.Map<String, String>)outValue).get(name);
			}
			return getOutValue();
		}
		
		public String getOutValue(){
			if(outValue instanceof String){
				return outValue.toString();
			}else{
				System.err.println("not single, wrong call");
				return null;
			}
		}
		
		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			
    stringBuffer.append(TEXT_792);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_793);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_794);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_795);
    
		}
		public void generate(){
		
    stringBuffer.append(TEXT_796);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_797);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_798);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_799);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_800);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_801);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_802);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_803);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_804);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_805);
    reducer.prepare();
    stringBuffer.append(TEXT_806);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_807);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_808);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_809);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_810);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_811);
    }else{
    stringBuffer.append(TEXT_812);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_813);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_814);
    }
    stringBuffer.append(TEXT_815);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_816);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_817);
    reducer.configure();
    stringBuffer.append(TEXT_818);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_819);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_820);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_821);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_822);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_823);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_824);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_825);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_826);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_827);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_828);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_829);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_830);
    reducer.reduce();
    stringBuffer.append(TEXT_831);
    reducer.close();
    stringBuffer.append(TEXT_832);
    
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_833);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_834);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_835);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_836);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_837);
    stringBuffer.append(outValueClass);
    stringBuffer.append(TEXT_838);
    
		}
	} 
	
	class MOReducerGenerator extends ReducerGenerator{
		public MOReducerGenerator(ReducerHelperBase reducer){
			super(reducer); 
		}
		
		public void output(String outKey, String outValue){
			outKey = (outKey == null ? "outputKey_"+cid : outKey);
			
    stringBuffer.append(TEXT_839);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_840);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_841);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_842);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_843);
    stringBuffer.append(outValue);
    stringBuffer.append(TEXT_844);
    
		}
		public void generate(){
		
    stringBuffer.append(TEXT_845);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_846);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_847);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_848);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_849);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_850);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_851);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_852);
    
				if(outValueClass instanceof java.util.Map){
					for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
					
    stringBuffer.append(TEXT_853);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_854);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_855);
    	
					}
				}
				
    stringBuffer.append(TEXT_856);
    reducer.prepare();
    stringBuffer.append(TEXT_857);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_858);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_859);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_860);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_861);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_862);
    if("NullWritable".equals(outKeyClass)){
    stringBuffer.append(TEXT_863);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_864);
    }else{
    stringBuffer.append(TEXT_865);
    stringBuffer.append(outKey);
    stringBuffer.append(TEXT_866);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_867);
    }
    stringBuffer.append(TEXT_868);
    
					if(outValueClass instanceof java.util.Map){
						for(String key : ((java.util.Map<String, String>)outValueClass).keySet()){
						
    stringBuffer.append(TEXT_869);
    stringBuffer.append(((java.util.Map)outValue).get(key));
    stringBuffer.append(TEXT_870);
    stringBuffer.append(((java.util.Map)outValueClass).get(key));
    stringBuffer.append(TEXT_871);
    	
						}
					}
					
    stringBuffer.append(TEXT_872);
    reducer.configure();
    stringBuffer.append(TEXT_873);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_874);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_875);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_876);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_877);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_878);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_879);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_880);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_881);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_882);
    reducer.reduce();
    stringBuffer.append(TEXT_883);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_884);
    reducer.close();
    stringBuffer.append(TEXT_885);
    
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_886);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_887);
    stringBuffer.append(inKeyClass);
    stringBuffer.append(TEXT_888);
    stringBuffer.append(inValueClass);
    stringBuffer.append(TEXT_889);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_890);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_891);
    
        	java.util.Map<String, String> values = (java.util.Map<String, String>)outValue;
        	for(String key : values.keySet()){
        	
    stringBuffer.append(TEXT_892);
    stringBuffer.append(values.get(key));
    stringBuffer.append(TEXT_893);
    stringBuffer.append(outKeyClass);
    stringBuffer.append(TEXT_894);
    stringBuffer.append(getOutValueClass(key));
    stringBuffer.append(TEXT_895);
    
        	}
		}
	}
	
	class CombinerGenerator extends ReducerGenerator{
		public CombinerGenerator(ReducerHelperBase reducer){
			super(reducer); 
		}
		public void init(String cid, Object inKey, Object inValue, Object outKey, Object outValue){
			super.init(cid, inKey, inValue, outKey, outValue);
			this.reducerClass = buildClassName(cid, "c");
		}
		public void generateConf(){
		
    stringBuffer.append(TEXT_896);
    stringBuffer.append(reducerClass);
    stringBuffer.append(TEXT_897);
    			
		}
	}
	
	final String R_TYPE_BASE = "base";
	final String R_TYPE_MO = "mo";
	final String R_TYPE_COMBINER = "combiner";
	
	class ReducerHelper extends ReducerHelperBase{
		
		ReducerGenerator reducerGen;
		
		String cid = null;
		
		public void setType(String type){
			if(type.equals(R_TYPE_BASE)){
				reducerGen = new ReducerGenerator(this);
			}else if(type.equals(R_TYPE_MO)){
				reducerGen = new MOReducerGenerator(this);
			}else if(type.equals(R_TYPE_COMBINER)){
				reducerGen = new CombinerGenerator(this);
			}
		}
		
		public void init(String cid, String inKey, String inValue, String outKey, Object outValue){
			if(reducerGen == null){
				reducerGen = new ReducerGenerator(this);
			}
			reducerGen.init(cid, inKey, inValue, outKey, outValue);
			this.cid = reducerGen.cid;
		}
		
		public String getInKeyClass(String name){
			return reducerGen.getInKeyClass(name);
		}
		
		public String getInKeyClass(){
			return reducerGen.getInKeyClass();
		}
		
		public String getInKey(String name){
			return reducerGen.getInKey(name);
		}
		
		public String getInKey(){
			return reducerGen.getInKey();
		}
		
		public String getOutKeyClass(String name){
			return reducerGen.getOutKeyClass(name);
		}
		
		public String getOutKeyClass(){
			return reducerGen.getOutKeyClass();
		}
		
		public String getOutKey(String name){
			return reducerGen.getOutKey(name);
		}
		
		public String getOutKey(){
			return reducerGen.getOutKey();
		}
		
		public String getInValueClass(String name){
			return reducerGen.getInValueClass(name);
		}
		
		public String getInValueClass(){
			return reducerGen.getInValueClass();
		}
		
		public String getInValue(String name){
			return reducerGen.getInValue(name);
		}
		
		public String getInValue(){
			return reducerGen.getInValue();
		}
		
		public String getOutValueClass(String name){
			return reducerGen.getOutValueClass(name);
		}
		
		public String getOutValueClass(){
			return reducerGen.getOutValueClass();
		}
		
		public String getOutValue(String name){
			return reducerGen.getOutValue(name);
		}
		
		public String getOutValue(){
			return reducerGen.getOutValue();
		}
		
		public void output(String outKey, String outValue){
			reducerGen.output(outKey, outValue);
		}
		
		public void generate(){
			reducerGen.generate();
		}
		
		public void generateConf(){
			reducerGen.generateConf();
		}
		
		public void reduce(){
		}
		
		public void prepare(){
		}
		
		public void configure(){
		}
		
		public void close(){
		}
	}
	
    stringBuffer.append(TEXT_898);
    stringBuffer.append(TEXT_899);
    

/**
 * Contains common processing for MapReduce tMap code generation.
 */
class TMapMrUtil extends TMapUtilLegacy {

    /** PARAMETER configuration for one of the *precedent* input. */
    public static final String PARAM_FILENAME = "FILENAME"; //$NON-NLS-1$

    /** Either this or emitToReducer will be provided for code generation. */
    private MapperHelper emitToMapper;

    /** Either this or emitToMapper will be provided for code generation. */
    private ReducerHelper emitToReducer;

    public TMapMrUtil(MapperComponent node){
        super(node);
    }

    /**
     * Outgoing connection names. The key and values are always identical: this is a Map for legacy reasons with
     * mr_mapper_helper and multiple outputs.
     */
    public Map<String, String> getOutConnNamesAsMap() {
        HashMap<String, String> map = new HashMap<String, String>();
        for (String name : getOutConnNames()) {
            map.put(name, name);
        }
        return map;
    }

    /**
     * Generates code to initialize the columns in the named key structure to
     * the values from the main table row.
     */
    public void generateInitializeJoinKeyStructFromMain(String keyStructName) {
        initJoinKey();
        for (IMetadataColumn keyColumn : getJoinKeyColumns()) {
            String keyColumnName = keyColumn.getLabel();
            
    stringBuffer.append(TEXT_900);
    stringBuffer.append(keyStructName);
    stringBuffer.append(TEXT_901);
    stringBuffer.append(keyColumnName);
    stringBuffer.append(TEXT_902);
    stringBuffer.append(getJoinKeyExpression(keyColumnName));
    stringBuffer.append(TEXT_903);
    
        }
    }

    /**
     * Generates code to initialize the columns in the named key structure to
     * the values from the given table.
     */
    public void generateInitializeJoinKeyStructFromLookup(String keyStructName,
           String tableName) {
        initJoinKey();
        for (IMetadataColumn keyColumn : getJoinKeyColumns()) {
            String keyColumnName = keyColumn.getLabel();
            
    stringBuffer.append(TEXT_904);
    stringBuffer.append(keyStructName);
    stringBuffer.append(TEXT_905);
    stringBuffer.append(keyColumnName);
    stringBuffer.append(TEXT_906);
    stringBuffer.append(tableName);
    stringBuffer.append(TEXT_907);
    stringBuffer.append(getJoinKeyForTable(tableName).get(keyColumnName));
    stringBuffer.append(TEXT_908);
    
        }
    }

    /** Redirects to either the emitToMapper or emitToReducer depending on which
     *  is present. */
    public String getOutValue(String name) {
        // TODO: It would be great if these had a common interface to avoid
        // redirection to either.
        return emitToReducer != null ? emitToReducer.getOutValue(name) : emitToMapper.getOutValue(name);
    }

    /** Redirects to either the emitToMapper or emitToReducer depending on which
     *  is present. */
    public void output(String outKey, String outValue) {
        // TODO: It would be great if these had a common interface to avoid
        // redirection to either.
        if (emitToReducer != null) {
            emitToReducer.output(outKey, outValue);
        } else {
            emitToMapper.output(outKey, outValue);
        }
    }

    public String getLookupSource(IConnection inConn) {
        // TODO(rskraba): this should always work because we guarantee that
        // the input is either a tHDFSInput or tMRInput during code
        // generation. To verify.
        return "new java.net.URI(\"hdfs\", null, null, -1, " //$NON-NLS-1$
                + getLookupConns().get(0).getSource().getElementParameter(PARAM_FILENAME).getValue().toString() + ", null, " //$NON-NLS-1$
                + getLookupSourceLocal(inConn) + ')';
    }

    public String getLookupSourceLocal(IConnection inConn) {
        return "\"lookupLocal_" + inConn.getName() + '"'; //$NON-NLS-1$
    }


    public void generateJoinWithLookupRows(MapperHelper mh, ReducerHelper rh) {
        // TODO: It would be great if these had a common interface to avoid
        // redirection to either.
        emitToMapper = mh;
        emitToReducer = rh;

        
    stringBuffer.append(TEXT_909);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_910);
    
                    for(IConnection lookupConn : getLookupConns()){
                        String lookupConnName = lookupConn.getName();
                        String lookupTableName = lookupConnName;
                        ExternalMapperTable lookupTable = getLookupTable(lookupConnName);
                        
    stringBuffer.append(TEXT_911);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_912);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_913);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_914);
    
                            if(lookupTable.isInnerJoin()){
                            
    stringBuffer.append(TEXT_915);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_916);
    
                            }
                            
    stringBuffer.append(TEXT_917);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_918);
    stringBuffer.append(TEXT_919);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_920);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_921);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_922);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_923);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_924);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_925);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_926);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_927);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_928);
    stringBuffer.append(TEXT_929);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_930);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_931);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_932);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_933);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_934);
    stringBuffer.append(TEXT_935);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_936);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_937);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_938);
    
                                if(!lookupTable.isSelfFilter() || (getOutputTablesRejectInner().size() != 0 && lookupTable.isInnerJoin())){
                                    String lookupTableExpression = lookupTable.isActivateExpressionFilter() && lookupTable.getExpressionFilter()!=null && !"".equals(lookupTable.getExpressionFilter().trim()) ? lookupTable.getExpressionFilter().trim() : null;
                                    if(lookupTableExpression != null){
                                    
    stringBuffer.append(TEXT_939);
    stringBuffer.append(lookupTableExpression);
    stringBuffer.append(TEXT_940);
    stringBuffer.append(TEXT_941);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_942);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_943);
    
                                            if(lookupTable.isInnerJoin()){
                                            
    stringBuffer.append(TEXT_944);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_945);
    
                                            }
                                            
    stringBuffer.append(TEXT_946);
    
                                    }
                                }
                                
    stringBuffer.append(TEXT_947);
    
                    }
                    
    
                            generateInitializeVarsTables();

                            if (getOutputTables().size() > 0 || needsRejectOutput()) {
                            
    stringBuffer.append(TEXT_948);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_949);
    
                                    if(needsRejectOutput()){
                                    
    stringBuffer.append(TEXT_950);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_951);
    
                                    }
                                    if(getOutputTables().size() > 0){
                                        for(ExternalMapperTable outputTable : getOutputTables()){
                                            String connName = outputTable.getIsJoinTableOf() == null ? getTableName(outputTable.getName()) : getTableName(outputTable.getIsJoinTableOf());
                                            List<ExternalMapperTableEntry> entries = outputTable.getMetadataTableEntries();
                                            String outputTableExpression = outputTable.isActivateExpressionFilter() && outputTable.getExpressionFilter()!=null && !"".equals(outputTable.getExpressionFilter().trim()) ? outputTable.getExpressionFilter().trim() : null;
                                            if(outputTableExpression != null){
                                            
    stringBuffer.append(TEXT_952);
    stringBuffer.append(outputTableExpression);
    stringBuffer.append(TEXT_953);
    
                                            }
                                                    if(needsRejectOutput()){
                                                    
    stringBuffer.append(TEXT_954);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_955);
    
                                                    }
                                            if (entries != null && isOutputConnected(connName)) {
                                                for (ExternalMapperTableEntry entry : entries) {
							                        String expression = entry.getExpression();
							                        if (expression != null && expression.trim().length() != 0) {
                                                    
    stringBuffer.append(TEXT_956);
    stringBuffer.append(getOutValue(connName));
    stringBuffer.append(TEXT_957);
    stringBuffer.append(entry.getName());
    stringBuffer.append(TEXT_958);
    stringBuffer.append(expression);
    stringBuffer.append(TEXT_959);
    
                                                    }
                                                }
                                                output(null, getOutValue(connName));
                                            }
                                            if(outputTableExpression != null){
                                            
    stringBuffer.append(TEXT_960);
    
                                            }
                                        }
                                    }
                                    if(needsRejectOutput()){
                                        for(ExternalMapperTable rejectOutputTable : getOutputTablesReject()){
                                            String connName = rejectOutputTable.getIsJoinTableOf() == null ? getTableName(rejectOutputTable.getName()) : getTableName(rejectOutputTable.getIsJoinTableOf());
                                            List<ExternalMapperTableEntry> entries = rejectOutputTable.getMetadataTableEntries();
                                            String rejectOutputTableExpression = rejectOutputTable.isActivateExpressionFilter() && rejectOutputTable.getExpressionFilter()!=null && !"".equals(rejectOutputTable.getExpressionFilter().trim()) ? rejectOutputTable.getExpressionFilter().trim() : null;
                                            
    stringBuffer.append(TEXT_961);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_962);
    stringBuffer.append(rejectOutputTableExpression != null ? " && " + rejectOutputTableExpression : "");
    stringBuffer.append(TEXT_963);
    
                                                if (entries != null && isOutputConnected(connName)) {
                                                    for (ExternalMapperTableEntry entry : entries) {
								                        String expression = entry.getExpression();
								                        if (expression != null && expression.trim().length() != 0) {
                                                        
    stringBuffer.append(TEXT_964);
    stringBuffer.append(getOutValue(connName));
    stringBuffer.append(TEXT_965);
    stringBuffer.append(entry.getName());
    stringBuffer.append(TEXT_966);
    stringBuffer.append(expression);
    stringBuffer.append(TEXT_967);
    
                                                        }
                                                    }
                                                    output(null, getOutValue(connName));
                                                }
                                                
    stringBuffer.append(TEXT_968);
    
                                        }
                                    }
                                    
    stringBuffer.append(TEXT_969);
    
                            }
                            if(getOutputTablesRejectInner().size() > 0){
                            
    stringBuffer.append(TEXT_970);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_971);
    
                                    for(ExternalMapperTable rejectInnerTable : getOutputTablesRejectInner()){
                                        String connName = rejectInnerTable.getIsJoinTableOf() == null ? getTableName(rejectInnerTable.getName()) : getTableName(rejectInnerTable.getIsJoinTableOf());
                                        List<ExternalMapperTableEntry> entries = rejectInnerTable.getMetadataTableEntries();
                                        String rejectInnerTableExpression = rejectInnerTable.isActivateExpressionFilter() && rejectInnerTable.getExpressionFilter()!=null && !"".equals(rejectInnerTable.getExpressionFilter().trim()) ? rejectInnerTable.getExpressionFilter().trim() : null;
                                        if(rejectInnerTableExpression != null){
                                        
    stringBuffer.append(TEXT_972);
    stringBuffer.append(rejectInnerTableExpression);
    stringBuffer.append(TEXT_973);
    
                                        }
                                        if (entries != null && isOutputConnected(connName)) {
                                            for (ExternalMapperTableEntry entry : entries) {
						                        String expression = entry.getExpression();
						                        if (expression != null && expression.trim().length() != 0) {
                                                
    stringBuffer.append(TEXT_974);
    stringBuffer.append(getOutValue(connName));
    stringBuffer.append(TEXT_975);
    stringBuffer.append(entry.getName());
    stringBuffer.append(TEXT_976);
    stringBuffer.append(expression);
    stringBuffer.append(TEXT_977);
    
                                                }
                                            }
                                            output(null, getOutValue(connName));
                                        }
                                        if(rejectInnerTableExpression != null){
                                        
    stringBuffer.append(TEXT_978);
    
                                        }
                                    }
                                    
    stringBuffer.append(TEXT_979);
    
                            }
                            
    stringBuffer.append(TEXT_980);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_981);
    
                    for(IConnection lookupConn : getLookupConns()){
                    
    stringBuffer.append(TEXT_982);
    
                    }
    }

    /**
     * When called, generates code for the Vars tables in the mapper.  This code
     * should exist in the top level scope of a Mapper or Reducer, and only takes
     * care of initializing the structure used to contain the Vars.
     *
     * The member in the class is the name of the Vars table.
     * <pre>
     * class Var__tMap_5__Struct {
     *     String blank;
     * }
     * Var__tMap_5__Struct Var = new Var__tMap_5__Struct();
     * </pre>
     */
    public void generatePrepareVarsTables() {
        for(ExternalMapperTable table : getVarsTables()){
            List<ExternalMapperTableEntry> tableEntries = table.getMetadataTableEntries();
            if (tableEntries == null) {
                continue;
            }
            String tableName = table.getName();
            String instanceVarName = tableName + "__" + cid;
            String className = instanceVarName + "__Struct";
            int lstSize = tableEntries.size();
            if(lstSize > 0){
            
    stringBuffer.append(TEXT_983);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_984);
    
                    for (int i = 0; i < lstSize; i++) {
                        ExternalMapperTableEntry varTableEntry = (ExternalMapperTableEntry) tableEntries.get(i);
                        String javaType = varTableEntry.getType();
                        
    stringBuffer.append(TEXT_985);
    stringBuffer.append(JavaTypesManager.getTypeToGenerate(javaType, varTableEntry.isNullable()));
    stringBuffer.append(TEXT_986);
    stringBuffer.append(varTableEntry.getName());
    stringBuffer.append(TEXT_987);
    
                    }
                    
    stringBuffer.append(TEXT_988);
    stringBuffer.append(TEXT_989);
    stringBuffer.append(className + " " + tableName + " = new " + className + "();" );
    
            }
        }
    }

    /**
     * When called, initializes the member created in generatePrepareVarsTables()
     * to their expression values.  That member must be accessible from the scope
     * where this method is called.
     * <pre>
     * Var.blank = "blank";
     * </pre>
     */
    public void generateInitializeVarsTables() {
        for(ExternalMapperTable varsTable : getVarsTables()){
            List<ExternalMapperTableEntry> varsTableEntries = varsTable.getMetadataTableEntries();
            if (varsTableEntries == null) {
                continue;
            }
            String varsTableName = varsTable.getName();
            String instanceVarName = varsTableName + "__" + cid;
            String className = instanceVarName + "__Struct";
            for(ExternalMapperTableEntry varsTableEntry : varsTableEntries){
                String varsColumnName = varsTableEntry.getName();
                String varExpression = varsTableEntry.getExpression();
                if (varExpression == null || varExpression.trim().length() == 0) {
                    varExpression = JavaTypesManager.getDefaultValueFromJavaIdType(varsTableEntry.getType(),
                            varsTableEntry.isNullable());
                }
                
    stringBuffer.append(TEXT_990);
    stringBuffer.append(varsTableName);
    stringBuffer.append(TEXT_991);
    stringBuffer.append(varsColumnName);
    stringBuffer.append(TEXT_992);
    stringBuffer.append(varExpression);
    stringBuffer.append(TEXT_993);
    
            }
        }
    }
}


    stringBuffer.append(TEXT_994);
    

// This variable can be reused by all of the tMap javajet templates.
final TMapMrUtil tMapUtil = new TMapMrUtil(node);


    if(!tMapUtil.hasLookup()){
        //main only
        class Mapper extends MapperHelper{

            public void prepare(){
                // constants
                for(ExternalMapperTable table : tMapUtil.getVarsTables()){
                    List<ExternalMapperTableEntry> tableEntries = table.getMetadataTableEntries();
                    if (tableEntries == null) {
                        continue;
                    }
                    String tableName = table.getName();
                    String instanceVarName = tableName + "__" + cid;
                    String className = instanceVarName + "__Struct";
                    int lstSize = tableEntries.size();
                    if(lstSize > 0){
                    
    stringBuffer.append(TEXT_995);
    stringBuffer.append(className);
    stringBuffer.append(TEXT_996);
    
                            for (int i = 0; i < lstSize; i++) {
                                ExternalMapperTableEntry varTableEntry = (ExternalMapperTableEntry) tableEntries.get(i);
                                String javaType = varTableEntry.getType();
                                
    stringBuffer.append(TEXT_997);
    stringBuffer.append(JavaTypesManager.getTypeToGenerate(javaType, varTableEntry.isNullable()));
    stringBuffer.append(TEXT_998);
    stringBuffer.append(varTableEntry.getName());
    stringBuffer.append(TEXT_999);
    
                            }
                            
    stringBuffer.append(TEXT_1000);
    stringBuffer.append(TEXT_1001);
    stringBuffer.append( className + " " + tableName + " = new " + className + "();" );
    
                    }
                }
            }
            public void map(){
            
    stringBuffer.append(TEXT_1002);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1003);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1004);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1005);
    
                if(tMapUtil.getMainTableExpression() != null){
                
    stringBuffer.append(TEXT_1006);
    stringBuffer.append(tMapUtil.getMainTableExpression());
    stringBuffer.append(TEXT_1007);
    
                }
                for(ExternalMapperTable varsTable : tMapUtil.getVarsTables()){
                    List<ExternalMapperTableEntry> varsTableEntries = varsTable.getMetadataTableEntries();
                    if (varsTableEntries == null) {
                        continue;
                    }
                    String varsTableName = varsTable.getName();
                    String instanceVarName = varsTableName + "__" + cid;
                    String className = instanceVarName + "__Struct";
                    for(ExternalMapperTableEntry varsTableEntry : varsTableEntries){
                        String varsColumnName = varsTableEntry.getName();
                        String varExpression = varsTableEntry.getExpression();
                        if (varExpression == null || varExpression.trim().length() == 0) {
                            varExpression = JavaTypesManager.getDefaultValueFromJavaIdType(varsTableEntry.getType(),
                                    varsTableEntry.isNullable());
                        }
                        
    stringBuffer.append(TEXT_1008);
    stringBuffer.append(varsTableName);
    stringBuffer.append(TEXT_1009);
    stringBuffer.append(varsColumnName);
    stringBuffer.append(TEXT_1010);
    stringBuffer.append(varExpression);
    stringBuffer.append(TEXT_1011);
    
                    }
                }

                if(tMapUtil.needsRejectOutput()){
                
    stringBuffer.append(TEXT_1012);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1013);
    
                }
                if(tMapUtil.getOutputTables().size() > 0){
                    for(ExternalMapperTable outputTable : tMapUtil.getOutputTables()){
                        String connName = outputTable.getIsJoinTableOf() == null ? tMapUtil.getTableName(outputTable.getName()) : tMapUtil.getTableName(outputTable.getIsJoinTableOf());
                        List<ExternalMapperTableEntry> entries = outputTable.getMetadataTableEntries();
                        String outputTableExpression = outputTable.isActivateExpressionFilter() && outputTable.getExpressionFilter()!=null && !"".equals(outputTable.getExpressionFilter().trim()) ? outputTable.getExpressionFilter().trim() : null;
                        if(outputTableExpression != null){
                        
    stringBuffer.append(TEXT_1014);
    stringBuffer.append(outputTableExpression);
    stringBuffer.append(TEXT_1015);
    
                        }
                        if(tMapUtil.needsRejectOutput()){
                        
    stringBuffer.append(TEXT_1016);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1017);
    
                        }
                        if (entries != null && tMapUtil.isOutputConnected(connName)) {
                            for (ExternalMapperTableEntry entry : entries) {
                            	String columnName = entry.getName();
		                        String expression = entry.getExpression();
		                        if (expression != null && expression.trim().length() != 0) {
                                
    stringBuffer.append(TEXT_1018);
    stringBuffer.append(getOutValue(connName));
    stringBuffer.append(TEXT_1019);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_1020);
    stringBuffer.append(expression);
    stringBuffer.append(TEXT_1021);
    
		                        }
                            }
                            output(null, getOutValue(connName));
                        }
                        if(outputTableExpression != null){
                        
    stringBuffer.append(TEXT_1022);
    
                        }
                    }
                }
                if(tMapUtil.needsRejectOutput()){
                    for(ExternalMapperTable rejectOutputTable : tMapUtil.getOutputTablesReject()){
                        String connName = rejectOutputTable.getIsJoinTableOf() == null ? tMapUtil.getTableName(rejectOutputTable.getName()) : tMapUtil.getTableName(rejectOutputTable.getIsJoinTableOf());
                        String rejectOutputTableExpression = rejectOutputTable.isActivateExpressionFilter() && rejectOutputTable.getExpressionFilter()!=null && !"".equals(rejectOutputTable.getExpressionFilter().trim()) ? rejectOutputTable.getExpressionFilter().trim() : null;
                        List<ExternalMapperTableEntry> entries = rejectOutputTable.getMetadataTableEntries();
                        
    stringBuffer.append(TEXT_1023);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1024);
    stringBuffer.append(rejectOutputTableExpression != null ? " && " + rejectOutputTableExpression : "");
    stringBuffer.append(TEXT_1025);
    
                            if (entries != null && tMapUtil.isOutputConnected(connName)) {
                                for (ExternalMapperTableEntry entry : entries) {
                                	String columnName = entry.getName();
			                        String expression = entry.getExpression();
			                        if (expression != null && expression.trim().length() != 0) {
                                    
    stringBuffer.append(TEXT_1026);
    stringBuffer.append(getOutValue(connName));
    stringBuffer.append(TEXT_1027);
    stringBuffer.append(columnName);
    stringBuffer.append(TEXT_1028);
    stringBuffer.append(expression);
    stringBuffer.append(TEXT_1029);
    
                                    }
                                }
                                output(null, getOutValue(connName));
                            }
                            
    stringBuffer.append(TEXT_1030);
    
                    }
                }
                if(tMapUtil.getMainTableExpression() != null){
                
    stringBuffer.append(TEXT_1031);
    
                }
            }
        }

        Mapper mapper = new Mapper();
        if(tMapUtil.hasMultipleOutputs()){
            mapper.setType(M_TYPE_MO);
            mapper.init(node, cid, null, tMapUtil.getMainName(), null, tMapUtil.getOutConnNamesAsMap());
        }else{
            mapper.init(node, cid, null, tMapUtil.getMainName(), null, tMapUtil.getOutConnName());
        }
        mapper.generate();
    } else if (tMapUtil.isReplicatedJoin()) {
    
    

/**
 * A common class that can be used to generate replicated join cache
 * initialization and use.  Different subclasses can use different stategies to
 * perform these operations, and they will stay coherent during the task.
 *
 * Note that the "cache" refered to here is the in-memory cache containing the
 * lookup values.
 */
abstract class TMapReplicatedLookupStrategy {

    protected String lookupRowName;
    protected String cacheName;
    protected String keyClass;
    protected List<IMetadataColumn> keyColumnsList;
    protected String valueClassNoStruct;
    protected String valueClass;
    protected List<IMetadataColumn> valueColumnsList;

    TMapReplicatedLookupStrategy(ExternalMapperTable lookupTable, IConnection lookupConn,
            List<IMetadataColumn> keyColumnsList,
            List<IMetadataColumn> filterColumnsList) {

        lookupRowName = lookupConn.getName();

        keyClass = "LookupKey_" + cid + "Struct";
        valueClassNoStruct = lookupRowName + "_filter_" + cid;
        valueClass = valueClassNoStruct + "Struct";
        cacheName = "cached_" + lookupRowName;

        // TODO: don't include the key values in the filter columns list.
        this.keyColumnsList = keyColumnsList;
        valueColumnsList = filterColumnsList;
    }

    /**
     * Generates the member declarations necessary for the in-memory cache.
     * This code belongs in the top-level of the Mapper task implementing the
     * join.
     */
    abstract public void generatePrepareCache();

    /**
     * Initializes the member declarations necessary for the in-memory cache,
     * normally once per declaration.  This is executed during the setup of the
     * Mapper task.
     */
    abstract public void generateConfigureCache();

    /**
     * Called multiple times in the Mapper task to add a new value to the
     * in-memory cache.
     */
    abstract public void generateConfigureAddLookupValueToCache(String lookupValueName);

    /**
     * @return a code snippet that returns the values from the in-memory cache,
     *    wrapped in a List (null or empty list if no values exist).
     */
    abstract public String getCodeFetchValuesFromCache(String lookupKeyName);

    /**
     * Creates and initializes a valueClass instance with the given name, from
     * the incoming lookup stream.
     */
    public void generateLookupValue(String lookupValueName) {
        // TODO: don't include values that are also in the key.
        
    stringBuffer.append(TEXT_1032);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1033);
    stringBuffer.append(lookupValueName);
    stringBuffer.append(TEXT_1034);
    stringBuffer.append( valueClass );
    stringBuffer.append(TEXT_1035);
    
        for (IMetadataColumn column : valueColumnsList) {
            
    stringBuffer.append(TEXT_1036);
    stringBuffer.append(lookupValueName);
    stringBuffer.append(TEXT_1037);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1038);
    stringBuffer.append(lookupRowName);
    stringBuffer.append(TEXT_1039);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1040);
    
        }
    }

    /**
     * Creates and initializes a valueClass instance with the given name, from
     * the incoming lookup stream.
     */
    public void generateCloseCache() {
        
    stringBuffer.append(TEXT_1041);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1042);
    
    }

    /**
     * Generates the structures used in this lookup.
     */
    public void generateCodeFilteredLookup() {
        new StructHelper(valueClassNoStruct, valueColumnsList, false).generateCode();
    }
}


/**
 * Contains a strategy for generating an in-memory cache that retains all
 * matches for a given key.
 */
class TMapReplicatedLookupAllMatches extends TMapReplicatedLookupStrategy {

    TMapReplicatedLookupAllMatches(ExternalMapperTable lookupTable, IConnection lookupConn,
            List<IMetadataColumn> keyColumnsList,
            List<IMetadataColumn> filterColumnsList) {
        super(lookupTable, lookupConn, keyColumnsList, filterColumnsList);
    }

    public void generatePrepareCache() {
        
    stringBuffer.append(TEXT_1043);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1044);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1045);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1046);
    
    }

    public void generateConfigureCache() {
        
    stringBuffer.append(TEXT_1047);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1048);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1049);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1050);
    
    }


    public void generateConfigureAddLookupValueToCache(String lookupValueName) {
        
    stringBuffer.append(TEXT_1051);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1052);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1053);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1054);
    stringBuffer.append(TEXT_1055);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1056);
     generateLookupValue(lookupValueName); 
    stringBuffer.append(TEXT_1057);
    
    }

    public String getCodeFetchValuesFromCache(String lookupKeyName) {
       return cacheName + ".get(" + lookupKeyName + ")";
    }
}


/**
 * Contains a strategy for generating an in-memory cache that retains a
 * single (first) match for a given key.
 */
class TMapReplicatedLookupUniqueMatch extends TMapReplicatedLookupStrategy {

    TMapReplicatedLookupUniqueMatch(ExternalMapperTable lookupTable, IConnection lookupConn,
            List<IMetadataColumn> keyColumnsList,
            List<IMetadataColumn> filterColumnsList) {
        super(lookupTable, lookupConn, keyColumnsList, filterColumnsList);
    }

    public void generatePrepareCache() {
        
    stringBuffer.append(TEXT_1058);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1059);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1060);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1061);
    
    }

    public void generateConfigureCache() {
        
    stringBuffer.append(TEXT_1062);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1063);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1064);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1065);
    
    }

    public void generateConfigureAddLookupValueToCache(String lookupValueName) {
        
    stringBuffer.append(TEXT_1066);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1067);
     generateLookupValue(lookupValueName); 
    stringBuffer.append(TEXT_1068);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1069);
    
    }

    public String getCodeFetchValuesFromCache(String lookupKeyName) {
        return cacheName + ".containsKey(" + lookupKeyName + ")"
                + "? java.util.Arrays.asList(" + cacheName + ".get(" + lookupKeyName + "))"
                + ": null";
    }
}

/**
 * Contains a strategy for generating an in-memory cache that retains a
 * single (first) match for a given key.
 */
class TMapReplicatedLookupBitSet extends TMapReplicatedLookupStrategy {

    final protected String idKeyColumnName;

    TMapReplicatedLookupBitSet(ExternalMapperTable lookupTable, IConnection lookupConn,
            List<IMetadataColumn> keyColumnsList,
            List<IMetadataColumn> filterColumnsList,
            String idKeyColumnName) {
        super(lookupTable, lookupConn, keyColumnsList, filterColumnsList);
        this.idKeyColumnName = idKeyColumnName;
    }

    public void generatePrepareCache() {
        
    stringBuffer.append(TEXT_1070);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1071);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1072);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1073);
    stringBuffer.append(valueClass);
    stringBuffer.append(TEXT_1074);
    
    }

    public void generateConfigureCache() {
        
    stringBuffer.append(TEXT_1075);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1076);
    
    }

    public void generateConfigureAddLookupValueToCache(String lookupValueName) {
        
    stringBuffer.append(TEXT_1077);
    stringBuffer.append(cacheName);
    stringBuffer.append(TEXT_1078);
    stringBuffer.append(idKeyColumnName);
    stringBuffer.append(TEXT_1079);
    
    }

    public String getCodeFetchValuesFromCache(String lookupKeyName) {
        return cacheName + ".get(" + lookupKeyName + "." + idKeyColumnName + ")"
                + "? " + cacheName + "DefaultSingle"
                + ": null";
    }
}


/**
 * A factory for creating TMapReplicatedLookupStrategies for specific lookup
 * connections.
 */
class TMapReplicatedLookupFactory {

    /** helper objects for generating replicated join caches and lookups. */
    private final TMapUtilLegacy tMapUtil;

    /** helper objects for generating replicated join caches and lookups. */
    private  HashMap<String, TMapReplicatedLookupStrategy> replicatedUtils;

    public TMapReplicatedLookupFactory(TMapUtilLegacy util) {
        tMapUtil = util;
    }

    /**
     * @return true if the join key is appropriate for use as an index into a
     *     java.util.BitSet (i.e., the BitSet could be expected to fit into
     *     memory).
     */
    private boolean isJoinKeyForReplicatedBitSet(List<IMetadataColumn> joinKeyColumns) {
        if (joinKeyColumns.size() != 1 || joinKeyColumns.get(0).isNullable())
            return false;

        JavaType javaType = JavaTypesManager.getJavaTypeFromId(joinKeyColumns.get(0).getTalendType());
        // For the moment, don't permit INTEGER types to use the BitSet for the
        // replicated in-memory cache, since it can result in memory usage on
        // order of the largest key (i.e. 2GB maximum).
        // TODO: some validation with tMRInput and tMROutput to ensure that an
        // Integer key is less than ~100M.  Optimisation of writing/reading the
        // temporary file.
        return (javaType == JavaTypesManager.BYTE || javaType == JavaTypesManager.SHORT);
        // return (javaType == JavaTypesManager.BYTE || javaType == JavaTypesManager.SHORT
        //        || javaType == JavaTypesManager.INTEGER);
    }

    /**
     * @return a helper class per lookup connection that manages the replication
     *     of the lookup values in the distributed cache.
     */
    public TMapReplicatedLookupStrategy getReplicatedUtil(IConnection lookupConn) {
        if (replicatedUtils == null)
            replicatedUtils = new HashMap<String, TMapReplicatedLookupStrategy>();

        TMapReplicatedLookupStrategy replicatedUtil = replicatedUtils.get(lookupConn.getName());
        if (replicatedUtil == null) {
            // Create the strategy depending on the configuration of the keys
            // and values.
            ExternalMapperTable lookupTable = tMapUtil.getLookupTable(lookupConn.getName());
            List<IMetadataColumn> joinKeyColumns = tMapUtil.getJoinKeyColumns();
            List<IMetadataColumn> filterColumns = tMapUtil.getFilterColumnsList(lookupConn);

            if (filterColumns.size() == 0 && isJoinKeyForReplicatedBitSet(joinKeyColumns)
                    && !"ALL_MATCHES".equals(lookupTable.getMatchingMode())) {
                replicatedUtil = new TMapReplicatedLookupBitSet(
                        lookupTable,
                        lookupConn,
                        joinKeyColumns,
                        filterColumns,
                        tMapUtil.getJoinKeyForTable(lookupTable.getName()).get(joinKeyColumns.get(0).getLabel()));
            } else if ("ALL_MATCHES".equals(lookupTable.getMatchingMode())) {
                replicatedUtil = new TMapReplicatedLookupAllMatches(
                        lookupTable,
                        lookupConn,
                        joinKeyColumns,
                        filterColumns);
            } else {
                replicatedUtil = new TMapReplicatedLookupUniqueMatch(
                        lookupTable,
                        lookupConn,
                        joinKeyColumns,
                        filterColumns);
            }
            replicatedUtils.put(lookupConn.getName(), replicatedUtil);
        }

        return replicatedUtil;
    }
}


    

final StructHelper lookupKeyStruct = new StructHelper("LookupKey_"+cid,
        tMapUtil.getJoinKeyColumns(), true);
lookupKeyStruct.generateCode();

final String lookupConnName = tMapUtil.getLookupConns().get(0).getName();

// Generate the structure to hold the filtered incoming main connection.
final List<IMetadataColumn> mainTableFilterColumnList = tMapUtil.getFilterColumnsList(tMapUtil.getMainConn());
final StructHelper mainTableFilterStruct = new StructHelper(tMapUtil.getMainName() + "_filter_" + cid,
        mainTableFilterColumnList, false);
mainTableFilterStruct.generateCode();

final TMapReplicatedLookupFactory tReplicatedFactory = new TMapReplicatedLookupFactory(tMapUtil);

// Generate the structures to hold the filtered incoming lookup connections.
for(IConnection lookupConn : tMapUtil.getLookupConns())
    tReplicatedFactory.getReplicatedUtil(lookupConn).generateCodeFilteredLookup();

class MainMapper extends MapperHelper{
    // The key and values to use in the lookup structure.
    String keyClass = lookupKeyStruct.getStructName();

     public void prepare() {
        // The structure for the Vars table.
        tMapUtil.generatePrepareVarsTables();

        // A cache for each lookup connection.
        for (IConnection lookupConn : tMapUtil.getLookupConns())
            tReplicatedFactory.getReplicatedUtil(lookupConn).generatePrepareCache();
        // Create default structures for all of the lookup inputs (in case of
        // left outer join).
        for (IConnection lookupConn : tMapUtil.getLookupConns()) {
            String lookupConnName = lookupConn.getName();
            
    stringBuffer.append(TEXT_1080);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1081);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1082);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1083);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1084);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1085);
    
        }
    }

    public void configure() {
        for (IConnection lookupConn : tMapUtil.getLookupConns()) {
            String lookupConnName = lookupConn.getName();

            tReplicatedFactory.getReplicatedUtil(lookupConn).generateConfigureCache();
            
    stringBuffer.append(TEXT_1086);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1087);
    stringBuffer.append(tMapUtil.getLookupSourceLocal(lookupConn));
    stringBuffer.append(TEXT_1088);
    stringBuffer.append(TEXT_1089);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1090);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1091);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1092);
    
                // If the input is tMRInput, we need to prevent the
                // hard-coded HDFS inputs from overriding the locally
                // cached files.
                if ("tMRInput".equals(tMapUtil.getLookupConns().get(0).getSource().getComponent().getName())) {
                
    stringBuffer.append(TEXT_1093);
    
                }
                
    stringBuffer.append(TEXT_1094);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1095);
    stringBuffer.append(TEXT_1096);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1097);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1098);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1099);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1100);
    stringBuffer.append(TEXT_1101);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1102);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1103);
    
                        ExternalMapperTable lookupTable = tMapUtil.getLookupTable(lookupConnName);
                        if(lookupTable.isSelfFilter() && (tMapUtil.getOutputTablesRejectInner().size() == 0 || !lookupTable.isInnerJoin())){
                            String lookupTableExpression = lookupTable.isActivateExpressionFilter() && lookupTable.getExpressionFilter()!=null && !"".equals(lookupTable.getExpressionFilter().trim()) ? lookupTable.getExpressionFilter().trim() : null;
                                if (lookupTableExpression != null) {
                            
    stringBuffer.append(TEXT_1104);
    stringBuffer.append(lookupTableExpression);
    stringBuffer.append(TEXT_1105);
    
                            }
                        }

                        // Construct the key from the lookup row to quickly find it in the replicated cache.
                        tMapUtil.generateInitializeJoinKeyStructFromLookup("lookupKey", lookupConnName);
                        tReplicatedFactory.getReplicatedUtil(lookupConn).generateConfigureAddLookupValueToCache("lookupValue");
                        
    stringBuffer.append(TEXT_1106);
    
        }
    }

    public void map() {
        
    stringBuffer.append(TEXT_1107);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1108);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1109);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1110);
     if(tMapUtil.getMainTableExpression() != null){ 
    stringBuffer.append(TEXT_1111);
    stringBuffer.append(tMapUtil.getMainTableExpression());
    stringBuffer.append(TEXT_1112);
     } 
    stringBuffer.append(TEXT_1113);
    stringBuffer.append(TEXT_1114);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1115);
    stringBuffer.append(keyClass);
    stringBuffer.append(TEXT_1116);
    
        tMapUtil.generateInitializeJoinKeyStructFromMain("lookupKey");

        // Get the matching lookup rows for the given key.
        for (IConnection lookupConn : tMapUtil.getLookupConns()) {
            String lookupConnName = lookupConn.getName();
            
    stringBuffer.append(TEXT_1117);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1118);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1119);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1120);
    stringBuffer.append(TEXT_1121);
    stringBuffer.append( tReplicatedFactory.getReplicatedUtil(lookupConn).getCodeFetchValuesFromCache("lookupKey") );
    stringBuffer.append(TEXT_1122);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1123);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1124);
    
        }

        // Refactor this into tMapUtils
        tMapUtil.generateJoinWithLookupRows(this, null);
    }

    public void close() {
        for (IConnection lookupConn : tMapUtil.getLookupConns())
            tReplicatedFactory.getReplicatedUtil(lookupConn).generateCloseCache();
    }
}


MainMapper mainMapper = new MainMapper();
if(tMapUtil.hasMultipleOutputs()){
    mainMapper.setType(M_TYPE_MO);
    mainMapper.init(node, cid, null, tMapUtil.getMainName(), null, tMapUtil.getOutConnNamesAsMap());
}else{
    mainMapper.init(node, cid, null, tMapUtil.getMainName(), null, tMapUtil.getOutConnName());
}
mainMapper.generate();

    
    }else{
        // With reduce-side join.
        // tMap only support same key on any lookup. but different lookup should have different columns name

        /**
         * The JoinKeyStructHelper creates the composite key that contains the
         * fields that are used to find rows in lookup tables from the main
         * table.
         */
        class JoinKeyStructHelper extends StructHelper{

            public JoinKeyStructHelper(String structName, List<IMetadataColumn> columns, boolean genComparator){
                super(structName, columns, genComparator);
                tagIndex = structName+"_tag";
            }

            public String tagIndex;

            public void declareVars(){
            
    stringBuffer.append(TEXT_1125);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1126);
    
            }
            public void overrideWrite(String dataOutput){
                super.overrideWrite(dataOutput);
                
    stringBuffer.append(TEXT_1127);
    stringBuffer.append(dataOutput);
    stringBuffer.append(TEXT_1128);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1129);
    
            }
            public void overrideReadFields(String dataInput){
                super.overrideReadFields(dataInput);
                
    stringBuffer.append(TEXT_1130);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1131);
    stringBuffer.append(dataInput);
    stringBuffer.append(TEXT_1132);
    
            }
            public void addMethods(){
            
    stringBuffer.append(TEXT_1133);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1134);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1135);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1136);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1137);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_1138);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1139);
    stringBuffer.append(tagIndex);
    stringBuffer.append(TEXT_1140);
    
            }
        }
        String joinKeyName = "TaggedJoinKey_"+cid;
        final JoinKeyStructHelper joinKeyStruct = new JoinKeyStructHelper(joinKeyName, tMapUtil.getJoinKeyColumns(), false);
        joinKeyStruct.generateCode();

        // Tag values for the composite key correspond to the order of lookup
        // tables.  The main input is assigned a larger value to appear after
        // all lookups.
        final int mainTagValue = tMapUtil.getLookupTables().size();

        /**
         * The MapOutputValueStructHelper is the composite value that contains
         * different rows from input tables depending on the tag.
         */
        class MapOutputValueStructHelper extends StructHelper{

            public MapOutputValueStructHelper(String structName){
                super(structName, null, false);
            }

            public void generateCode(){
            
    stringBuffer.append(TEXT_1141);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_1142);
    stringBuffer.append( mainTagValue );
    stringBuffer.append(TEXT_1143);
    stringBuffer.append(tMapUtil.getMainName());
    stringBuffer.append(TEXT_1144);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1145);
    for (int i = 0; i < mainTagValue; i++) {
    stringBuffer.append(TEXT_1146);
    stringBuffer.append( i );
    stringBuffer.append(TEXT_1147);
    stringBuffer.append(tMapUtil.getLookupTables().get(i).getName());
    stringBuffer.append(TEXT_1148);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1149);
    }
    stringBuffer.append(TEXT_1150);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_1151);
    stringBuffer.append(structName);
    stringBuffer.append(TEXT_1152);
    
            }
        }
        String mapOutputValueName = "TaggedMapOutput_"+cid;
        MapOutputValueStructHelper mapOutputValueStruct = new MapOutputValueStructHelper(mapOutputValueName);
        mapOutputValueStruct.generateCode();

        final List<IMetadataColumn> mainTableFilterColumnList = tMapUtil.getFilterColumnsList(tMapUtil.getMainConn());
        final StructHelper mainTableFilterStruct = new StructHelper(tMapUtil.getMainName() + "_filter_" + cid,
                mainTableFilterColumnList, false);
        mainTableFilterStruct.generateCode();

        class MainMapper extends MapperHelper{
            public void prepare(){
            
    stringBuffer.append(TEXT_1153);
    stringBuffer.append(mainTableFilterStruct.getStructName());
    stringBuffer.append(TEXT_1154);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1155);
    
            }

            public void configure(){
            
    stringBuffer.append(TEXT_1156);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1157);
    stringBuffer.append(mainTableFilterStruct.getStructName());
    stringBuffer.append(TEXT_1158);
    
            }

            public void map(){
            
    stringBuffer.append(TEXT_1159);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1160);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1161);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1162);
    for(IMetadataColumn column : mainTableFilterColumnList){
    stringBuffer.append(TEXT_1163);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1164);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1165);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1166);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1167);
    }
    stringBuffer.append(TEXT_1168);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1169);
    stringBuffer.append( mainTagValue );
    stringBuffer.append(TEXT_1170);
    
                if(tMapUtil.getMainTableExpression() != null){
                
    stringBuffer.append(TEXT_1171);
    stringBuffer.append(tMapUtil.getMainTableExpression());
    stringBuffer.append(TEXT_1172);
    
                }
                
    stringBuffer.append(TEXT_1173);
    stringBuffer.append(getOutKey());
    stringBuffer.append(TEXT_1174);
    stringBuffer.append(joinKeyStruct.tagIndex);
    stringBuffer.append(TEXT_1175);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1176);
    
                tMapUtil.generateInitializeJoinKeyStructFromMain(getOutKey());
                
    stringBuffer.append(TEXT_1177);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_1178);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1179);
    stringBuffer.append(TEXT_1180);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_1181);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1182);
    
                this.output(getOutKey(), getOutValue());
                if(tMapUtil.getMainTableExpression() != null){
                
    stringBuffer.append(TEXT_1183);
    
                }
            }
        }
        MainMapper mainMapper = new MainMapper();
        mainMapper.init(node, cid+tMapUtil.getMainName(), null, tMapUtil.getMainName(), joinKeyName, mapOutputValueName);
        mainMapper.generate();

        for(IConnection lookupConn : tMapUtil.getLookupConns()){
            final String lookupConnName = lookupConn.getName();
            final int lookupTagValue = tMapUtil.getLookupTables().indexOf(tMapUtil.getLookupTable(lookupConnName));

            final List<IMetadataColumn> lookupTableFilterColumnList = tMapUtil.getFilterColumnsList(lookupConn);

            final StructHelper lookupTableFilterStruct = new StructHelper(lookupConnName + "_filter_" + cid,
                    lookupTableFilterColumnList, false);
            lookupTableFilterStruct.generateCode();

            class LookupMapper extends MapperHelper{
                public void prepare(){
                
    stringBuffer.append(TEXT_1184);
    stringBuffer.append(lookupTableFilterStruct.getStructName());
    stringBuffer.append(TEXT_1185);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1186);
    
                }

                public void configure(){
                
    stringBuffer.append(TEXT_1187);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1188);
    stringBuffer.append(lookupTableFilterStruct.getStructName());
    stringBuffer.append(TEXT_1189);
    
                }

                public void map(){
                
    stringBuffer.append(TEXT_1190);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1191);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1192);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1193);
    
                    ExternalMapperTable lookupTable = tMapUtil.getLookupTable(lookupConnName);
                    if(lookupTable.isSelfFilter() && (tMapUtil.getOutputTablesRejectInner().size() == 0 || !lookupTable.isInnerJoin())){
                        String lookupTableExpression = lookupTable.isActivateExpressionFilter() && lookupTable.getExpressionFilter()!=null && !"".equals(lookupTable.getExpressionFilter().trim()) ? lookupTable.getExpressionFilter().trim() : null;
                        if(lookupTableExpression != null){
                        
    stringBuffer.append(TEXT_1194);
    stringBuffer.append(lookupTableExpression);
    stringBuffer.append(TEXT_1195);
    
                        }
                    }
    for(IMetadataColumn column : lookupTableFilterColumnList){
    stringBuffer.append(TEXT_1196);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1197);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1198);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1199);
    stringBuffer.append(column.getLabel());
    stringBuffer.append(TEXT_1200);
    }
    stringBuffer.append(TEXT_1201);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1202);
    stringBuffer.append( lookupTagValue );
    stringBuffer.append(TEXT_1203);
    stringBuffer.append(TEXT_1204);
    stringBuffer.append(getOutKey());
    stringBuffer.append(TEXT_1205);
    stringBuffer.append(joinKeyStruct.tagIndex);
    stringBuffer.append(TEXT_1206);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1207);
    
                    tMapUtil.generateInitializeJoinKeyStructFromLookup(getOutKey(), lookupConnName);
                    
    stringBuffer.append(TEXT_1208);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_1209);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1210);
    stringBuffer.append(TEXT_1211);
    stringBuffer.append(getOutValue());
    stringBuffer.append(TEXT_1212);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1213);
    
                    this.output(getOutKey(), getOutValue());
                }
            }
            LookupMapper lookupMapper = new LookupMapper();
            lookupMapper.init(node, cid+lookupConnName, null, lookupConnName, joinKeyName, mapOutputValueName);
            lookupMapper.generate();
        }
        
    stringBuffer.append(TEXT_1214);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1215);
    stringBuffer.append(joinKeyStruct.getStructName());
    stringBuffer.append(TEXT_1216);
    stringBuffer.append(joinKeyStruct.getStructName());
    stringBuffer.append(TEXT_1217);
    
        ComparatorHelper joinKeyGroupingComparator = new ComparatorHelper();
        joinKeyGroupingComparator.init("JoinKeyGroupingComparator_"+cid, joinKeyStruct.columns, joinKeyStruct.getStructName());
        joinKeyGroupingComparator.generateCode();

        class JoinKeySortComparator extends ComparatorHelper{
            public void compareAfterColumns(){
            
    stringBuffer.append(TEXT_1218);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1219);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1220);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1221);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1222);
    
            }
        }
        JoinKeySortComparator joinKeySortComparator = new JoinKeySortComparator();
        joinKeySortComparator.init("JoinKeySortComparator_"+cid, joinKeyStruct.columns, joinKeyStruct.getStructName());
        joinKeySortComparator.generateCode();

        class Combiner extends ReducerHelper{
            public void reduce(){
            
    stringBuffer.append(TEXT_1223);
    stringBuffer.append(getInKeyClass());
    stringBuffer.append(TEXT_1224);
    stringBuffer.append(getInKey());
    stringBuffer.append(TEXT_1225);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1226);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1227);
    stringBuffer.append(TEXT_1228);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1229);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1230);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1231);
    
                    for(ExternalMapperTable lookupTable : tMapUtil.getLookupTables()){
                        if(!"ALL_MATCHES".equals(lookupTable.getMatchingMode())){
                            String lookupTableName = lookupTable.getName();
                            
    stringBuffer.append(TEXT_1232);
    stringBuffer.append(lookupTableName);
    stringBuffer.append(TEXT_1233);
    stringBuffer.append( tMapUtil.getLookupTables().indexOf(lookupTable) );
    stringBuffer.append(TEXT_1234);
    stringBuffer.append(getInValue());
    stringBuffer.append(TEXT_1235);
    stringBuffer.append(lookupTableName);
    stringBuffer.append(TEXT_1236);
    this.output(getInKey(), getInValue());
    stringBuffer.append(TEXT_1237);
    stringBuffer.append(lookupTableName);
    stringBuffer.append(TEXT_1238);
    
                        }
                    }
                    
    this.output(getInKey(), getInValue());
    stringBuffer.append(TEXT_1239);
    
            }
        }
        if(tMapUtil.hasUniqueLookupMatch()) {
            Combiner combiner = new Combiner();
            combiner.setType(R_TYPE_COMBINER);
            combiner.init(cid, joinKeyName, mapOutputValueName, joinKeyName, mapOutputValueName);
            combiner.generate();
        }
        class Reducer extends ReducerHelper{

            public void prepare(){
            
    stringBuffer.append(TEXT_1240);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1241);
    
                tMapUtil.generatePrepareVarsTables();
            }
            public void configure(){
            
    stringBuffer.append(TEXT_1242);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1243);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1244);
    
            }
            public void reduce(){
                // Create ArrayList for all of the lookup inputs.
                for (IConnection lookupConn : tMapUtil.getLookupConns()) {
                    String lookupConnName = lookupConn.getName();
                    
    stringBuffer.append(TEXT_1245);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1246);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1247);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1248);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1249);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1250);
    stringBuffer.append(TEXT_1251);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1252);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1253);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1254);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1255);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1256);
    
                }

                // Initialize the lists from the lookup.  This goes through all of the values up to
                // and *including* the first main row, which is temporarily stored in lookupValue_cid.
                
    stringBuffer.append(TEXT_1257);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1258);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1259);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1260);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1261);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1262);
    
                    for(IConnection lookupConn : tMapUtil.getLookupConns()) {
                        String lookupConnName = lookupConn.getName();
                        int lookupTagValue = tMapUtil.getLookupTables().indexOf(tMapUtil.getLookupTable(lookupConnName));
                        
    stringBuffer.append(TEXT_1263);
    stringBuffer.append(lookupTagValue);
    stringBuffer.append(TEXT_1264);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1265);
    
                            for(ExternalMapperTable lookupTable : tMapUtil.getLookupTables()){
                                if(lookupTable.getName().equals(lookupConnName)){
                                    if(!"ALL_MATCHES".equals(lookupTable.getMatchingMode())){
    stringBuffer.append(TEXT_1266);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1267);
    
                                    }
                                }
                            }
                            
    stringBuffer.append(TEXT_1268);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1269);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1270);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1271);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1272);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1273);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1274);
    stringBuffer.append(lookupConnName);
    stringBuffer.append(TEXT_1275);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1276);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1277);
    
                    }
                    
    stringBuffer.append(TEXT_1278);
    stringBuffer.append(mainTagValue);
    stringBuffer.append(TEXT_1279);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1280);
     // Process each main row.  The first main row may be stored in lookupValue_cid, and should be used if non-null.
                
    stringBuffer.append(TEXT_1281);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1282);
    stringBuffer.append(mainTagValue);
    stringBuffer.append(TEXT_1283);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1284);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1285);
    stringBuffer.append(TEXT_1286);
    stringBuffer.append(getInValueClass());
    stringBuffer.append(TEXT_1287);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1288);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1289);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1290);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1291);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1292);
    stringBuffer.append(TEXT_1293);
    stringBuffer.append(tMapUtil.getMainName());
    stringBuffer.append(TEXT_1294);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1295);
    stringBuffer.append(tMapUtil.getMainName());
    stringBuffer.append(TEXT_1296);
    stringBuffer.append(tMapUtil.getMainName());
    stringBuffer.append(TEXT_1297);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1298);
    stringBuffer.append(cid);
    stringBuffer.append(TEXT_1299);
    
                    // All of the common code for generating the loop for MapReduce is contained here.
                    tMapUtil.generateJoinWithLookupRows(null, this);
                    
    stringBuffer.append(TEXT_1300);
    
            }
        }
        Reducer reducer = new Reducer();
        if(tMapUtil.hasMultipleOutputs()){
            reducer.setType(R_TYPE_MO);
            reducer.init(cid, joinKeyName, mapOutputValueName, null, tMapUtil.getOutConnNamesAsMap());
        }else{
            reducer.init(cid, joinKeyName, mapOutputValueName, null, tMapUtil.getOutConnName());
        }
        reducer.generate();
    }
    
    return stringBuffer.toString();
  }
}
