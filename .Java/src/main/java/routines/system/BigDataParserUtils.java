// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package routines.system;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Reimplementation of ParserUtils. Trying to use org.apache.commons.lang.StringUtils as much a possible and trying to
 * map any numric type to any numeric type, without a "toString" conversion
 *
 */
public class BigDataParserUtils extends ParserUtils {

    public static final boolean defaultValueBoolean = false;

    public static final int defaultValueInt = 0;

    public static final byte defaultValueByte = 0;

    public static final char defaultValueChar = ' ';

    public static final double defaultValueDouble = 0d;

    public static final float defaultValueFloat = 0f;

    public static final long defaultValueLong = 0l;

    public static final short defaultValueShort = 0;

    /**
     * StringUtils from apache commons-lang3 3.3.2
     *
     * We use this implementation in order to avoid using the common lang package and be compatible with any hadoop
     * distribution.
     *
     * Checks if a CharSequence is whitespace, empty ("") or null.
     *
     * @param cs the CharSequence to check, may be null
     * @returntrue if the CharSequence is null, empty or whitespace
     */
    public static boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {

            return true;
        }

        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;

            }
        }
        return true;
    }

    /**
     * the source should be a string wrapped in chars[ ] which stands for it is a collection
     *
     * @param stSrc
     * @param fieldSep
     * @return
     */
    public static List<String> parseTo_List(final String strSrc, String fieldSep) {
        if (isBlank(strSrc)) {
            return null;
        }
        List<String> list = new ArrayList<String>();

        // the source string is wrap in [] which means it is a collection
        if ((fieldSep == null || "".equals(fieldSep)) || !(strSrc.startsWith("[") && strSrc.endsWith("]"))) {
            list.add(strSrc);
            return list;
        }
        String strTemp = strSrc.substring(1, strSrc.length() - 1); // remove the [ ]
        for (String str : strTemp.split(fieldSep, -1)) {
            list.add(str);
        }
        return list;
    }

    /**
     * This method returns its List argument.
     *
     * @param list the list to parse into itself.
     * @return the list itself
     */
    public static <T> List<T> parseTo_List(List<T> list) {
        return list;
    }

    public static Character parseTo_Character(String s) {
        if (isBlank(s)) {
            return null;
        }
        return s.charAt(0);
    }

    public static Byte parseTo_Byte(String s) {
        if (isBlank(s)) {
            return null;
        }
        return Byte.decode(s).byteValue();
    }

    public static Byte parseTo_Byte(String s, boolean isDecode) {
        if (isBlank(s)) {
            return null;
        }
        if (isDecode) {
            return Byte.decode(s).byteValue();
        } else {
            return Byte.parseByte(s);
        }
    }

    public static byte parseTo_byte(String s) {
        if (isBlank(s)) {
            return defaultValueByte;
        }
        return parseTo_Byte(s);
    }

    public static Double parseTo_Double(String s) {
        if (isBlank(s)) {
            return null;
        }
        return Double.parseDouble(s);
    }

    public static double parseTo_double(String s) {
        if (isBlank(s)) {
            return defaultValueDouble;
        }
        return parseTo_Double(s);
    }

    public static float parseTo_float(String s) {
        if (isBlank(s)) {
            return defaultValueFloat;
        }
        return Float.parseFloat(s);
    }

    public static Float parseTo_Float(String s) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_float(s);
    }

    public static int parseTo_int(String s) {
        if (isBlank(s)) {
            return defaultValueInt;
        }
        return Integer.parseInt(s);
    }

    public static Integer parseTo_Integer(String s) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_int(s);
    }

    public static Integer parseTo_Integer(String s, boolean isDecode) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_int(s, isDecode);
    }

    public static short parseTo_short(String s) {
        if (isBlank(s)) {
            return defaultValueShort;
        }
        return Short.parseShort(s);
    }

    public static Short parseTo_Short(String s) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_short(s);
    }

    public static Short parseTo_Short(String s, boolean isDecode) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_short(s, isDecode);
    }

    public static long parseTo_long(String s) {
        if (isBlank(s)) {
            return defaultValueLong;
        }
        return Long.parseLong(s);
    }

    public static Long parseTo_Long(String s) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_long(s);
    }

    public static Long parseTo_Long(String s, boolean isDecode) {
        if (isBlank(s)) {
            return null;
        }
        return parseTo_long(s, isDecode);
    }

    public static Boolean parseTo_Boolean(String s) {
        if (isBlank(s)) {
            return null;
        }
        if ("1".equals(s)) { //$NON-NLS-1$
            return Boolean.parseBoolean("true"); //$NON-NLS-1$
        }
        return Boolean.parseBoolean(s);
    }

    public static Boolean parseTo_Boolean(Boolean b) {
        return b;
    }

    public static Boolean parseTo_Boolean(boolean b) {
        return b;
    }

    public static boolean parseTo_boolean(String s) {
        if (isBlank(s)) {
            return defaultValueBoolean;
        }
        return parseTo_Boolean(s);
    }

    public static boolean parseTo_boolean(boolean b) {
        return b;
    }

    public static boolean parseTo_boolean(Boolean b) {
        if (b == null) {
            return defaultValueBoolean;
        } else {
            return b;
        }
    }

    public static Object parseTo_Object(Object input) {
        return input;
    }

    public static String parseTo_String(java.nio.ByteBuffer input) {
        if (input == null) {
            return null;
        } else {
            return new String(input.array());
        }
    }

    public static String parseTo_String(Object input) {
        if (input == null) {
            return null;
        } else {
            return input.toString();
        }
    }

    public static String parseTo_String(java.util.Date input, String pattern) {
        return FormatterUtils.format_DateInUTC(input, pattern);
    }

    public static String parseTo_String(byte input) {
        return String.valueOf(input);
    }

    public static String parseTo_String(char input) {
        return String.valueOf(input);
    }

    public static String parseTo_String(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return String.valueOf(input);
    }

    public static String parseTo_String(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return String.valueOf(input);
    }

    public static String parseTo_String(int input) {
        return String.valueOf(input);
    }

    public static String parseTo_String(long input) {
        return String.valueOf(input);
    }

    public static String parseTo_String(short input) {
        return String.valueOf(input);
    }

    public static BigDecimal parseTo_BigDecimal(String s) {
        if (isBlank(s)) {
            return null;
        }
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException nfe) {
            if (nfe.getMessage() == null) {
                throw new NumberFormatException("Incorrect input \"" + s + "\" for BigDecimal.");
            } else {
                throw nfe;
            }
        }
    }

    public static routines.system.Document parseTo_Document(String s, boolean ignoreDTD, String encoding)
            throws org.dom4j.DocumentException {
        if (isBlank(s)) {
            return null;
        }
        routines.system.Document theDoc = new routines.system.Document();
        org.dom4j.io.SAXReader reader = new org.dom4j.io.SAXReader();

        if (ignoreDTD) {
            reader.setEntityResolver(new EntityResolver() {

                @Override
                public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
                    return new org.xml.sax.InputSource(new java.io.ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>"
                            .getBytes()));
                }
            });
        }

        org.dom4j.Document document = reader.read(new java.io.StringReader(s));
        if (encoding != null && !("".equals(encoding))) {
            document.setXMLEncoding(encoding);
        }
        theDoc.setDocument(document);
        return theDoc;
    }

    public synchronized static java.util.Date parseTo_Date(String s, String pattern) {
        if (isBlank(s)) {
            return null;
        }
        String s2 = s.trim();
        String pattern2 = pattern;
        if (isBlank(pattern2)) {
            pattern2 = Constant.dateDefaultPattern;
        }
        java.util.Date date = null;
        if (pattern2.equals("yyyy-MM-dd'T'HH:mm:ss'000Z'")) {
            if (!s2.endsWith("000Z")) {
                throw new RuntimeException("Unparseable date: \"" + s2 + "\""); //$NON-NLS-1$ //$NON-NLS-2$
            }
            pattern2 = "yyyy-MM-dd'T'HH:mm:ss";
            s2 = s.substring(0, s.lastIndexOf("000Z"));
        }
        DateFormat format = FastDateParser.getInstance(pattern2);
        ParsePosition pp = new ParsePosition(0);
        pp.setIndex(0);

        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        date = format.parse(s2, pp);
        if (pp.getIndex() != s2.length() || date == null) {
            throw new RuntimeException("Unparseable date: \"" + s2 + "\""); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return date;
    }

    public synchronized static java.util.Date parseTo_Date(String s, String pattern, boolean lenient) {
        if (isBlank(s)) {
            return null;
        }
        String s2 = s.trim();
        String pattern2 = pattern;
        if (isBlank(pattern2)) {
            pattern2 = Constant.dateDefaultPattern;
        }
        java.util.Date date = null;
        if (pattern2.equals("yyyy-MM-dd'T'HH:mm:ss'000Z'")) {
            if (!s2.endsWith("000Z")) {
                throw new RuntimeException("Unparseable date: \"" + s2 + "\""); //$NON-NLS-1$ //$NON-NLS-2$
            }
            pattern2 = "yyyy-MM-dd'T'HH:mm:ss";
            s2 = s2.substring(0, s.lastIndexOf("000Z"));
        }
        DateFormat format = FastDateParser.getInstance(pattern2, lenient);
        ParsePosition pp = new ParsePosition(0);
        pp.setIndex(0);

        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        date = format.parse(s2, pp);
        if (pp.getIndex() != s2.length() || date == null) {
            throw new RuntimeException("Unparseable date: \"" + s2 + "\""); //$NON-NLS-1$ //$NON-NLS-2$
        }

        return date;
    }

    /**
     * in order to transform the string "1.234.567,89" to number 1234567.89
     */
    public static String parseTo_Number(String s, Character thousandsSeparator, Character decimalSeparator) {
        if (isBlank(s)) {
            return null;
        }
        String result = s;
        if (thousandsSeparator != null) {
            result = routines.system.StringUtils.deleteChar(s, thousandsSeparator);
        }
        if (decimalSeparator != null) {
            result = result.replace(decimalSeparator, '.');
        }
        return result;
    }

    /**
     * Parse anything to Integer
     *
     * @param input
     * @return
     */
    public static Integer parseTo_Integer(Number input) {
        if (input == null) {
            return null;
        }
        return input.intValue();
    }

    public static Integer parseTo_Integer(byte input) {
        return ((Byte) input).intValue();
    }

    public static Integer parseTo_Integer(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return ((Double) input).intValue();
    }

    public static Integer parseTo_Integer(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).intValue();
    }

    public static Integer parseTo_Integer(int input) {
        return input;
    }

    public static Integer parseTo_Integer(long input) {
        return ((Long) input).intValue();
    }

    public static Integer parseTo_Integer(short input) {
        return ((Short) input).intValue();
    }

    /**
     * Parse anything to int
     *
     * @param input
     * @return
     */
    public static int parseTo_int(Number input) {
        if (input == null) {
            return defaultValueInt;
        }
        return input.intValue();
    }

    public static int parseTo_int(byte input) {
        return ((Byte) input).intValue();
    }

    public static int parseTo_int(double input) {
        if (Double.isNaN(input)) {
            return defaultValueInt;
        }
        return ((Double) input).intValue();
    }

    public static int parseTo_int(float input) {
        if (Float.isNaN(input)) {
            return defaultValueInt;
        }
        return ((Float) input).intValue();
    }

    public static int parseTo_int(int input) {
        return input;
    }

    public static int parseTo_int(long input) {
        return ((Long) input).intValue();
    }

    public static int parseTo_int(short input) {
        return ((Short) input).intValue();
    }

    /**
     * Parse anything to Byte
     *
     * @param input
     * @return
     */
    public static Byte parseTo_Byte(Number input) {
        if (input == null) {
            return null;
        }
        return input.byteValue();
    }

    public static Byte parseTo_Byte(byte input) {
        return input;
    }

    public static Byte parseTo_Byte(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return ((Double) input).byteValue();
    }

    public static Byte parseTo_Byte(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).byteValue();
    }

    public static Byte parseTo_Byte(int input) {
        return ((Integer) input).byteValue();
    }

    public static Byte parseTo_Byte(long input) {
        return ((Long) input).byteValue();
    }

    public static Byte parseTo_Byte(short input) {
        return ((Short) input).byteValue();
    }

    public static Byte parseTo_Byte(boolean input) {
        return (byte) (input ? 1 : 0);
    }

    public static Byte parseTo_Byte(Boolean input) {
        if (input == null) {
            return null;
        }
        return (byte) (input ? 1 : 0);
    }

    /**
     * Parse anything to byte
     *
     * @param input
     * @return
     */
    public static byte parseTo_byte(Number input) {
        if (input == null) {
            return defaultValueByte;
        }
        return input.byteValue();
    }

    public static byte parseTo_byte(byte input) {
        return input;
    }

    public static byte parseTo_byte(double input) {
        if (Double.isNaN(input)) {
            return defaultValueByte;
        }
        return ((Double) input).byteValue();
    }

    public static byte parseTo_byte(float input) {
        if (Float.isNaN(input)) {
            return defaultValueByte;
        }
        return ((Float) input).byteValue();
    }

    public static byte parseTo_byte(int input) {
        return ((Integer) input).byteValue();
    }

    public static byte parseTo_byte(long input) {
        return ((Long) input).byteValue();
    }

    public static byte parseTo_byte(short input) {
        return ((Short) input).byteValue();
    }

    public static byte parseTo_byte(boolean input) {
        return (byte) (input ? 1 : 0);
    }

    public static byte parseTo_byte(Boolean input) {
        if (input == null) {
            return defaultValueByte;
        }
        return (byte) (input ? 1 : 0);
    }

    /**
     * Parse anything to double
     *
     * @param input
     * @return
     */
    public static Double parseTo_Double(Number input) {
        if (input == null) {
            return null;
        }
        return input.doubleValue();
    }

    public static Double parseTo_Double(byte input) {
        return ((Byte) input).doubleValue();
    }

    public static Double parseTo_Double(double input) {
        return ((Double) input).doubleValue();
    }

    public static Double parseTo_Double(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).doubleValue();
    }

    public static Double parseTo_Double(int input) {
        return ((Integer) input).doubleValue();
    }

    public static Double parseTo_Double(long input) {
        return ((Long) input).doubleValue();
    }

    public static Double parseTo_Double(short input) {
        return ((Short) input).doubleValue();
    }

    /**
     * Parse anything to double
     *
     * @param input
     * @return
     */
    public static double parseTo_double(Number input) {
        if (input == null) {
            return defaultValueDouble;
        }
        return input.doubleValue();
    }

    public static double parseTo_double(byte input) {
        return ((Byte) input).doubleValue();
    }

    public static double parseTo_double(double input) {
        return input;
    }

    public static double parseTo_double(float input) {
        if (Float.isNaN(input)) {
            return defaultValueDouble;
        }
        return ((Float) input).doubleValue();
    }

    public static double parseTo_double(int input) {
        return ((Integer) input).doubleValue();
    }

    public static double parseTo_double(long input) {
        return ((Long) input).doubleValue();
    }

    public static double parseTo_double(short input) {
        return ((Short) input).doubleValue();
    }

    /**
     * Parse anything to Float
     *
     * @param input
     * @return
     */
    public static Float parseTo_Float(Number input) {
        if (input == null) {
            return null;
        }
        return input.floatValue();
    }

    public static Float parseTo_Float(byte input) {
        return ((Byte) input).floatValue();
    }

    public static Float parseTo_Float(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return ((Double) input).floatValue();
    }

    public static Float parseTo_Float(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).floatValue();
    }

    public static Float parseTo_Float(int input) {
        return ((Integer) input).floatValue();
    }

    public static Float parseTo_Float(long input) {
        return ((Long) input).floatValue();
    }

    public static Float parseTo_Float(short input) {
        return ((Short) input).floatValue();
    }

    /**
     * Parse anything to float
     *
     * @param input
     * @return
     */
    public static float parseTo_float(Number input) {
        if (input == null) {
            return defaultValueFloat;
        }
        return input.floatValue();
    }

    public static float parseTo_float(byte input) {
        return ((Byte) input).floatValue();
    }

    public static float parseTo_float(double input) {
        if (Double.isNaN(input)) {
            return defaultValueFloat;
        }
        return ((Double) input).floatValue();
    }

    public static float parseTo_float(float input) {
        return input;
    }

    public static float parseTo_float(int input) {
        return ((Integer) input).floatValue();
    }

    public static float parseTo_float(long input) {
        return ((Long) input).floatValue();
    }

    public static float parseTo_float(short input) {
        return ((Short) input).floatValue();
    }

    /**
     * Parse anything to BigDecimal
     *
     * @param input
     * @return
     */
    public static BigDecimal parseTo_BigDecimal(Number input) {
        if (input == null) {
            return null;
        }
        return new BigDecimal(input.toString());
    }

    public static BigDecimal parseTo_BigDecimal(byte input) {
        return new BigDecimal(input);
    }

    public static BigDecimal parseTo_BigDecimal(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return BigDecimal.valueOf(input);
    }

    public static BigDecimal parseTo_BigDecimal(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return BigDecimal.valueOf(input);
    }

    public static BigDecimal parseTo_BigDecimal(int input) {
        return new BigDecimal(input);
    }

    public static BigDecimal parseTo_BigDecimal(long input) {
        return new BigDecimal(input);
    }

    public static BigDecimal parseTo_BigDecimal(short input) {
        return new BigDecimal(input);
    }

    /**
     * Parse anything to Long
     *
     * @param input
     * @return
     */
    public static Long parseTo_Long(Number input) {
        if (input == null) {
            return null;
        }
        return input.longValue();
    }

    public static Long parseTo_Long(byte input) {
        return ((Byte) input).longValue();
    }

    public static Long parseTo_Long(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return ((Double) input).longValue();
    }

    public static Long parseTo_Long(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).longValue();
    }

    public static Long parseTo_Long(int input) {
        return ((Integer) input).longValue();
    }

    public static Long parseTo_Long(long input) {
        return input;
    }

    public static Long parseTo_Long(short input) {
        return ((Short) input).longValue();
    }

    /**
     * Parse anything to long
     *
     * @param input
     * @return
     */
    public static long parseTo_long(Number input) {
        if (input == null) {
            return defaultValueLong;
        }
        return input.longValue();
    }

    public static long parseTo_long(byte input) {
        return ((Byte) input).longValue();
    }

    public static long parseTo_long(double input) {
        if (Double.isNaN(input)) {
            return defaultValueLong;
        }
        return ((Double) input).longValue();
    }

    public static long parseTo_long(float input) {
        if (Float.isNaN(input)) {
            return defaultValueLong;
        }
        return ((Float) input).longValue();
    }

    public static long parseTo_long(int input) {
        return ((Integer) input).longValue();
    }

    public static long parseTo_long(long input) {
        return input;
    }

    public static long parseTo_long(short input) {
        return ((Short) input).longValue();
    }

    /**
     * Parse anything to Short
     *
     * @param input
     * @return
     */
    public static Short parseTo_Short(Number input) {
        if (input == null) {
            return null;
        }
        return input.shortValue();
    }

    public static Short parseTo_Short(byte input) {
        return ((Byte) input).shortValue();
    }

    public static Short parseTo_Short(double input) {
        if (Double.isNaN(input)) {
            return null;
        }
        return ((Double) input).shortValue();
    }

    public static Short parseTo_Short(float input) {
        if (Float.isNaN(input)) {
            return null;
        }
        return ((Float) input).shortValue();
    }

    public static Short parseTo_Short(int input) {
        return ((Integer) input).shortValue();
    }

    public static Short parseTo_Short(long input) {
        return ((Long) input).shortValue();
    }

    public static Short parseTo_Short(short input) {
        return ((Short) input).shortValue();
    }

    /**
     * Parse anything to short
     *
     * @param input
     * @return
     */
    public static short parseTo_short(Number input) {
        if (input == null) {
            return defaultValueShort;
        }
        return input.shortValue();
    }

    public static short parseTo_short(byte input) {
        return ((Byte) input).shortValue();
    }

    public static short parseTo_short(double input) {
        if (Double.isNaN(input)) {
            return defaultValueShort;
        }
        return ((Double) input).shortValue();
    }

    public static short parseTo_short(float input) {
        if (Float.isNaN(input)) {
            return defaultValueShort;
        }
        return ((Float) input).shortValue();
    }

    public static short parseTo_short(int input) {
        return ((Integer) input).shortValue();
    }

    public static short parseTo_short(long input) {
        return ((Long) input).shortValue();
    }

    public static short parseTo_short(short input) {
        return input;
    }

    public static java.util.Date parseTo_Date(java.util.Date input) {
        return input;
    }

    public static char parseTo_char(String s) {
        if (isBlank(s)) {
            return defaultValueChar;
        }
        return s.charAt(0);
    }

    public static Character parseTo_Character(Character input) {
        return input;
    }

    public static Character parseTo_Character(char input) {
        return input;
    }

    public static char parseTo_char(char input) {
        return input;
    }

    public static char parseTo_char(Character input) {
        if (input == null) {
            return defaultValueChar;
        }
        return input;
    }

}
