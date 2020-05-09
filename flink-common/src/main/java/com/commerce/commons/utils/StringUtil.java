package com.commerce.commons.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

/**
 * @description:
 * @author: zhangdongsheng
 * @date: 2020/5/8 15:29
 */
public class StringUtil {
    private static final String[] CHARS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a1", "a2"};

    public static String convert(String srcStr) {
        StringBuilder sb = new StringBuilder();
        int length = srcStr.length();

        for (int i = 0; i < length; i += 3) {
            int end = i + 3;
            if (end >= length) {
                end = length;
            }

            int t = Integer.parseInt(srcStr.substring(i, end), 16);
            sb.append(CHARS[t % 64]);
            sb.append(CHARS[t / 64]);
        }

        return sb.toString();
    }

    /**
     * 字符串转换成为16进制(无需Unicode编码)
     *
     * @param str
     * @return
     */
    public static String str2HexStr(String str) throws UnsupportedEncodingException {
        return String.format("%x", new BigInteger(1, str.getBytes("UTF-8")));
    }
}
