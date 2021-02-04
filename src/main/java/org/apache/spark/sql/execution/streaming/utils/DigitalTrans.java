package org.apache.spark.sql.execution.streaming.utils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.UnknownHostException;

public class DigitalTrans {

    /**
     * 数字字符串转ASCII码字符串
     * 
     * @param content
     *            字符串
     * @return ASCII字符串
     */
    public static String StringToAsciiString(String content) {
        String result = "";
        int max = content.length();
        for (int i = 0; i < max; i++) {
            char c = content.charAt(i);
            String b = Integer.toHexString(c);
            result = result + b;
        }
        return result;
    }
    /**
     * 字符串转成16进制 
     * @param str
     * @return  E6 88 91 E7 88 B1 E4 BD A0 E4 B8 AD E5
     */
    public static String  printHexStringbak(String str) {  
    	StringBuffer sbReturn=new StringBuffer();
    	byte[] b=str.getBytes();
        for (int i = 0; i < b.length; i++) {   
            String hex = Integer.toHexString(b[i] & 0xFF);   
            if (hex.length() == 1) {   
                hex = '0' + hex;   
            }   
            sbReturn.append(hex.toUpperCase());
            if(i< b.length-1){
            	 sbReturn.append(" ");
            }
        }
		return sbReturn.toString();   
    }
    /**
     * 汉字字符串转成16进制 
     * @param s
     * @return  E6 88 91 E7 88 B1 E4 BD A0 E4 B8 AD E5
     */
    public static String toHexString(String s) {  
 	   StringBuffer str = new StringBuffer();  
 	   for (int i = 0; i < s.length(); i++) {  
	    	    int ch = (int) s.charAt(i);  
	    	    String s4 = Integer.toHexString(ch);
	    	    if(s4.length()>2){
	    	    	str.append(s4.substring(0,2).toUpperCase());
		    	    str.append(" ");
		    	    str.append(s4.substring(2).toUpperCase());
	    	    }else{
	    	    	 str.append("00 ");
			    	 str.append(s4.toUpperCase());
	    	    }
	    	    if(i < s.length()-1){
	    	    	str.append(" ");
	    	    }
 	   }  
 	   return str.toString();  
	}  
    /**
     * 十六进制转字符串
     * 
     * @param hexString
     *            十六进制字符串
     * @param encodeType
     *            编码类型4：Unicode，2：普通编码
     * @return 字符串
     */
    public static String hexStringToString(String hexString, int encodeType) {
        String result = "";
        int max = hexString.length() / encodeType;
        for (int i = 0; i < max; i++) {
            char c = (char) DigitalTrans.hexStringToAlgorism(hexString
                    .substring(i * encodeType, (i + 1) * encodeType));
            result += c;
        }
        return result;
    }
    
    /**
     * 十六进制字符串装十进制
     * 
     * @param hex
     *            十六进制字符串
     * @return 十进制数值
     */
    public static int hexStringToAlgorism(String hex) {
        hex = hex.toUpperCase();
        int max = hex.length();
        int result = 0;
        for (int i = max; i > 0; i--) {
            char c = hex.charAt(i - 1);
            int algorism = 0;
            if (c >= '0' && c <= '9') {
                algorism = c - '0';
            } else {
                algorism = c - 55;
            }
            result += Math.pow(16, max - i) * algorism;
        }
        return result;
    }
    /**
     * 十六转二进制
     * 
     * @param hex
     *            十六进制字符串
     * @return 二进制字符串
     */
    public static String hexStringToBinary(String hex) {
        hex = hex.toUpperCase();
        StringBuilder result = new StringBuilder();
        int max = hex.length();
        for (int i = 0; i < max; i++) {
            char c = hex.charAt(i);
            switch (c) {
            case '0':
                result.append("0000");
                break;
            case '1':
                result.append("0001");
                break;
            case '2':
                result.append("0010");
                break;
            case '3':
                result.append("0011");
                break;
            case '4':
                result.append("0100");
                break;
            case '5':
                result.append("0101");
                break;
            case '6':
                result.append("0110");
                break;
            case '7':
                result.append("0111");
                break;
            case '8':
                result.append("1000");
                break;
            case '9':
                result.append("1001");
                break;
            case 'A':
                result.append("1010");
                break;
            case 'B':
                result.append("1011");
                break;
            case 'C':
                result.append("1100");
                break;
            case 'D':
                result.append("1101");
                break;
            case 'E':
                result.append("1110");
                break;
            case 'F':
                result.append("1111");
                break;
            }
        }
        return result.toString();
    }
    /**
     * ASCII码字符串转数字字符串
     * 
     * @param  content
     *            ASCII字符串
     * @return 字符串
     */
    public static String AsciiStringToString(String content) {
        String result = "";
        int length = content.length() / 2;
        for (int i = 0; i < length; i++) {
            String c = content.substring(i * 2, i * 2 + 2);
            int a = hexStringToAlgorism(c);
            char b = (char) a;
            String d = String.valueOf(b);
            result += d;
        }
        return result;
    }
    /**
     * 将十进制转换为指定长度的十六进制字符串
     * 
     * @param algorism
     *            int 十进制数字
     * @param maxLength
     *            int 转换后的十六进制字符串长度
     * @return String 转换后的十六进制字符串
     */
    public static String algorismToHEXString(int algorism, int maxLength) {
        String result = "";
        result = Integer.toHexString(algorism);

        if (result.length() % 2 == 1) {
            result = "0" + result;
        }
        return patchHexString(result.toUpperCase(), maxLength);
        //return result;
    }
    /**
     * 字节数组转为普通字符串（ASCII对应的字符）
     * 
     * @param bytearray
     *            byte[]
     * @return String
     */
    public static String bytetoString(byte[] bytearray) {
        String result = "";
        char temp;

        int length = bytearray.length;
        for (int i = 0; i < length; i++) {
            temp = (char) bytearray[i];
            result += temp;
        }
        return result;
    }
    /**
     * 二进制字符串转十进制
     * 
     * @param binary
     *            二进制字符串
     * @return 十进制数值
     */
    public static int binaryToAlgorism(String binary) {
        int max = binary.length();
        int result = 0;
        for (int i = max; i > 0; i--) {
            char c = binary.charAt(i - 1);
            int algorism = c - '0';
            result += Math.pow(2, max - i) * algorism;
        }
        return result;
    }

    /**
     * 十进制转换为十六进制字符串
     * 
     * @param algorism
     *            int 十进制的数字
     * @return String 对应的十六进制字符串
     */
    public static String algorismToHEXString(int algorism) {
        String result = "";
        result = Integer.toHexString(algorism);

        if (result.length() % 2 == 1) {
            result = "0" + result;

        }
        result = result.toUpperCase();

        return result;
    }
    public static String algorismToHEXString1(int algorism) {
        String result = "";
       // result = Integer.toHexString(algorism);
        if (result.length() % 2 == 1) {
            result = "0" + result;

        }
        result = result.toUpperCase();

        return result;
    }
    /**
     * HEX字符串前补0，主要用于长度位数不足。
     * 
     * @param str
     *            String 需要补充长度的十六进制字符串
     * @param maxLength
     *            int 补充后十六进制字符串的长度
     * @return 补充结果
     */
    static public String patchHexString(String str, int maxLength) {
        String temp = "";
        for (int i = 0; i < maxLength - str.length(); i++) {
            temp = "0" + temp;
        }
        str = (temp + str).substring(0, maxLength);
        return str;
    }
    /**
     * 将一个字符串转换为int
     * 
     * @param s
     *            String 要转换的字符串
     * @param defaultInt
     *            int 如果出现异常,默认返回的数字
     * @param radix
     *            int 要转换的字符串是什么进制的,如16 8 10.
     * @return int 转换后的数字
     */
    public static int parseToInt(String s, int defaultInt, int radix) {
        int i = 0;
        try {
            i = Integer.parseInt(s, radix);
        } catch (NumberFormatException ex) {
            i = defaultInt;
        }
        return i;
    }
    /**
     * 将一个十进制形式的数字字符串转换为int
     * 
     * @param s
     *            String 要转换的字符串
     * @param defaultInt
     *            int 如果出现异常,默认返回的数字
     * @return int 转换后的数字
     */
    public static int parseToInt(String s, int defaultInt) {
        int i = 0;
        try {
            i = Integer.parseInt(s);
        } catch (NumberFormatException ex) {
            i = defaultInt;
        }
        return i;
    }
    /**
     * 十六进制字符串转为Byte数组,每两个十六进制字符转为一个Byte
     * 
     * @param hex
     *            十六进制字符串
     * @return byte 转换结果
     */
    public static byte[] hexStringToByte(String hex) {
        int max = hex.length() / 2;
        byte[] bytes = new byte[max];
        String binarys = DigitalTrans.hexStringToBinary(hex);
        for (int i = 0; i < max; i++) {
            bytes[i] = (byte) DigitalTrans.binaryToAlgorism(binarys.substring(
                    i * 8 + 1, (i + 1) * 8));
            if (binarys.charAt(8 * i) == '1') {
                bytes[i] = (byte) (0 - bytes[i]);
            }
        }
        return bytes;
    }
    /**
     * 十六进制串转化为byte数组
     * 
     * @return the array of byte
     */
    public static final byte[] hex2byte(String hex)
            throws IllegalArgumentException {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException();
        }
        char[] arr = hex.toCharArray();
        byte[] b = new byte[hex.length() / 2];
        for (int i = 0, j = 0, l = hex.length(); i < l; i++, j++) {
            String swap = "" + arr[i++] + arr[i];
            int byteint = Integer.parseInt(swap, 16) & 0xFF;
            b[j] = new Integer(byteint).byteValue();

        }
        /*for(int i=0;i<b.length;i++){
            System.out.println(b[i]);
        }*/
        return b;
    }
    /**
     * 字节数组转换为十六进制字符串
     * 
     * @param b
     *            byte[] 需要转换的字节数组
     * @return String 十六进制字符串
     */
    public static final String byte2hex(byte b[]) {
        if (b == null) {
            throw new IllegalArgumentException(
                    "Argument b ( byte array ) is null! ");
        }
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = Integer.toHexString(b[n] & 0xff);
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }
        return hs.toUpperCase();
    }
    /**
     * 报文校验和计算
     * 根据传入的16进制字符串进行拆分相加，返回16进制(低八位)总和
     * @param str 例如：09 05 00 00 A4 3A 00
     * @return String 返回：EC
     */ 
    public static String getMyHexString(String str){
		String returnStr=null;
		if(str!=null && str.length()>0){
			String [] getKeyList=str.split(" ");
			int getNVal=0;
			for(int i=0;i<getKeyList.length;i++){
				if(!"".equals(getKeyList[i])){
					getNVal+=Integer.valueOf(getKeyList[i],16);
				}
			}
			returnStr=Integer.toHexString(getNVal).toUpperCase();
			if(returnStr.length()>2){
				returnStr=hexStringToBinary(returnStr);
				returnStr=returnStr.substring(returnStr.length()-8);
				int nten=binaryToAlgorism(returnStr);
				returnStr=algorismToHEXString(nten);
			}
			if(returnStr.length()==1){
				returnStr="0"+returnStr;
			}
		}
		return returnStr;
	}
    
    public static String stringNumToHEXString(String stringNum){
    	StringBuffer sb=new StringBuffer();
    	if(stringNum!=null){
    		String []numList=stringNum.split(" ");
    		for(int i=0;i<numList.length;i++){
    			sb.append(DigitalTrans.algorismToHEXString(Integer.valueOf(numList[i])));
    			if(i<numList.length-1){
    				sb.append(" ");
    			}
    		}
    	}
    	return sb.toString();
    }

    /**
     * 获得socket 数据
     *
     */
    public static String getSocketData(InputStreamReader input1, BufferedOutputStream out1, String sendCode) throws UnknownHostException, IOException {
        /*if(skt ==null){
            skt = new Socket();
            skt.connect(new InetSocketAddress(ip, port), 400);
        }*/

        //"10.136.216.101",5002

        byte[] b = DigitalTrans.hex2byte(sendCode);//3E2A07D100000004  3E2A07D1
        /*for(int i=0;i<b.length;i++){
            System.out.println(b[i]);
        }*/
        out1.write(b);
        out1.flush();

        String result="";
        String getNowCode ="";
        String count = "";
        int stop=5002;
        for(int i=0;i<5002;i++){
            int getCode = input1.read();
            getNowCode=algorismToHEXString(getCode);
            if(i==4){
                count+=getNowCode;
            }
            if(i==5){
                count+=getNowCode;
                stop =hexStringToAlgorism(count)+5;
            }
            result+=getNowCode+" ";
            if(i==stop){
                break;
            }
        }
        return result;
    }
}