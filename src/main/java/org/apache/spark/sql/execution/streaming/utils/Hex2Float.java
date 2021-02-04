package org.apache.spark.sql.execution.streaming.utils;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class Hex2Float {
	
	 /**
     * 测试十六进制字符串转float.
     * 
     * @throws DecoderException
     */
    
    public final static Float hex2Float(String hexStr) throws DecoderException {
        //String hexStr = "0000c642";
        Float result = hexStr2Float(hexStr);
        return result;
    }
 
    /**
     * 十六进制字符串转float.
     * 
     * @param hexStr
     * @return
     * @throws DecoderException
     */
    private static Float hexStr2Float(String hexStr) throws DecoderException {
        Float result = null;
        // 先通过apahce的 hex类转换十六进制字符串为byte数组. 也可以自己用<a href="https://www.baidu.com/s?wd=JDK&tn=44039180_cpr&fenlei=mv6quAkxTZn0IZRqIHckPjm4nH00T1d9mvu9myF-nADkmHfYPhm40ZwV5Hcvrjm3rH6sPfKWUMw85HfYnjn4nH6sgvPsT6KdThsqpZwYTjCEQLGCpyw9Uz4Bmy-bIi4WUvYETgN-TLwGUv3EPH0krHfYnWDd" target="_blank" class="baidu-highlight">JDK</a>原声的方法来循环转
        // Character.digit(ch, 16);
        byte[] decodes = Hex.decodeHex(hexStr.toCharArray());
        // 获得byte转float的结果
        result = getFloat(decodes, 0);
        return result;
    }
 
    /**
     * 通过byte数组取得float
     * 
     * @param b
     * @param index
     *            第几位开始取.
     * @return
     */
    public static float getFloat(byte[] b, int index) {
        int l;
        l = b[index + 0];
        l &= 0xff;
        l |= ((long) b[index + 1] << 8);
        l &= 0xffff;
        l |= ((long) b[index + 2] << 16);
        l &= 0xffffff;
        l |= ((long) b[index + 3] << 24);
        return Float.intBitsToFloat(l);
    }
    
}
