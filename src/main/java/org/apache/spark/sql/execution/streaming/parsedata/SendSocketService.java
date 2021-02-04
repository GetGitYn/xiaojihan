package org.apache.spark.sql.execution.streaming.parsedata;

import org.apache.spark.sql.execution.streaming.utils.Hex2Float;
import org.apache.spark.sql.execution.streaming.utils.DigitalTrans;
import org.apache.commons.codec.DecoderException;
import scala.annotation.meta.param;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

public class SendSocketService {
    private  InputStreamReader input;
    private  BufferedOutputStream out;
    private Socket skt;

    public void Connect(String ip,int port) throws UnknownHostException, IOException {

        if (skt != null && skt.isConnected()) {
            skt.close();
            skt = null;
        }
        while (true) {
            try {
                skt = new Socket();
                skt.connect(new InetSocketAddress(ip, port), 1000);
                if (skt.isConnected()) {
                      input = new InputStreamReader(skt.getInputStream(), "ISO-8859-1");
                       out = new BufferedOutputStream(skt.getOutputStream());
                    break;
                }
            } catch (SocketTimeoutException e) {
                skt = null;
                Thread.yield();
            }
        }
    }
    public static String getSocketData(InputStreamReader input1,BufferedOutputStream out1,String sendCode) throws UnknownHostException, IOException {
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
        int stop=100;
        for(int i=0;i<100;i++){
            int getCode = input1.read();
            getNowCode=algorismToHEXString(getCode);
            if(i==4){
                count+=getNowCode;
            }
            if(i==5){
                count+=getNowCode;
                stop = DigitalTrans.hexStringToAlgorism(count)+5;
                System.out.println(stop);
            }
            result+=getNowCode+" ";
            if(i==stop){
                break;
            }
        }

       // out.close();
        //input.close();
        //skt.close();
        return result;
    }

    public  String getSocketData1(String sendCode) throws UnknownHostException, IOException {
       /* if(skt ==null){
            skt = new Socket();
            skt.connect(new InetSocketAddress(ip, port), 400);
        }*/

        //"10.136.216.101",5002

        byte[] b = DigitalTrans.hex2byte(sendCode);//3E2A07D100000004  3E2A07D1
        out.write(b);
        out.flush();

        String result="";
        String getNowCode ="";
        String count = "";
        int stop=4096;
        for(int i=0;i<4096;i++){
            int getCode = input.read();
            getNowCode=DigitalTrans.algorismToHEXString(getCode);
           // System.out.println(getNowCode);
            if(i==4){
                count+=getNowCode;
            }
            if(i==5){
                count+=getNowCode;
                stop = DigitalTrans.hexStringToAlgorism(count)+5;
                System.out.println(stop);
            }
            result+=getNowCode+" ";
            if(i==stop){
                break;
            }
        }

        // out.close();
        //input.close();
        //skt.close();
        return result;
    }

    /**
     * 十进制字符串转十六进制字符串
     * @param algorism
     * @return
     */
    static String algorismToHEXString(int algorism) {
        String result = "";
        result = Integer.toHexString(algorism);

        if (result.length() % 2 == 1) {
            result = "0" + result;

        }
        result = result.toUpperCase();
      //  System.out.println(result);
        return result;
    }
    /**
     * 获取模拟量,1个地址长度获取1个模拟量值(4个=>"1200.0|250.0|1000.0|302.0")
     * @param
     * @param
     * @return
*/
    public String getAnalogData(int startIndex,int length){

        String ip = "172.17.3.179";
        int port = 5002;
        String sendCode ="";//客户端的请求指令
        String start="";//起始地址转化为十六进制字符串
        String len=""; //长度转化为十六进制字符串
        String result="";//返回结果

        /*ip = rfu.getZzxUrl().get("scadaIP").toString();
        port = Integer.parseInt(rfu.getZzxUrl().get("scadaPort").toString());*/

        start = DigitalTrans.algorismToHEXString(startIndex,4);
        len = DigitalTrans.algorismToHEXString(length,4);
        //拼接成完整的 请求指令
        sendCode = "3E2A07D1"+start+len;

        try {
            String data = getSocketData1(sendCode);
            String[] str1 = data.split(" ");

            for(int i=6;i<length*4+3;i+=4){

                String cl = str1[i] + str1[i+1] + str1[i+2] + str1[i+3];
                try {
                    Float fl = Hex2Float.hex2Float(cl);
                    result += fl.toString() +"|";
                } catch (DecoderException e) {
                    e.printStackTrace();
                }
            }
            result = result.substring(0, result.length()-1);
            return result;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取开关量，1个地址长度获取相连8个开关量（"0|0|1|1|1|1|1|1"）
     * @param
     *
     * @param
     * @return
     */
   public String getSwitchData(String ip,int port,int startIndex,int length){

        String sendCode ="";//客户端的请求指令
        String start="";//起始地址转化为十六进制字符串
        String len=""; //长度转化为十六进制字符串
        String result="";//返回结果


       /* ip = rfu.getZzxUrl().get("scadaIP").toString();
        port = Integer.parseInt(rfu.getZzxUrl().get("scadaPort").toString());*/

        start = DigitalTrans.algorismToHEXString(startIndex,4);
        len = DigitalTrans.algorismToHEXString(length,4);
        //拼接成完整的 请求指令
        sendCode = "3E2A07D2"+start+len;

        try {
            String data = getSocketData1(sendCode);
            String[] str1 = data.split(" ");

            for(int i=6;i<length+6;i++){
                    //String str = str1[i];
                String bb = DigitalTrans.hexStringToBinary(str1[i]);
                char[] chars = bb.toCharArray();//1111 1100
                for(int m=0;m < 8;m++){
                    result += chars[7-m]+"|";
                }
            }
            result = result.substring(0, result.length()-1);
            return result;
        } catch (UnknownHostException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SendSocketService sendSocketService = new SendSocketService();
        sendSocketService.Connect("172.17.10.178", 5002);

        for (int i = 0; i < 5; i++) {
           // String socketData = sendSocketService.getSocketData1( "3E2A07D100030005");
            String analogData = sendSocketService.getAnalogData(0, 110);
            Thread.sleep(1000);
            System.out.println(analogData);
        }

    }
}
