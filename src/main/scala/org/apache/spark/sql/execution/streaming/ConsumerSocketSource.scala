package org.apache.spark.sql.execution.streaming

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{BufferedOutputStream, IOException, InputStreamReader}
import java.net.Socket
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Locale}

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.utils.{DigitalTrans, Hex2Float}
import org.apache.spark.sql.functions.{col, length, size}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

object ConsumerSocketSource {
  val SCHEMA_REGULAR: StructType = StructType(StructField("tagvalue", StringType) :: StructField("tagName", StringType) :: Nil)
  val SCHEMA_TIMESTAMP: StructType = StructType(StructField("tagvalue", StringType) :: StructField("tagName", StringType) ::
    StructField("timestamp", StringType) :: Nil)
  val DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
}
/**
 * A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
 * This source will *not* work in production applications due to multiple reasons, including no
 * support for fault recovery and keeping all of the text read in memory forever.
 */
class ConsumerSocketSource(host: String, port: Int,sendCode:String,dataType:Boolean,path:String, includeTimestamp: Boolean, sqlContext: SQLContext)
  extends Source with Logging {

  @GuardedBy("this")
  private var socket: Socket = null

  @GuardedBy("this")
  private var readThread: Thread = null

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[(String,String,String)]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var lastOffsetCommitted: LongOffset = new LongOffset(-1)

  initialize()

  private def initialize(): Unit = synchronized {
    socket = new Socket(host, port)
    val out: BufferedOutputStream = new BufferedOutputStream(socket.getOutputStream)
    val reader: InputStreamReader = new InputStreamReader(socket.getInputStream, "ISO-8859-1")
    import sqlContext.sparkSession.implicits._
    val dataschmea=sqlContext.sparkSession.read.textFile(path).select(functions.decode(col("value"), "gbk").as("value")).as[String].collect()
    readThread = new Thread(s"org.apache.spark.sql.execution.streaming($host, $port,$sendCode)") {
      setDaemon(true)

      override def run(): Unit = {
        try while (true) {
          //将16进制请求转为字节数组进行发送
         val hexdata: String =DigitalTrans.getSocketData(reader,out,sendCode)
          //logWarning(s"get line1 $hexdata")
          //将返回的16进制字节数组转为字符串
          val line: String = byteDataToStr(hexdata,dataType)
         // logWarning(s"get line2 $line")
          if (line == null) {
            // End of file reached
            logWarning(s"Stream closed by $host:$port")
            return
          }
          //保证同一时刻只有一个scoket 数据可以写入dataframe
          ConsumerSocketSource.this.synchronized {
            val datatime=
              ConsumerSocketSource.DATE_FORMAT.format(Calendar.getInstance().getTime)
            val i1: Int = dataschmea.size - 1
            for(i<- 0 to  i1){
             currentOffset = currentOffset + 1
              val tuple: (String, String, String) = (line.split(",")(i), dataschmea(i), datatime)

             batches.append(tuple)
              //logWarning(s"new DATA= $tuple")
            }

          }
        } catch {
          case e: IOException =>
        }
      }
    }
    readThread.start()
  }

  /** Returns the schema of the data from this source */
  override def schema: StructType = if (includeTimestamp) ConsumerSocketSource.SCHEMA_TIMESTAMP
  else ConsumerSocketSource.SCHEMA_REGULAR

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal: Int =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    val rdd: RDD[InternalRow] = sqlContext.sparkContext
      .parallelize(rawList)
      .map { case (v1,v2, ts) => InternalRow(UTF8String.fromString(v1),UTF8String.fromString(v2),UTF8String.fromString(ts)) }
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = LongOffset.convert(end).getOrElse(
      sys.error(s"TextSocketStream.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    batches.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    if (socket != null) {
      try {
        // Unfortunately, BufferedReader.readLine() cannot be interrupted, so the only way to
        // stop the readThread is to close the socket.
        socket.close()
      } catch {
        case e: IOException =>
      }
      socket = null
    }
  }

  override def toString: String = s"ConsumerSocketSource[host: $host, port: $port]"

  /**
   *
   * @param data 16进制数据串 以空格进行分割
   * @param flag  判断获取开关变量还是模拟变量
   * @return str 如果是模拟变量即为float 类型 开关变量 返回0或者1
   */
  def byteDataToStr(data:String,flag:Boolean):String= {
    //val line=new StringBuffer()
    val bytedata: Array[String] =data.split(" ")
    val i1: Int = bytedata.size - 1
    val result=if(flag){
      for{i <- Range(6, i1, 4) }yield Hex2Float.hex2Float(bytedata(i) + bytedata(i + 1) + bytedata(i + 2) + bytedata(i + 3))
    }else{
      for{i<-6 to i1 if !flag}yield DigitalTrans.hexStringToBinary(bytedata(i)).reverse.toCharArray.mkString(",")
    }

    result.mkString(",")
  }


}
