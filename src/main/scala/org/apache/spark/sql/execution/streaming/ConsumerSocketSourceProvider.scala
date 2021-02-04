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

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}


class ConsumerSocketSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {
  private def parseIncludeTimestamp(params: Map[String, String]): Boolean = {
    Try(params.getOrElse("includeTimestamp", "false").toBoolean) match {
      case Success(bool) => bool
      case Failure(_) =>
       throw new AnalysisException("includeTimestamp must be set to either true or false")
    }
  }

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    logWarning("The socket source should not be used for production applications! " +
      "It does not support recovery.")
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    if (!parameters.contains("sendCode")) {
      throw new AnalysisException("Set a sendCode to write the request from with option(\"sendCode\", ...).")
    }
    if (!parameters.contains("dataType")) {
      throw new AnalysisException("Set a dataType to read the type data from with option(\"dataType\", ...).")
    }
    if (schema.nonEmpty) {
      throw new AnalysisException("The socket source does not support a user-specified schema.")
    }

    val sourceSchema =
      if (parseIncludeTimestamp(parameters)) {
        ConsumerSocketSource.SCHEMA_TIMESTAMP
      } else {
        ConsumerSocketSource.SCHEMA_REGULAR
      }
    ("consumersocket", sourceSchema)
  }

  /**
   * 创建自定义数据源 对象
   * @param sqlContext
   * @param metadataPath
   * @param schema
   * @param providerName
   * @param parameters
   * @return
   */
  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    val sendcode=parameters("sendCode")
    val flag=parameters("dataType").toBoolean
    val path=parameters("proppath")
    new ConsumerSocketSource(host, port,sendcode,flag,path,parseIncludeTimestamp(parameters), sqlContext)
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "consumersocket"
}
