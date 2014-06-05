package io.github.dmitrib.elasticsearch.cli

import com.fasterxml.jackson.databind.{SerializationFeature, MappingJsonFactory, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.elasticsearch.search.SearchHit
import java.io.StringWriter
import com.beust.jcommander.{ParameterException, JCommander, Parameter}
import java.util
import scala.collection.JavaConverters._
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

object EsTool {
  @Parameter(names = Array("--index"), description = "ElasticSearch index", required = true)
  var index: String = _
  @Parameter(names = Array("--type"), description = "ElasticSearch document type")
  var kind: String = _

  @Parameter(
    names = Array("--sniff"),
    description = "Request a list of other ElasticSearch cluster nodes from the endpoint nodes")
  var nodeSniff = false

  @Parameter(
    names = Array("--endpoint"),
    description = "ElasticSearch cluster endpoints; can be specified multiple times", required = true)
  var endpoints: util.List[String] = _

  @Parameter(
      names = Array("--request-timeout-mins"),
      description = "Request timeout minutes")
  var requestTimeoutMins = 10

  @Parameter(names = Array("--help"), help = true)
  var help: Boolean = _

  lazy val client = {
    val settings = ImmutableSettings.settingsBuilder
      .put("client.transport.ignore_cluster_name", true)
      .put("client.transport.sniff", nodeSniff)
      .build

    val client = endpoints.asScala.foldLeft(new TransportClient(settings)) { (cl, hostPort) =>
      hostPort.split(":") match {
        case Array(host, port) =>
          cl.addTransportAddress(new InetSocketTransportAddress(host, port.toInt))
        case Array(host) =>
          cl.addTransportAddress(new InetSocketTransportAddress(host, 9300))
      }
    }

    client
  }

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false)
  mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)

  val jsonFactory = new MappingJsonFactory(mapper)

  def hitToString(hit: SearchHit, srcOnly: Boolean = false, srcIdPair: Boolean = false) = {
    val fields = hit.getFields.asScala
    if (srcOnly) {
      fields.get("partial").fold(hit.getSourceAsString) { field =>
        mapper.writeValueAsString(field.getValue[AnyRef])
      }
    } else if (srcIdPair) {
      val id = hit.getId
      val source = fields.get("partial").fold(hit.getSourceAsString) { field =>
        mapper.writeValueAsString(field.getValue[AnyRef])
      }
      s"$id\t$source"
    } else {
      val writer = new StringWriter()
      val generator = jsonFactory.createGenerator(writer)

      generator.writeStartObject()
      generator.writeStringField("_id", hit.getId)
      val source = fields.get("partial").fold(hit.getSource) { field =>
        field.getValue[util.Map[String, AnyRef]]
      }
      if (source != null) {
        generator.writeObjectField("_source", source)
      }
      val remainingFields = fields - "partial"
      if (!remainingFields.isEmpty) {
        generator.writeObjectField("fields", remainingFields.map { e =>
          (e._1, e._2.getValue[AnyRef])
        })
      }
      generator.writeEndObject()

      generator.close()
      writer.toString
    }
  }

  def main(args: Array[String]) {
    try {
      val jc = new JCommander
      jc.addObject(EsTool)
      jc.addCommand("scan", ScanCommand)
      jc.addCommand("scan-join", ScanJoinCommand)
      jc.addCommand("batch-upload", BatchUploadCommand)
      jc.addCommand("search-by-input", SearchByInputCommand)

      jc.parse(args: _*)

      if (help) {
        jc.usage()
      } else {
        jc.getCommands.get(jc.getParsedCommand).getObjects.asScala.head.asInstanceOf[Runnable].run()
      }
    } catch {
      case e: ParameterException =>
        println(e.getMessage)
        println("use --help to see the available options")
        System.exit(1)
      case t: Throwable =>
        t.printStackTrace()
        System.exit(1)
    }
  }
}
