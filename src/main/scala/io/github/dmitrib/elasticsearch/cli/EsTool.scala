package io.github.dmitrib.elasticsearch.cli

import com.fasterxml.jackson.databind.{SerializationFeature, MappingJsonFactory, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.elasticsearch.search.SearchHit
import java.io.StringWriter
import com.beust.jcommander.{JCommander, Parameter}
import java.util
import scala.collection.JavaConverters._
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

object EsTool {
  @Parameter(names = Array("--index"), description = "ElasticSearch index", required = true)
  var index: String = _
  @Parameter(names = Array("--type"), description = "ElasticSearch document type", required = true)
  var kind: String = _
  @Parameter(names = Array("--cluster-name"), description = "ElasticSearch cluster name", required = true)
  var clusterName: String = _

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
      .put("cluster.name", clusterName)
      .put("client.transport.sniff", nodeSniff)
      .build

    val client = endpoints.asScala.foldLeft(new TransportClient(settings)) { (cl, hostPort) =>
      hostPort.split(":") match {
        case Array(host, port) => {
          cl.addTransportAddress(new InetSocketTransportAddress(host, port.toInt))
        }
        case Array(host) => {
          cl.addTransportAddress(new InetSocketTransportAddress(host, 9300))
        }
      }
    }

    client
  }

  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false)
  mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false)

  val jsonFactory = new MappingJsonFactory(mapper)

  def hitToString(hit: SearchHit) = {
    val writer = new StringWriter()
    val generator = jsonFactory.createGenerator(writer)

    generator.writeStartObject()
    generator.writeStringField("_id", hit.getId)
    val fields = hit.getFields.asScala
    val source = fields.get("partial").map(_.getValue[AnyRef]).getOrElse(hit.getSource)
    generator.writeObjectField("_source", source)
    val remainingFields = fields - "partial"
    generator.writeObjectField("fields", remainingFields.map((e) => (e._1, e._2.getValue[AnyRef])))
    generator.writeEndObject()
    generator.close()
    writer.toString
  }

  def main(args: Array[String]) {
    try {
      val jc = new JCommander
      jc.addObject(EsTool)
      jc.addCommand("scan", ScanCommand)
      jc.addCommand("scan-join", ScanJoinCommand)
      jc.addCommand("multi-get", MultiGetCommand)
      jc.addCommand("batch-upload", BatchUploadCommand)
      jc.addCommand("search-by-input", SearchByInputCommand)

      jc.parse(args: _*)

      if (help) {
        jc.usage()
      } else {
        jc.getCommands.get(jc.getParsedCommand).getObjects.asScala.head.asInstanceOf[Runnable].run()
      }
    } catch {
      case t: Throwable => {
        t.printStackTrace()
        System.exit(1)
      }
    }
  }
}
