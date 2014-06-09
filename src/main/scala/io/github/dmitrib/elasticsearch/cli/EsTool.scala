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
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.ec2.model.{Filter, DescribeInstancesRequest}
import com.amazonaws.regions.{Region, Regions}

object EsTool {
  @Parameter(names = Array("--index"), description = "ElasticSearch index", required = true)
  var index: String = _
  @Parameter(names = Array("--type"), description = "ElasticSearch document type")
  var kind: String = _

  @Parameter(
    names = Array("--sniff"),
    description = "Request a list of other cluster nodes from the endpoint nodes")
  var nodeSniff = false

  @Parameter(
    names = Array("--endpoint"),
    description = "Cluster endpoints; can be specified multiple times")
  var endpoints = new util.ArrayList[String]

  @Parameter(
    names = Array("--ec2-tag"),
    description = "Use running EC2 instances with certain tag as cluster endpoints; tag should be set in form name:value")
  var ec2tag: String = _

  @Parameter(
    names = Array("--ec2-node-port"),
    description = "Connection port for discovered EC2 nodes")
  var ec2nodePort = 9300

  @Parameter(
    names = Array("--ec2-region"),
    description = "EC2 region for node discovery")
  var ec2region = Regions.EU_WEST_1.getName

  def discoverEc2Endpoints(tagName: String, tagValue: String) = {
    val ec2 = new AmazonEC2Client
    ec2.setRegion(Region.getRegion(Regions.fromName(ec2region)))
    val dreq = new DescribeInstancesRequest().withFilters(
      new Filter(s"tag:$tagName").withValues(tagValue),
      new Filter("instance-state-name").withValues("running")
    )
    val resp = ec2.describeInstances(dreq)
    resp.getReservations.asScala.flatMap(_.getInstances.asScala.map(_.getPublicDnsName))
  }

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

    val discoveredEndpoints = Option(ec2tag).map(_.split(":")).collect {
      case Array(name, value) => discoverEc2Endpoints(name, value)
    }.getOrElse(Seq.empty[String]).map { host =>
      new InetSocketTransportAddress(host, ec2nodePort)
    }

    val providedEndpoints = endpoints.asScala.map(_.split(":")).map {
      case Array(host, port) =>
        new InetSocketTransportAddress(host, port.toInt)
      case Array(host) =>
        new InetSocketTransportAddress(host, 9300)
    }

    if (discoveredEndpoints.isEmpty && providedEndpoints.isEmpty) {
      throw new ParameterException("no provided endpotints and can't discover EC2 endpoints")
    }

    val client = new TransportClient(settings)
    discoveredEndpoints.foreach(client.addTransportAddress)
    providedEndpoints.foreach(client.addTransportAddress)

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
