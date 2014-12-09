package io.github.dmitrib.elasticsearch.cli

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

  @Parameter(
    names = Array("--ping-timeout-sec"),
    description = "Ping timeout seconds")
  var pingTimeoutSec = 15

  @Parameter(names = Array("--help"), help = true)
  var help: Boolean = _

  lazy val client = {
    val settings = ImmutableSettings.settingsBuilder
      .put("client.transport.ignore_cluster_name", true)
      .put("client.transport.sniff", nodeSniff)
      .put("client.transport.ping_timeout", pingTimeoutSec)
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
      throw new ParameterException("no provided endpoints and can't discover EC2 endpoints")
    }

    val client = new TransportClient(settings)
    discoveredEndpoints.foreach(client.addTransportAddress)
    providedEndpoints.foreach(client.addTransportAddress)

    client
  }

  def hitToString(id: String, source: String, srcOnly: Boolean, srcIdTsv: Boolean) = {
    if (srcOnly) {
      source
    } else if (srcIdTsv) {
      s"$id\t$source"
    } else {
      s"""{"_id":"$id","_source":$source}"""
    }
  }

  def main(args: Array[String]) {
    try {
      val jc = new JCommander
      jc.addObject(EsTool)
      jc.addCommand("scan", ScanCommand)
      jc.addCommand("scan-join", ScanJoinCommand)
      jc.addCommand("batch-upload", BatchUploadCommand)
      jc.addCommand("search-by-input", SearchByInputParallel)
      jc.addCommand("multi-get", MultiGetCommand)

      jc.parse(args: _*)

      if (help) {
        jc.usage()
      } else {
        jc.getCommands.get(jc.getParsedCommand).getObjects.asScala.head.asInstanceOf[Runnable].run()
        client.close()
      }
    } catch {
      case e: ParameterException =>
        println(e.getMessage)
        println("use --help to see the available options")
        System.exit(1)
      case t: Throwable =>
        println(t.getMessage)
        t.printStackTrace()
        System.exit(1)
    }
  }
}
