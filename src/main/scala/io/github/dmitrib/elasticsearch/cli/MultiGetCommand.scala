package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameters, Parameter}
import java.util
import java.io.{FileInputStream, InputStreamReader, BufferedReader}
import org.elasticsearch.index.query.QueryBuilders
import scala.collection.JavaConverters._

@Parameters(commandDescription = "Get documents by its IDs")
object MultiGetCommand extends Runnable {
  import EsTool._

  @Parameter(
    names = Array("--batch-size"),
    description = "Number of params to supply in each search request")
  var batchSize = 100

  @Parameter(
    names = Array("-f", "--field"),
    description = "A field to retrieve, can be specified multiple times; if no fields is supplied the whole document will be returned")
   var fields: util.List[String] = new util.ArrayList[String]

  @Parameter(
    names = Array("--file"),
    description = "A file to read newline-separated document IDs, system input will be used if no file is specified")
  var file: String = _

  def run() {
    val stream = Option(file).map(new FileInputStream(_)).getOrElse(System.in)
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)
    for (batch <- it) {
      val idsQ = QueryBuilders.idsQuery(kind)
      batch.foreach(idsQ.addIds(_))
      val req = client.prepareSearch(index).setQuery(idsQ).setSize(batch.size)
      fields.asScala.foreach(req.addField)
      val resp = req.execute().actionGet()
      resp.getHits.getHits.foreach((h) => println(hitToString(h)))

      client.close()
    }
  }
}
