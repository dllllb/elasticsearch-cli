package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import org.elasticsearch.index.query.QueryBuilders
import scala.collection.JavaConverters._

@Parameters(commandDescription = "Retrieve documents by field values")
object SearchByInputCommand extends Runnable {
  import EsTool._

  @Parameter(
    names = Array("--search-field"),
    description = "A field to search",
    required = true)
  var searchField: String = _

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
    description = "A file to read newline-separated search attributes, system input will be used if no file is specified")
  var file: String = _

  def run() {
    val stream = Option(file).map(new FileInputStream(_)).getOrElse(System.in)
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)
    for (batch <- it) {
      val qb = QueryBuilders.boolQuery()
      batch.foreach((attr) => qb.should(QueryBuilders.termQuery(searchField, attr)))
      val req = client.prepareSearch(index).setQuery(qb).setSize(batch.size)
      Option(kind).foreach(req.setTypes(_))
      fields.asScala.foreach(req.addField)

      val resp = req.execute().actionGet()
      val hits = resp.getHits
      hits.getHits.foreach((h) => println(hitToString(h)))

      if (hits.totalHits() > batch.size) {
        req.setSize(hits.totalHits().toInt - hits.getHits.size)
          .setFrom(hits.getHits.size)
        val respAdd = req.execute().actionGet()
        respAdd.getHits.getHits.foreach((h) => println(hitToString(h)))
      }
    }

    client.close()
  }
}
