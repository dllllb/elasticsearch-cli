package io.github.dmitrib.elasticsearch.cli

import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.SearchHits
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

  @Parameter(
    names = Array("--exclude"),
    description = "A wildcard pattern for fields to exclude from source, can be specified multiple times")
  val excludeFields: util.List[String] = new util.ArrayList[String]

  @Parameter(
    names = Array("--include"),
    description = "A wildcard pattern for fields to include in source, can be specified multiple times")
  val includeFields: util.List[String] = new util.ArrayList[String]

  @Parameter(
    names = Array("--src-only"),
    description = "print only source JSON")
  val srcOnly = false

  @Parameter(
    names = Array("--max-jobs"),
    description = "number of requests to execute in parallel")
  val maxJobs = 1

  def run() {
    val stream = Option(file).fold(System.in)(new FileInputStream(_))
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)

    val respQueue = new ArrayBlockingQueue[Either[(SearchHits, Boolean), Throwable]](maxJobs)

    def executeBatch(batch: Seq[String]) {
      val fb = FilterBuilders.orFilter(
        batch.map { attr => FilterBuilders.termFilter(searchField, attr).cache(false) } :_*
      ).cache(false)
      val qb = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), fb)
      val req = client.prepareSearch(index)
        .setQuery(qb).setSize(batch.size)
      Option(kind).foreach(req.setTypes(_))
      fields.asScala.foreach(req.addField)

      if (!excludeFields.isEmpty || !includeFields.isEmpty) {
        req.addPartialField("partial",
          includeFields.asScala.toArray,
          excludeFields.asScala.toArray
        )
      }

      req.execute(new ActionListener[SearchResponse] {
        override def onFailure(e: Throwable) {
          respQueue.put(Right(e))
        }
        override def onResponse(response: SearchResponse) {
          val hits = response.getHits
          if (hits.totalHits() > batch.size) {
            req.setSize(hits.totalHits().toInt - hits.getHits.size)
              .setFrom(hits.getHits.size)

            req.execute(new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse) {
                respQueue.put(Left(response.getHits, true))
              }

              override def onFailure(e: Throwable) {
                respQueue.put(Right(e))
              }
            })
            respQueue.put(Left(hits, false))
          }
          respQueue.put(Left(hits, true))
        }
      })
    }

    var activeJobs = 0

    while (activeJobs > 0 || it.hasNext) {
      while (activeJobs < maxJobs && it.hasNext) {
        executeBatch(it.next())
        activeJobs = activeJobs + 1
      }
      val res = respQueue.poll(5, TimeUnit.MINUTES)

      res match {
        case Left((hits, finished)) =>
          if (finished)
            activeJobs = activeJobs - 1
          hits.getHits.foreach { hit =>
            println(hitToString(hit, srcOnly))
          }
        case Right(e) =>
          throw e
      }
    }

    client.close()
  }
}
