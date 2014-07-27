package io.github.dmitrib.elasticsearch.cli

import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{SearchPhaseExecutionException, SearchRequestBuilder, SearchRequest, SearchResponse}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.index.query.{QueryBuilder, FilterBuilders, QueryBuilders}
import org.elasticsearch.search.SearchHits
import scala.collection.JavaConverters._

@Parameters(commandDescription = "Retrieve documents by field values")
trait SearchByInputCommand extends Runnable {
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
    names = Array("--src-id-tsv"),
    description = "print ID and source separated by TAB")
  val srcIdTsv = false

  @Parameter(
    names = Array("--max-jobs"),
    description = "number of requests to execute in parallel")
  val maxJobs = 1

  @Parameter(
    names = Array("--rejected-execution-delay"),
    description = "delay after rejected execution (in milliseconds)")
  val rejectedExecutionDelayMillis = 30

  def query(batch: Seq[String]): QueryBuilder

  trait RetryListener extends ActionListener[SearchResponse] {
    val request: SearchRequestBuilder

    override def onFailure(e: Throwable) {
      e.getCause match {
        case ex: SearchPhaseExecutionException =>
          if (ex.shardFailures().exists(_.failure().isInstanceOf[EsRejectedExecutionException])) {
            Thread.sleep(rejectedExecutionDelayMillis)
            request.execute(this)
          }
        case _ => fail(e)
      }
    }

    def fail(e: Throwable)
  }

  def run() {
    val stream = Option(file).fold(System.in)(new FileInputStream(_))
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)

    val respQueue = new ArrayBlockingQueue[Either[(SearchHits, Boolean), Throwable]](maxJobs)

    def executeBatch(batch: Seq[String]) {
      val qb = query(batch)
      val req = client.prepareSearch(index)
        .setQuery(qb)
        .setSize(batch.size)
        .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))
      Option(kind).foreach(req.setTypes(_))

      if (!excludeFields.isEmpty || !includeFields.isEmpty) {
        req.setFetchSource(
          includeFields.asScala.toArray,
          excludeFields.asScala.toArray
        )
      }

      req.execute(new RetryListener {
        val request = req

        override def fail(e: Throwable) {
          respQueue.put(Right(e))
        }

        override def onResponse(response: SearchResponse) {
          val hits = response.getHits
          if (hits.totalHits() > batch.size) {
            req.setSize(hits.totalHits().toInt - hits.getHits.size)
              .setFrom(hits.getHits.size)

            req.execute(new RetryListener {
              val request = req

              override def onResponse(response: SearchResponse) {
                respQueue.put(Left(response.getHits, true))
              }

              override def fail(e: Throwable) {
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
    var pollTimeotCount = 0

    while (activeJobs > 0 || it.hasNext) {
      while (activeJobs < maxJobs && it.hasNext) {
        executeBatch(it.next())
        activeJobs = activeJobs + 1
      }
      val res = respQueue.poll(requestTimeoutMins*60+10, TimeUnit.SECONDS)

      res match {
        case Left((hits, finished)) =>
          if (finished)
            activeJobs = activeJobs - 1
          hits.getHits.foreach { hit =>
            println(hitToString(hit.getId, hit.getSourceAsString, srcOnly, srcIdTsv))
          }
        case Right(e) =>
          throw e
        case null =>
          throw new RuntimeException("timeout on waiting for response")
      }
    }

    client.close()
  }
}

object SearchByInputQueryCommand extends SearchByInputCommand {
  override def query(batch: Seq[String]): QueryBuilder = {
    val qb = QueryBuilders.boolQuery()
    batch.foreach { attr => qb.should(QueryBuilders.termQuery(searchField, attr)) }
    qb
  }
}

object SearchByInputFilteredCommand extends SearchByInputCommand {
  def query(batch: Seq[String]) = {
    val fb = FilterBuilders.orFilter(
      batch.map { attr => FilterBuilders.termFilter(searchField, attr).cache(false) } :_*
    ).cache(false)
    val qb = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), fb)
    qb
  }
}
