package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import org.elasticsearch.client.transport.NoNodeAvailableException
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import org.elasticsearch.search.SearchHit

trait ScanCommandParams extends {
  @Parameter(names = Array("--query"), description = "Search query in Lucene syntax")
  var query: String = _

  def queryBuilder = Option(query)
    .map(QueryBuilders.queryString)
    .getOrElse(QueryBuilders.matchAllQuery())

  @Parameter(
    names = Array("-f", "--field"),
    description = "A field to retrieve, can be specified multiple times; if no fields is supplied the whole document will be returned")
   var fields: util.List[String] = new util.ArrayList[String]

  @Parameter(
    names = Array("--hits-per-shard"),
    description = "Number of hits to extract in each iteration from each shard")
  var hitsPerShard = 1000

  @Parameter(
    names = Array("--retry-max"),
    description = "Number of retries for a failed scan request"
  )
  var retryMax = 3

  @Parameter(
    names = Array("--routing"),
    description = "Param to calculate a shard to execute search"
  )
  var routing: String = _

  @Parameter(
    names = Array("--shard"),
    description = "Shard number on which to execute search"
  )
  var shard: String = _
}

@Parameters(commandDescription = "Read search resutls using scroll")
object ScanCommand extends ScanCommandParams with Runnable {
  import EsTool._

  def scan(reqBuilder: SearchRequestBuilder)(hitAction: (Array[SearchHit]) => Unit) {
    var scrollResp = reqBuilder.execute.actionGet(requestTimeoutMins, TimeUnit.MINUTES)

    def scroll() {
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(600000))
        .execute
        .actionGet(requestTimeoutMins, TimeUnit.MINUTES)

      val hits = scrollResp.getHits.getHits
      hitAction(hits)
    }

    def scrollWithRetry(retryCount: Int) {
      try {
        scroll()
      } catch {
        case e: NoNodeAvailableException => {
          System.err.println(s"scroll attempt N:$retryCount failed: ${e.getMessage}")
          if (retryCount < retryMax) {
            Thread.sleep(1000)
            scrollWithRetry(retryCount+1)
          } else {
            throw e
          }
        }
      }
    }

    @tailrec def iterate() {
      scrollWithRetry(1)

      if (scrollResp.getHits.getHits.length == 0) {
        return
      } else {
        iterate()
      }
    }

    iterate()

    client.close()
  }

  def run() {
    val reqBuilder = client.prepareSearch(index)
      .setTypes(kind)
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(600000))
      .setQuery(queryBuilder)
      .setSize(hitsPerShard)
      .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))

    Option(routing) foreach { (r) =>
      reqBuilder.setRouting(r)
    }

    Option(shard) foreach { (s) =>
      reqBuilder.setPreference(s"_shards:$s")
    }

    ScanCommand.fields.asScala.foreach(reqBuilder.addField)

    scan(reqBuilder) { (hits) =>
      hits.foreach((h) => println(hitToString(h)))
    }

    client.close()
  }
}
