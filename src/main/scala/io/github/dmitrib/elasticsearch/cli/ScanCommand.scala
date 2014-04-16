package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import org.elasticsearch.index.query.{QueryBuilder, FilterBuilders, QueryBuilders}
import org.elasticsearch.action.search.{SearchResponse, SearchRequestBuilder, SearchType}
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import org.elasticsearch.client.transport.NoNodeAvailableException
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import org.elasticsearch.search.SearchHit
import io.github.dmitrib.elasticsearch.cli.EsTool._

trait ScanCommandParams extends {
  @Parameter(names = Array("--query"), description = "Search query in Lucene syntax")
  var query: String = _

  def queryBuilder = {
    val q: QueryBuilder = Option(query)
      .map(QueryBuilders.queryString)
      .getOrElse(QueryBuilders.matchAllQuery())

    Option(scriptFilter).map { script =>
      QueryBuilders.filteredQuery(q, FilterBuilders.scriptFilter(script))
    }.getOrElse(q)
  }

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

  @Parameter(
    names = Array("--script-filter"),
    description = "Script to filter results"
  )
  var scriptFilter: String = _

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
}

@Parameters(commandDescription = "Read search results using scroll")
object ScanCommand extends ScanCommandParams with Runnable {
  import EsTool._

  def run() {
    val reqBuilder = client.prepareSearch(index)
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(600000))
      .setQuery(queryBuilder)
      .setSize(hitsPerShard)
      .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))

    Option(kind).foreach(reqBuilder.setTypes(_))
    Option(routing).foreach(reqBuilder.setRouting)

    Option(shard) foreach { s =>
      reqBuilder.setPreference(s"_shards:$s")
    }

    if (!excludeFields.isEmpty || !includeFields.isEmpty) {
      reqBuilder.addPartialField("partial",
        includeFields.asScala.toArray,
        excludeFields.asScala.toArray
      )
    }

    ScanCommand.fields.asScala.foreach(reqBuilder.addField)

    EsUtil.scan(client, reqBuilder, retryMax, requestTimeoutMins)._1.flatMap(_.getHits.getHits).foreach { h =>
      println(hitToString(h, srcOnly))
    }

    client.close()
  }
}
