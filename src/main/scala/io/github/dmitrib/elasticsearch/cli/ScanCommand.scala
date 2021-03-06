package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameter, Parameters}
import java.util
import org.elasticsearch.index.query.{QueryBuilder, FilterBuilders, QueryBuilders}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import io.github.dmitrib.elasticsearch.cli.EsTool._

trait ScanCommandParams extends {
  @Parameter(names = Array("--query"), description = "Search query in Lucene syntax")
  var query: String = _

  def queryBuilder = {
    val q: QueryBuilder = Option(query)
      .map(QueryBuilders.queryString)
      .getOrElse(QueryBuilders.matchAllQuery())

    Option(scriptFilter).fold(q) { script =>
      QueryBuilders.filteredQuery(q, FilterBuilders.scriptFilter(script))
    }
  }

  @Parameter(
    names = Array("--hits-per-shard"),
    description = "Number of hits to extract in each iteration from each shard")
  var _hitsPerShard: Integer = _

  lazy val hitsPerShard = Option(_hitsPerShard).fold {
    val resp = client.admin().indices().prepareStats(index).get(TimeValue.timeValueMinutes(5))
    val stats = resp.getTotal
    val primaryShardCount = resp.getShards.count(_.getShardRouting.primary)

    val docCount = stats.getDocs.getCount
    val indexSize = stats.getStore.getSizeInBytes

    val hps = (5000000D/(indexSize.toDouble/docCount*primaryShardCount)).toInt
    System.err.println(s"using $hps hits per shard")
    hps
  } { hps =>
    hps.intValue
  }

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

  @Parameter(
    names = Array("--src-id-tsv"),
    description = "print ID and source separated by TAB")
  val srcIdTsv = false
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
      reqBuilder.setFetchSource(
        includeFields.asScala.toArray,
        excludeFields.asScala.toArray
      )
    }

    val (it, total) = EsUtil.scan(client, reqBuilder, retryMax, requestTimeoutMins)
    System.err.println(s"total hits: $total")
    it.flatMap(_.getHits.getHits).zip(Stream.from(1).iterator).foreach { case (hit, count) =>
      if (count % (total/100) == 0) {
        System.err.println(s"${count/(total/100)}%")
      }
      println(hitToString(hit.getId, hit.getSourceAsString, srcOnly, srcIdTsv))
    }
  }
}
