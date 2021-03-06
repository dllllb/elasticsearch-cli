package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameters, Parameter}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

@Parameters(commandDescription = "Use search result documents attribute to get documents from other index")
object ScanJoinCommand extends ScanCommandParams with Runnable {
  import EsTool._

  @Parameter(names = Array("--join-index"), description = "Index for documents to join", required = true)
  var joinIndex: String = _

  @Parameter(
    names = Array("--source-field"),
    description = "A field in source document to use for join",
    required = true)
  var sourceField: String = _

  @Parameter(
    names = Array("--target-field"),
    description = "A field in target document to use for join",
    required = true)
  var targetField: String = _

  @Parameter(names = Array("--join-request-size"), description = "Number of params in each join request")
  var joinRequestSize = 100

  def run() {
    val reqBuilder = client.prepareSearch(index)
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(600000))
      .setQuery(queryBuilder)
      .setSize(hitsPerShard)
      .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))

    Option(kind).foreach(reqBuilder.setTypes(_))

    reqBuilder.addField(sourceField)

    val (it, _) = EsUtil.scan(client, reqBuilder, retryMax, requestTimeoutMins)
    it.map(_.getHits.getHits).flatMap { hits =>
      val joinGroups = hits.map { (hit) =>
        hit.getFields.get(sourceField).getValue.toString
      }.grouped(joinRequestSize)

      joinGroups.flatMap { (group) =>
        val qb = QueryBuilders.boolQuery()
        group.foreach { (attr) => qb.should(QueryBuilders.termQuery(targetField, attr)) }

        val joinReq = client.prepareSearch(joinIndex)
          .setQuery(qb)
          .setSize(group.size)

        joinReq.execute().actionGet().getHits.getHits
      }
    }.foreach { hit =>
      println(hitToString(hit.getId, hit.getSourceAsString, srcOnly, srcIdTsv))
    }
  }
}
