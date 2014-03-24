package io.github.dmitrib.elasticsearch.cli

import org.elasticsearch.action.search.{SearchResponse, SearchRequestBuilder}
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.client.Client

/**
 * @author Dmitri Babaev (dmitri.babaev@gmail.com)
 */
object EsUtil {
  def scan(client: Client, reqBuilder: SearchRequestBuilder, retryMax: Int, requestTimeoutMins: Int) = {
    def scroll(scrollResp: SearchResponse, retryCount: Int): SearchResponse = {
      try {
        client.prepareSearchScroll(scrollResp.getScrollId)
          .setScroll(new TimeValue(600000))
          .execute
          .actionGet(requestTimeoutMins, TimeUnit.MINUTES)
      } catch {
        case e: NoNodeAvailableException =>
          System.err.println(s"scroll attempt N:$retryCount failed: ${e.getMessage}")
          if (retryCount <= retryMax) {
            Thread.sleep(1000)
            scroll(scrollResp, retryCount+1)
          } else {
            throw e
          }
      }
    }

    val scrollResp = reqBuilder.execute.actionGet(requestTimeoutMins, TimeUnit.MINUTES)
    val it = Iterator.continually {
      scroll(scrollResp, 1)
    }.takeWhile(_.getHits.getHits.length != 0)
    (it, scrollResp.getHits.totalHits())
  }
}
