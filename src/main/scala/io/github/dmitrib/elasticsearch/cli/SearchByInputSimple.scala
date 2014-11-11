package io.github.dmitrib.elasticsearch.cli

import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import java.util.concurrent.TimeUnit

import io.github.dmitrib.elasticsearch.cli.EsTool._
import org.elasticsearch.common.unit.TimeValue

import scala.collection.JavaConverters._

object SearchByInputSimple extends SearchByInputCommand {
  override def run() {
    val stream = Option(file).fold(System.in)(new FileInputStream(_))
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)

    it.foreach { batch =>
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

      val resp = req.execute().get(requestTimeoutMins * 60 + 10, TimeUnit.SECONDS)
      val hits = resp.getHits

      hits.getHits.foreach { hit =>
        println(hitToString(hit.getId, hit.getSourceAsString, srcOnly, srcIdTsv))
      }

      if (hits.totalHits() > batch.size) {
        req.setSize(hits.totalHits().toInt - hits.getHits.size)
          .setFrom(hits.getHits.size)
        val addResp = req.execute().get()

        addResp.getHits.getHits.foreach { hit =>
          println(hitToString(hit.getId, hit.getSourceAsString, srcOnly, srcIdTsv))
        }
      }
    }
  }
}
