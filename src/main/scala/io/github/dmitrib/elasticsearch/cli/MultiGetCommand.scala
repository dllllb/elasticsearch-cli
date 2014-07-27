package io.github.dmitrib.elasticsearch.cli

import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import java.util
import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

import com.beust.jcommander.{Parameter, Parameters}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.{MultiGetRequest, MultiGetResponse}
import org.elasticsearch.search.fetch.source.FetchSourceContext
import scala.collection.JavaConverters._

@Parameters(commandDescription = "Retrieve documents by its key")
object MultiGetCommand extends Runnable {
  import EsTool._

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

  def run() {
    val stream = Option(file).fold(System.in)(new FileInputStream(_))
    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)

    val respQueue = new ArrayBlockingQueue[Either[MultiGetResponse, Throwable]](maxJobs)

    def executeBatch(batch: Seq[String]) {

      val req = client.prepareMultiGet()
      batch.foreach { id =>
        val item = new MultiGetRequest.Item(
          index,
          Option(kind).getOrElse(throw new IllegalStateException("type is not set")),
          id
        )

        if (!excludeFields.isEmpty || !includeFields.isEmpty) {
          item.fetchSourceContext(
            new FetchSourceContext(includeFields.asScala.toArray, excludeFields.asScala.toArray)
          )
        }

        req.add(item)
      }


      req.execute(new ActionListener[MultiGetResponse] {
        override def onFailure(e: Throwable) {
          respQueue.put(Right(e))
        }
        override def onResponse(response: MultiGetResponse) {
          respQueue.put(Left(response))
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
        case Left(hits) =>
          activeJobs = activeJobs - 1
          hits.getResponses.foreach { hit =>
            if (hit.isFailed) {
              println(hit.getFailure.getMessage)
            } else if (hit.getResponse.isExists) {
              println(hitToString(hit.getId, hit.getResponse.getSourceAsString, srcOnly, srcIdTsv))
            }
          }
        case Right(e) =>
          throw e
        case null =>
          throw new RuntimeException("timeout on waiting for response")
      }
    }
  }
}
