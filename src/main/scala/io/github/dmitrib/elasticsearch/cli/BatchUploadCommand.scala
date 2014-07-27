package io.github.dmitrib.elasticsearch.cli

import java.util.concurrent.{TimeUnit, ArrayBlockingQueue}

import com.beust.jcommander.{Parameters, Parameter}
import java.io.{InputStreamReader, BufferedReader, FileInputStream}

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.common.unit.TimeValue

@Parameters(commandDescription = "Upload a list of documents in batches")
object BatchUploadCommand extends Runnable {
  import EsTool._

  @Parameter(
    names = Array("--batch-size"),
    description = "Number of params to supply in each search request")
  var batchSize = 10000

  @Parameter(
    names = Array("--file"),
    description = "A file with newline-separated 'id TAB json' to upload, system input will be used if no file is specified")
  var file: String = _

  @Parameter(
    names = Array("--max-jobs"),
    description = "number of requests to execute in parallel")
  val maxJobs = 1

  def run() {
    val stream = Option(file).fold(System.in)(new FileInputStream(_))

    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)

    val respQueue = new ArrayBlockingQueue[Either[BulkResponse, Throwable]](maxJobs)

    def executeBatch(batch: Seq[String]) {
      val req = client.prepareBulk()
        .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))
      for (line <- batch) {
        val (id, doc) = line.split("\t") match {
          case Array(a, b) => (a, b)
          case _ => throw new Exception(s"error splitting line $line")
        }
        req.add(client.prepareIndex(
          index,
          Option(kind).getOrElse(throw new IllegalStateException("type is not set")),
          id
        ).setSource(doc))
      }

      req.execute(new ActionListener[BulkResponse] {
        override def onResponse(response: BulkResponse) {
          respQueue.put(Left(response))
        }

        override def onFailure(e: Throwable) {
          respQueue.put(Right(e))
        }
      })
    }

    var activeJobs = 0

    while (activeJobs > 0 || it.hasNext) {
      while (activeJobs < maxJobs && it.hasNext) {
        executeBatch(it.next())
      }
      val res = respQueue.poll(requestTimeoutMins*60+10, TimeUnit.SECONDS)

      res match {
        case Left(resp) =>
          activeJobs = activeJobs - 1
          if (resp.hasFailures) {
            println(resp.buildFailureMessage())
          }
        case Right(e) =>
          throw e
        case null =>
          throw new RuntimeException("timeout on waiting for response")
      }
    }

    client.admin().indices().prepareFlush(index).execute().get()
  }
}
