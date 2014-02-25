package io.github.dmitrib.elasticsearch.cli

import com.beust.jcommander.{Parameters, Parameter}
import java.io.{InputStreamReader, BufferedReader, FileInputStream}

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

  def run() {
    val stream = Option(file).map(new FileInputStream(_)).getOrElse(System.in)

    val reader = new BufferedReader(new InputStreamReader(stream))
    val it = Iterator.continually(reader.readLine).takeWhile(_ != null).grouped(batchSize)
    for (batch <- it) {
      val req = client.prepareBulk()
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
      val res = req.execute().actionGet()
      if (res.hasFailures) {
        println(res.buildFailureMessage())
      }
    }

    client.admin().indices().prepareFlush(index).execute().get()

    client.close()
  }
}
