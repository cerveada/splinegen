package com.github.cerveada.splinegen

import org.apache.commons.configuration.BaseConfiguration
import org.apache.commons.io.FileUtils
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpLineageDispatcherConfig
import za.co.absa.spline.harvester.dispatcher.{ConsoleLineageDispatcher, HttpLineageDispatcher, LineageDispatcher}

import scala.concurrent.duration.DurationInt

/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {

    val opCount = args(0).toInt
    println(s"Plan with $opCount operations will be generated.")
    println(s"Approximate size of plan string is ${FileUtils.byteCountToDisplaySize(opCount * 180)}")

    val dispatcher = createDispatcher("file", opCount)

    println("Generating plan")
    val plan = PlanGenerator.generate(opCount)
    dispatcher.send(plan)

    println("Generating event")
    val event = EventGenerator.generate(plan)
    dispatcher.send(event)
  }

  private def createDispatcher(name: String, opCount: Int): LineageDispatcher = name match {
    case "file" =>
      new FileDispatcher(s"lineage-${opCount}ops")
    case "console" =>
      new ConsoleLineageDispatcher(new BaseConfiguration {
        addProperty("stream", "OUT")
      })
    case "http" =>
      new HttpLineageDispatcher(new BaseConfiguration {
        addProperty(HttpLineageDispatcherConfig.ProducerUrlProperty, "http://localhost:8080/producer")
        addProperty(HttpLineageDispatcherConfig.ReadTimeoutMsKey, 120.seconds.toMillis)
        addProperty(HttpLineageDispatcherConfig.ConnectionTimeoutMsKey, 120.seconds.toMillis)
      })
  }
}
