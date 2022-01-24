package com.github.cerveada.splinegen

import org.apache.commons.io.FileUtils
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher.ModelEntity
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.json.HarvesterJsonSerDe
import za.co.absa.spline.producer.model.v1_1.{ExecutionEvent, ExecutionPlan}

import java.io.File

class FileDispatcher(fileNamePrefix: String) extends LineageDispatcher{

  import HarvesterJsonSerDe.impl._
  import ModelEntity.Implicits._


  override def send(plan: ExecutionPlan): Unit =
    FileUtils.writeStringToFile(
      new File(s"$fileNamePrefix-plan.json"),
      plan.toJson
    )

  override def send(event: ExecutionEvent): Unit =
    FileUtils.writeStringToFile(
      new File(s"$fileNamePrefix-event.json"),
      Seq(event).toJson
    )

  private def getEntityName(entity: ModelEntity): String = entity.name.toLowerCase

}
