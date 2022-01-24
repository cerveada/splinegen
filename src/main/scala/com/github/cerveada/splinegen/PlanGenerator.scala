package com.github.cerveada.splinegen

import za.co.absa.spline.producer.model.v1_1._

import java.util.UUID
import scala.annotation.tailrec

object PlanGenerator {

  def generate(opCount: Long): ExecutionPlan = {
    val planId = UUID.randomUUID()
    ExecutionPlan(
      id = Some(planId),
      name = Some(s"generated plan $planId"),
      operations = generateOperations(opCount),
      attributes = None,
      expressions = None,
      systemInfo = NameAndVersion("splinegen", "0.1-SNAPSHOT"),
      agentInfo = None,
      extraInfo = None
    )
  }

  def generateOperations(opCount: Long): Operations = {

    val read = generateRead()
    val dataOps = generateDataOperations(opCount, Seq.empty, Seq(read.id))

    Operations(
      write = generateWrite(dataOps.last.id),
      reads = Some(Seq(read)),
      other = Some(dataOps),
    )
  }

  def generateRead(): ReadOperation = {
    val id = UUID.randomUUID().toString
    ReadOperation(
      inputSources = Seq(s"file://splinegen/read_$id.csv"),
      id = id,
      name = Some(s"generated read $id"),
      output = None,
      params = None,
      extra = None
    )
  }

  @tailrec
  def generateDataOperations(opCount: Long, allOps: Seq[DataOperation], childIds: Seq[String]): Seq[DataOperation] =
    if(opCount == 0) {
      allOps
    } else {
      val op = generateDataOperation(childIds)
      generateDataOperations(opCount - 1, allOps :+ op, Seq(op.id))
    }

  def generateDataOperation(childIds: Seq[String]) = {
    val id = UUID.randomUUID().toString
    DataOperation(
      id = id,
      name = Some(s"generated data operation $id"),
      childIds = Some(childIds),
      output = None,
      params = None,
      extra = None
    )
  }

  def generateWrite(childId: String): WriteOperation = {
    WriteOperation(
      outputSource = "file://splinegen/write.csv",
      append = false,
      id = UUID.randomUUID().toString,
      name = Some("generatedWrite"),
      childIds = Seq(childId),
      params = None,
      extra = None
    )
  }

}
