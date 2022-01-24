package com.github.cerveada.splinegen

import za.co.absa.spline.producer.model.v1_1._

object EventGenerator {

  def generate(executionPlan: ExecutionPlan): ExecutionEvent = {
    ExecutionEvent(
      planId = executionPlan.id.get,
      timestamp = System.currentTimeMillis(),
      durationNs = None,
      error = None,
      extra = None
    )
  }
}
