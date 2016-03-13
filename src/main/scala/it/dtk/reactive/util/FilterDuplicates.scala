package it.dtk.reactive.util

import akka.stream.{Outlet, Inlet, Attributes, FlowShape}
import akka.stream.stage.{InHandler, GraphStageLogic, GraphStage}

import scala.collection.mutable

/**
  * Created by fabiofumarola on 10/03/16.
  * this class filter duplicates in a stream using a local memory
  */
class FilterDuplicates[A](size: Int) extends GraphStage[FlowShape[A, A]] {

  val in = Inlet[A]("FilterDuplicates.in")
  val out = Outlet[A]("FilterDuplicates.out")

  override def shape: FlowShape[A, A] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val lastElements = mutable.Queue[A]()
      val elementsSet = mutable.Set[A]()

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          val contains = elementsSet.contains(elem)

          lastElements.enqueue(elem)
          ensureSize()

          if (contains)
            pull(in)
          else push(out, elem)
        }

        def ensureSize(): Unit = {
          for (i <- (size - 1) to lastElements.size) {
            elementsSet -= lastElements.dequeue()
          }
        }

        override def onUpstreamFinish(): Unit = {
          complete(out)
        }
      })
    }

}
