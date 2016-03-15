package it.dtk.akkastream.test

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.FanInShape.{ Init, Name }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.Await
import scala.util._
import scala.concurrent.duration._

object LogExample extends App {

  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val f = Source(1 to 100).log("before-map")
    .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
    .map(List(_))
    .to(Sink.foreach(println)).run()

}

object Tutorial0 extends App {

  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val f = Source(1 to 100)
    .map(List(_))
    .to(Sink.foreach(println)).run()

}

/**
 * Created by fabiofumarola on 08/03/16.
 */
object Tutorial extends App {

  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val sink = Sink.fold[Int, Int](0)(_ + _)
  val runnnable = Source(1 to 10).toMat(sink)(Keep.right)

  val f = Source(1 to 100)
    .map(_ * 2)
    .to(Sink.foreach(println)).run()

  val sum1 = runnnable.run()
  val sum2 = runnnable.run()

  sum1.onComplete {
    case Success(res) =>
      println(res)

    case Failure(ex) => ex.printStackTrace()
  }

  sum2.onComplete {
    case Success(res) =>
      println(res)

    case Failure(ex) => ex.printStackTrace()
  }

  Source(1 to 6)
    .map(_ * 2)
    .to(Sink.foreach(println)).run()

  val myFlow =
    Flow[Int].map(_ * 2)
      .alsoTo(Sink.foreach(println)).to(Sink.ignore)

  Source(1 to 10).to(myFlow).run()

}

import akka.stream.Fusing

object FusionTest extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val flow = Flow[Int].map(_ * 2).filter(_ > 500)
  val fused = Fusing.aggressive(flow)

  Source.fromIterator { () => Iterator from 0 }
    .via(fused)
    .take(1000)
    .to(Sink.foreach(println)).run()

}

object SimpleGraph extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._

    val in = Source(1 to 10)
    val out = Sink.foreach(println)

    val bcast = builder.add(Broadcast[Int](2))
    val merge = builder.add(Merge[Int](2))

    val f1, f2, f3, f4 = Flow[Int].map(_ + 10)

    in ~> f1 ~> bcast ~> f2 ~> merge ~> f3 ~> out
    bcast ~> f2 ~> merge

    ClosedShape
  })

  g.run()

}

import scala.concurrent.duration._

object UnconnectedGraph extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val pickMaxOfThree = GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val zip1 = b.add(ZipWith[Int, Int, Int](math.max _))
    val zip2 = b.add(ZipWith[Int, Int, Int](math.max _))
    zip1.out ~> zip2.in0

    UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
  }

  val resultSink = Sink.head[Int]

  val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit b => sink =>
    import GraphDSL.Implicits._

    val pm3 = b.add(pickMaxOfThree)

    Source.single(1) ~> pm3.in(0)
    Source.single(2) ~> pm3.in(1)
    Source.single(3) ~> pm3.in(2)

    pm3.out ~> sink.in

    ClosedShape
  })

  val max = g.run()

  println(Await.result(max, 300.millis))
}

object SimpleCombination extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val sourceOne = Source.single(1)
  val sourceTwo = Source.single(2)
  val merged = Source.combine(sourceOne, sourceTwo)(Merge(_))

  val result = merged
    .reduce(_ + _)
    .runWith(Sink.foreach(println))

}

case class PriorityWorkerPoolShape[In, Out](
    jobsIn: Inlet[In],
    priorityIn: Inlet[In],
    result: Outlet[Out]
) extends Shape {

  override def inlets: immutable.Seq[Inlet[_]] = jobsIn :: priorityIn :: Nil

  override def outlets: immutable.Seq[Outlet[_]] = result :: Nil

  override def deepCopy(): Shape = PriorityWorkerPoolShape(
    jobsIn.carbonCopy(),
    priorityIn.carbonCopy(),
    result.carbonCopy()
  )

  override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape = {
    PriorityWorkerPoolShape[In, Out](
      inlets(0).as[In],
      inlets(1).as[In],
      outlets(0).as[Out]
    )
  }
}

case class PriorityWorkerPoolShape2[In, Out](_init: Init[Out] = Name("PriorityWorkerPool"))
    extends FanInShape[Out](_init) {
  override protected def construct(init: Init[Out]): FanInShape[Out] = new PriorityWorkerPoolShape2(init)

  val jobsIn = newInlet[In]("jobsIn")
  val priorityIn = newInlet[In]("priorityIn")

}

object PriorityWorkerPool {
  def apply[In, Out](
    worker: Flow[In, Out, Any],
    workerCount: Int
  ): Graph[PriorityWorkerPoolShape[In, Out], NotUsed] = {

    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val priorityMerge = b.add(MergePreferred[In](1))
      val balance = b.add(Balance[In](workerCount))
      val resultsMerge = b.add(Merge[Out](workerCount))

      priorityMerge ~> balance

      for (i <- 0 until workerCount)
        balance.out(i) ~> worker ~> resultsMerge.in(i)

      PriorityWorkerPoolShape(
        jobsIn = priorityMerge.in(0),
        priorityIn = priorityMerge.preferred,
        result = resultsMerge.out
      )
    }
  }
}

object WorkerPoolExample extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val worker1 = Flow[String].map("step 1 " + _)
  val worker2 = Flow[String].map("step 2 " + _)

  RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val priorityPool1 = b.add(PriorityWorkerPool(worker1, 4))
    val priorityPool2 = b.add(PriorityWorkerPool(worker2, 2))

    Source(1 to 100).map("job: " + _) ~> priorityPool1.jobsIn
    Source(1 to 100).map("priority job: " + _) ~> priorityPool1.priorityIn

    priorityPool1.result ~> priorityPool2.jobsIn
    Source(1 to 100).map("one-step, priority " + _) ~> priorityPool2.priorityIn

    priorityPool2.result ~> Sink.foreach(println)
    ClosedShape
  }).run()

}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import scala.util._

object ExampleHttpClient extends App {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  val response = Http().singleRequest(HttpRequest(uri = "http://www.google.it"))

  response.onComplete {
    case Success(response) =>
      println(response.headers)
      val body = response.entity.dataBytes.runFold(ByteString(""))(_ ++ _)
      val b = Await.result(body, 5 seconds)
      println(b.utf8String)

    case Failure(ex) =>
      ex.printStackTrace()
  }

}