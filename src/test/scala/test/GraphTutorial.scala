package test

import akka.{NotUsed, Done}
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

/**
  * Created by fabiofumarola on 19/03/16.
  */
object GraphTutorial {
  implicit val actorSystem = ActorSystem("Tutorial")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher

  def main(args: Array[String]) {

    //    graph1().run()
    //    val res = graph2().run()
    //
    //    res match {
    //      case (f1, f2) =>
    //        f1.zip(f2).onComplete {
    //          case Success((a, b)) =>
    //            println(s"$a,$b")
    //          case Failure(ex) =>
    //            ex.printStackTrace()
    //        }
    //    }
    //    partialGraph1

    flowExample
  }


  def graph1(): RunnableGraph[NotUsed] = {

    val source = Source(1 to 1000)
    val sink1: Sink[Int, Future[Done]] = Sink.foreach(i => println(s"sink1 print: $i"))
    val sink2: Sink[Int, Future[Done]] = Sink.foreach(i => println(s"sink2 print: $i"))

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Int](2))

      source ~> bcast.in
      bcast.out(0) ~> Flow[Int].map(_ * 1) ~> sink1
      bcast.out(1) ~> Flow[Int].map(_ * 1) ~> sink2

      ClosedShape
    })

    g
  }

  def graph2(): RunnableGraph[(Future[Int], Future[Int])] = {

    val topHeadSink = Sink.head[Int]
    val bottomHeadSink = Sink.head[Int]
    val sharedDoubler = Flow[Int].map(_ * 2)

    RunnableGraph.fromGraph(GraphDSL.create(topHeadSink, bottomHeadSink)((_, _)) { implicit builder =>
      (topHS, bottomHS) =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[Int](2))
        Source.single(1) ~> broadcast.in

        broadcast.out(0) ~> sharedDoubler ~> topHS.in
        broadcast.out(1) ~> sharedDoubler ~> bottomHS.in
        ClosedShape
    })
  }


  def partialGraph1() = {

    val pickMaxOfThree: Graph[UniformFanInShape[Int, Int], NotUsed] = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip1 = b.add(ZipWith[Int, Int, Int](math.max))
      val zip2 = b.add(ZipWith[Int, Int, Int](math.max))
      zip1.out ~> zip2.in0

      UniformFanInShape(zip2.out, zip1.in0, zip1.in1, zip2.in1)
    }

    val resultSink: Sink[Int, Future[Int]] = Sink.head[Int]

    val g: RunnableGraph[Future[Int]] = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit b =>
      sink =>
        import GraphDSL.Implicits._

        val pm3 = b.add(pickMaxOfThree)

        Source.single(1) ~> pm3.in(0)
        Source.single(2) ~> pm3.in(1)
        Source.single(3) ~> pm3.in(2)
        pm3.out ~> sink.in
        ClosedShape
    })

    println(Await.result(g.run, 300.millis))
  }


  def sourceExample(): Unit = {

    val pairSource: Source[(Int, Int), NotUsed] = Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip = b.add(Zip[Int, Int]())

      def ints = Source.fromIterator(() => Iterator.from(1))

      ints.filter(_ % 2 != 0) ~> zip.in0
      ints.filter(_ % 2 == 0) ~> zip.in1

      SourceShape(zip.out)
    })

    pairSource.runForeach(p => println(p))
  }

  def sinkExample(): Unit = {

    val pairSink: Sink[Int, NotUsed] = Sink.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val zip = b.add(Zip[Int, Int]())
      def ints = Source.fromIterator(() => Iterator.from(1))

      ints.filter(_ % 2 != 0) ~> zip.in0
      zip.out ~> Sink.foreach(println)

      SinkShape.of(zip.in1)
    })


    Source(1000 to 1010)
      .to(pairSink).run()
  }


  def flowExample(): Unit = {

    val flow = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Int](2))
      val zip = b.add(Zip[Int, String]())

      bcast.out(0).map(identity) ~> zip.in0
      bcast.out(1).map(_.toString + " toString") ~> zip.in1

      FlowShape(bcast.in, zip.out)
    })


    Source(1 to 100)
      .via(flow)
      .runForeach(println)
  }
}
