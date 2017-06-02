package scala.cn.scala.akka

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging

/**
  * Created by wangzhilong on 2017/6/2.
  */
class MyActor extends Actor{
  val log = Logging(context.system,this)
  println("init MyActor")
  override def receive: Receive = {
    case "test" => log.info("received test")
    case other => log.info("receive unknown message : " + other)
  }

  override def preStart(): Unit = {
    println("preStart code")
  }
}

object MyActor{
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("MySystem")
    val myActor = system.actorOf(Props[MyActor],name = "myActor")
    myActor ! "tefst"
  }
}
