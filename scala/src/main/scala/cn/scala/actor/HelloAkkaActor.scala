package scala.cn.scala.actor

import akka.actor.{Actor, ActorSystem, Props}

/**
  * Created by wangzhilong on 2017/6/2.
  */
class HelloAkkaActor extends Actor{
  override def receive: Receive = {
    case "hello" => println("hello back at yot")
    case _ => println("huh?")
  }
}

object HelloAkkaActor {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("HelloSystem")
    val helloActor = system.actorOf(Props[HelloAkkaActor], name="helloactor")
    helloActor ! "hello"
    helloActor ! "disa"
  }
}
