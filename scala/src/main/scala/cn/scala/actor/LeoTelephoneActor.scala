package scala.cn.scala.actor

import akka.actor.{Actor, ActorSystem, Props}
/**
  * Created by wangzhilong on 2017/6/2.
  */
case class Message(content:String, sender: Actor)

class LeoTelephoneActor extends Actor{
  override def receive: Receive = {
    case Message(content,sender) => {
      println("leo telephone:"+content)
//      sender ! "I'm leo,please call me after 10 minutes."
    }
  }


}

class JackTelephoneActor(val leoTelephoneActor: Actor) extends Actor{


  override def receive: Receive = {
    case response:String => println("jack telephone :" +response)
  }

  override def preStart(): Unit = {
//      leoTelephoneActor ! Message("Hello,leo,I'm Jack.", this)
  }
}

object PhoneMessage{
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("PhoneMessage")
    val props = Props(new LeoTelephoneActor)
    val actor = system.actorOf(props,name = "actor")
    actor ! "bobo"
  }
}
