package scala.cn.scala.actor

import akka.actor.{Actor, ActorSystem, Props}

/**
  * Created by wangzhilong on 2017/6/2.
  */
case class Login(name:String, password : String)

case class Register(name:String, password:String)

case class Result(msg:String)

class UserManagerActor extends Actor{
  override def receive: Receive = {
    case Login(name,password) => println("login,name is "+ name+", password is "+ password)
    case Register(name,password) => println("register,name is "+name+", password is "+ password)
    case _ => sender ! Result("unknowd message!")
  }
}

object UserManagerActor{
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("UserManagerActor")
    val actor = system.actorOf(Props[UserManagerActor],name = "actor")
    actor ! Register("leo","1234") ; actor ! Login("leo","1234")
    actor ! "leo"
  }
}
