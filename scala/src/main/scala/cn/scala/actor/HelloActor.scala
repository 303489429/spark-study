package scala.cn.scala.actor
/*

import scala.actors.Actor


/**
  * scala.actors.Actor 已不推荐使用，改用akka.actor
  * Created by wangzhilong on 2017/6/2.
  */
class HelloActor extends Actor{
  override def act(): Unit = {
    while (true){
      receive{
        case name:String => println("hello," + name)
      }
    }
  }
}

object HelloActor {
  def main(args: Array[String]): Unit = {
    val helloActor = new HelloActor
    helloActor.start()
    helloActor ! "leo"
  }
}
*/
