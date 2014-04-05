package actors

import akka.actor.Actor
import akka.actor.Props
import akka.routing.FromConfig
import models.backend.UserPosition
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.CircuitBreaker

object UserMetaDataService {
  case class GetUser(id: String)
  case class User(id: String, distance: Double)
  
  val props = Props[UserMetaDataService]
}

class UserMetaDataService extends Actor {
  import UserMetaDataService._

  import context.dispatcher
  implicit val timeout = Timeout(2.seconds)
  val router = context.actorOf(Props.empty.withRouter(FromConfig), "router")
  
  val cb = CircuitBreaker(context.system.scheduler, 3, 1.second, 30.seconds)

  def receive = {
    case p: UserPosition ⇒ router ! p
    case g: GetUser      ⇒ cb.withCircuitBreaker(router ? g) pipeTo sender
  }

}