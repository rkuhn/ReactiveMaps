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
  
  /*
   * TODO: use a CircuitBreaker to short-circuit GetUser requests while the
   * backend is unavailable
   */
  
  def receive = {
    case p: UserPosition ⇒ // TODO
    case g: GetUser      ⇒ // TODO
  }

}