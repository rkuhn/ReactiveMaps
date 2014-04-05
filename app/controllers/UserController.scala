package controllers

import play.api.mvc.Controller
import play.api.libs.json.Json
import actors.UserMetaDataService._
import play.api.mvc.Action
import akka.pattern.ask
import actors.Actors
import scala.concurrent.duration._
import akka.util.Timeout
import play.api.libs.concurrent.Execution.Implicits._
import akka.pattern.AskTimeoutException
import akka.pattern.CircuitBreakerOpenException

object UserController extends Controller {

  implicit val userWrites = Json.writes[User]
  implicit val timeout = Timeout(2.seconds)
  import play.api.Play.current

  def get(id: String) = Action.async {
    (Actors.userMetaData ? GetUser(id))
      .mapTo[User]
      .map(user ⇒ Ok(Json.toJson(user)))
      .recover {
        case _: AskTimeoutException ⇒ NotFound
        case _: CircuitBreakerOpenException => ServiceUnavailable
      }
  }

}