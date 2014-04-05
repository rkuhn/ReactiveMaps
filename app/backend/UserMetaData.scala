package backend

import scala.collection.immutable.TreeSet

import GeoFunctions.distanceBetweenPoints
import actors.UserMetaDataService.{ GetUser, User }
import akka.actor.{ Actor, ActorLogging }
import akka.actor.{ Props, RootActorPath, actorRef2Scala }
import akka.actor.ActorSelection.toScala
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import akka.cluster.Member
import models.backend.UserPosition

object UserMetaData {
  val clusterRole = "meta-data"
  val actorName = "userMetaData"

  /*
   * Internal messages for starting-up actors looking for their peers’ state.
   */
  type Users = Map[String, PositionHistory]

  private case object GetSnapshot
  private case class Snapshot(users: Users)

  val props = Props[UserMetaData]

  /**
   * A limited-depth commutative replicated data type for storing the position
   * history of a user.
   */
  @SerialVersionUID(1L)
  class PositionHistory private (private val history: TreeSet[UserPosition], initialDistance: Double) extends Serializable {
    import GeoFunctions._
    import userPositionOrder.mkOrderingOps

    private val aggregateDistance: ((Double, UserPosition), UserPosition) ⇒ (Double, UserPosition) = {
      case ((d, oldPos), newPos) ⇒ (d + distanceBetweenPoints(oldPos.position, newPos.position)) -> newPos
    }

    lazy val (latestDistance, _) = history.iterator.drop(1).foldLeft((initialDistance, history.head))(aggregateDistance)

    def merge(position: UserPosition): PositionHistory =
      if (history contains position) this
      else if (position < history.head) this
      else {
        val size = history.size
        if (size > 999) {
          val merged = history + position
          val (d, _) = merged.iterator.drop(1).take(size - 999).foldLeft((initialDistance, history.head))(aggregateDistance)
          new PositionHistory(merged.drop(size - 999), d)
        } else new PositionHistory(history + position, initialDistance)
      }

    def merge(other: PositionHistory): PositionHistory =
      if (other.history.head < history.head) other.merge(this)
      else {
        val merged = history ++ other.history
        val size = merged.size
        if (size > 999) {
          val (d, _) = merged.iterator.drop(1).take(size - 999).foldLeft((initialDistance, history.head))(aggregateDistance)
          new PositionHistory(merged.drop(size - 999), d)
        } else new PositionHistory(merged, initialDistance)
      }
  }

  object PositionHistory {
    def apply(position: UserPosition): PositionHistory = new PositionHistory(TreeSet(position), 0d)
  }

  implicit val userPositionOrder: Ordering[UserPosition] = Ordering.by(_.timestamp)
}

class UserMetaData extends Actor with ActorLogging {

  import UserMetaData._
  import actors.UserMetaDataService._

  var users: Users = Map[String, PositionHistory]()

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[MemberUp])

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case GetUser(id) ⇒
      users get id match {
        case Some(history) ⇒ sender ! User(id, history.latestDistance)
        case None          ⇒ // nothing to send
      }
    case p: UserPosition ⇒
      val newHistory = users get p.id match {
        case Some(history) ⇒ history.merge(p)
        case None          ⇒ PositionHistory(p)
      }
      users += p.id -> newHistory
    case GetSnapshot ⇒
      log.info("got snapshot request from {}", sender)
      sender ! Snapshot(users)
    case Snapshot(snap) ⇒
      log.info("got snapshot from {} with size {}", sender, snap.size)
      users = merge(snap, users)
    case CurrentClusterState(members, _, _, _, _) ⇒ members foreach peerUp
    case MemberUp(member)                         ⇒ peerUp(member)
  }

  private def peerUp(member: Member): Unit =
    if (member.address != cluster.selfAddress && member.hasRole(clusterRole)) {
      val other = context.actorSelection(RootActorPath(member.address) / "user" / actorName)
      log.info("initial chat with {}", other)
      other ! GetSnapshot
      other ! Snapshot(users)
    }

  private def merge(left: Users, right: Users): Users =
    if (left.size < right.size) merge(right, left)
    else {
      right.foldLeft(left) {
        case (users, pair @ (id, history)) ⇒
          if (users contains id) {
            users + (id -> users(id).merge(history))
          } else users + pair
      }
    }

}