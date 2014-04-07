package backend

import scala.collection.immutable.TreeSet
import GeoFunctions.distanceBetweenPoints
import actors.UserMetaDataService.{ GetUser, User }
import akka.actor.{ Actor, ActorLogging, ActorSelection, Address, Props, RootActorPath }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp, MemberRemoved }
import akka.cluster.Member
import models.backend.UserPosition
import scala.concurrent.duration._
import akka.actor.Address
import akka.actor.ActorSelection
import scala.collection.immutable.TreeMap
import play.extras.geojson.LatLng

object UserMetaData {
  val clusterRole = "meta-data"
  val actorName = "userMetaData"

  /*
   * Internal messages for starting-up actors looking for their peers’ state.
   */
  type Users = Map[String, PositionHistory]

  private case class UserData(id: String, timestamp: Long, distance: Double)

  private case class Snapshot(current: Vector[UserData])
  private case class GetDetails(ids: Set[String])
  private case class Details(users: Users)

  val props = Props[UserMetaData]

  /**
   * A limited-depth commutative replicated data type for storing the position
   * history of a user.
   */
  @SerialVersionUID(1L)
  class PositionHistory private (private val history: TreeMap[Long, LatLng], initialDistance: Double) extends Serializable {
    import GeoFunctions._

    private def calcDistance(h: TreeMap[Long, LatLng], init: Double): Double = {
      h.iterator.drop(1).foldLeft((init, h.head._2)) {
        case ((d, oldPos), (_, newPos)) ⇒ (d + distanceBetweenPoints(oldPos, newPos)) -> newPos
      }._1
    }

    lazy val latestDistance = calcDistance(history, initialDistance)
    def latestTimestamp = history.last._1

    def isMissing(data: UserData): Boolean =
      (!history.contains(data.timestamp) ||
        calcDistance(history.to(data.timestamp), initialDistance) < data.distance)

    def merge(position: UserPosition): PositionHistory =
      if (history contains position.timestamp) this
      else if (position.timestamp < history.head._1) this
      else compact(history + (position.timestamp -> position.position))

    def merge(other: PositionHistory): PositionHistory =
      if (other.history.head._1 < history.head._1) other.merge(this)
      else compact(history ++ other.history)

    private def compact(merged: TreeMap[Long, LatLng]): PositionHistory = {
      val size = merged.size
      if (size > 999) {
        val toRemove = size - 900
        val d = calcDistance(merged.take(toRemove + 1), initialDistance)
        new PositionHistory(merged.drop(toRemove), d)
      } else new PositionHistory(merged, initialDistance)
    }
  }

  object PositionHistory {
    def apply(position: UserPosition): PositionHistory = new PositionHistory(TreeMap(position.timestamp -> position.position), 0d)
  }
}

class UserMetaData extends Actor with ActorLogging {

  import UserMetaData._
  import actors.UserMetaDataService._

  var users: Users = Map[String, PositionHistory]()

  val cluster = Cluster(context.system)
  cluster.subscribe(self, classOf[MemberUp])
  cluster.subscribe(self, classOf[MemberRemoved])

  import context.dispatcher
  case object Tick
  val tick = context.system.scheduler.schedule(5.seconds, 5.seconds, self, Tick)
  var peers = Map[Address, ActorSelection]()

  override def postStop(): Unit = {
    tick.cancel()
    cluster.unsubscribe(self)
  }

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
    case Tick ⇒
      val snapshot = Snapshot(createSnapshot())
      log.info("sending snapshot of size {} to {} peers", snapshot.current.size, peers.size)
      peers.valuesIterator foreach (_ ! snapshot)
    case Snapshot(snap) ⇒
      log.info("got snapshot from {} with size {}", sender, snap.size)
      val deficits = getDeficits(snap)
      if (deficits.nonEmpty) {
        log.info("sending request for {} details", deficits.size)
        sender ! GetDetails(deficits)
      }
    case GetDetails(ids) ⇒
      log.info("got request for {} details from {}", ids.size, sender)
      sender ! Details(users.filter(u ⇒ ids.contains(u._1)))
    case Details(details) ⇒
      log.info("got {} details from {}", details.size, sender)
      users = merge(users, details)
    case CurrentClusterState(members, _, _, _, _) ⇒ members foreach peerUp
    case MemberUp(member)                         ⇒ peerUp(member)
    case MemberRemoved(member, _)                 => peerDown(member)
  }

  private def createSnapshot(): Vector[UserData] = users.map {
    case (id, history) ⇒ UserData(id, history.latestTimestamp, history.latestDistance)
  }(collection.breakOut)

  private def getDeficits(snap: Vector[UserData]): Set[String] = snap.flatMap(userData ⇒
    users get userData.id match {
      case Some(history) if !history.isMissing(userData) ⇒ None
      case _ ⇒ Some(userData.id)
    })(collection.breakOut)

  private def peerUp(member: Member): Unit =
    if (member.address != cluster.selfAddress && member.hasRole(clusterRole)) {
      val other = context.actorSelection(RootActorPath(member.address) / "user" / actorName)
      log.info("initial chat with {}", other)
      other ! Snapshot(createSnapshot())
      peers += member.address -> other
    }

  private def peerDown(member: Member): Unit = peers -= member.address

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