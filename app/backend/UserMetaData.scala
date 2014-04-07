package backend

import scala.collection.immutable.TreeMap
import scala.concurrent.duration.DurationInt
import GeoFunctions.distanceBetweenPoints
import actors.UserMetaDataService.{ GetUser, User }
import akka.actor.{ Actor, ActorLogging, ActorSelection, Address, Props, RootActorPath }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp, MemberRemoved }
import akka.cluster.Member
import models.backend.UserPosition
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
   * A limited-depth convergent replicated data type for storing the position
   * history of a user. It works based on the assumption that the timestamps
   * of the position updates are consistently produced by that user’s client.
   */
  @SerialVersionUID(1L)
  class PositionHistory private (private val history: TreeMap[Long, LatLng], initialDistance: Double) extends Serializable {
    import GeoFunctions._

    private def calcDistance(h: TreeMap[Long, LatLng], init: Double): Double = {
      h.iterator.drop(1).foldLeft((init, h.head._2)) {
        case ((d, oldPos), (_, newPos)) ⇒ (d + distanceBetweenPoints(oldPos, newPos)) -> newPos
      }._1
    }

    /**
     * Cached latest distance as calculated from the history.
     */
    lazy val latestDistance = calcDistance(history, initialDistance)

    /**
     * The youngest data point’s timestamp.
     */
    def latestTimestamp = history.last._1

    /**
     * Calculate if we are missing that precise timestamp or if our estimate of the distance
     * traveled at that point in time is smaller than the other replica’s data.
     */
    def isMissing(data: UserData): Boolean =
      (!history.contains(data.timestamp) ||
        calcDistance(history.to(data.timestamp), initialDistance) < data.distance)

    /**
     * Merge the given update into this history.
     */
    def merge(position: UserPosition): PositionHistory =
      if (history contains position.timestamp) this
      else if (position.timestamp < history.head._1) this
      else compact(history + (position.timestamp -> position.position))

    /**
     * Merge this history with the given other history.
     */
    def merge(other: PositionHistory): PositionHistory =
      if (other.history.head._1 < history.head._1) other.merge(this)
      else compact(history ++ other.history)

    /**
     * History pruning: construct a new history which contains at most 1000
     * elements, updating the initialDistance if values are discarded.
     */
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
    cluster.unsubscribe(self)
    tick.cancel()
  }

  def receive = {
    /**
     * Query for the given user, reply with UserData.
     */
    case GetUser(id)     ⇒ // TODO
    /**
     * Incoming position update, store in users map.
     */
    case p: UserPosition ⇒ // TODO
    /**
     * Periodic reminder to send out the latest snapshot to our peers.
     */
    case Tick            ⇒ // TODO
    /**
     * Got a snapshot from a peer, check if it contains news and ask for
     * details if so.
     */
    case Snapshot(snap) ⇒
      // TODO
      log.info("got snapshot from {} with size {}", sender, snap.size)
    /**
     * A peer asks for details in response to a Snapshot we sent, so help out!
     */
    case GetDetails(ids) ⇒
      // TODO
      log.info("got request for {} details from {}", ids.size, sender)
    /**
     * Received the response to a GetDetails request, merge into local history.
     */
    case Details(details) ⇒
      // TODO
      log.info("got {} details from {}", details.size, sender)
    /**
     * Keep track of the current cluster members.
     */
    case CurrentClusterState(members, _, _, _, _) ⇒ members foreach peerUp
    case MemberUp(member)                         ⇒ peerUp(member)
    case MemberRemoved(member, _)                 ⇒ peerDown(member)
  }

  /**
   * Transform the users map into a vector of UserData(id, timestamp, distance)
   */
  private def createSnapshot(): Vector[UserData] = ??? // TODO

  /**
   * Calculate which UserData in the given Vector contain knowledge that we do
   * not have already and return the set of user IDs for which we need to ask
   * for details accordingly.
   */
  private def getDeficits(snap: Vector[UserData]): Set[String] = ??? // TODO

  /**
   * Send an initial Snapshot(...) message to the other member if that member
   * has the right role (UserMetaData.clusterRole) and has not cluster.selfAddress,
   * and add the actor selection used for this purpose to the peers variable.
   */
  private def peerUp(member: Member): Unit = ??? // TODO
  
  /**
   * Remove the given member from the peers variable.
   */
  private def peerDown(member: Member): Unit = ??? // TODO

  /**
   * Merge the two given user maps and return the result.
   */
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