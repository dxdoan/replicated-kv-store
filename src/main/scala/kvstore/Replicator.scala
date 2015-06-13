package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  
  case class TimeOut(seq: Long)

  case object ProcessSnapshot
  
  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var senderMap = Map.empty[Long, ActorRef]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  val cancellable = context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, ProcessSnapshot)
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => pending find {_.key.equals(key)} match {
                                              case Some(Snapshot(_, _, prevId)) => senderMap -= prevId
                                              case None =>
                                            }
                                            senderMap += id -> sender

                                            pending = (pending filter {!_.key.equals(key)}) :+ Snapshot(key, valueOption, id)
    case ProcessSnapshot => pending foreach { snapshot =>
                              val seq = nextSeq
                              val cancellable = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, replica, Snapshot(snapshot.key, snapshot.valueOption, seq))
                              acks += seq -> (senderMap.getOrElse(snapshot.seq, self), Replicate(snapshot.key, snapshot.valueOption, snapshot.seq), cancellable)
                              senderMap -= snapshot.seq
                              context.system.scheduler.scheduleOnce(1.second, self, TimeOut(seq))
                            }
    
                            pending = Vector.empty[Snapshot]
                                            
    case SnapshotAck(key, seq) if (acks contains seq) => val (primary, replicate, cancellable) = acks apply seq
                                                         cancellable.cancel()
                                                         acks -= seq
                                                         if (replicate.id != Long.MinValue) primary ! Replicated(replicate.key, replicate.id)
                                            
    case TimeOut(seq) if (acks contains seq) => val (primary, replicate, cancellable) = acks apply seq
                                                cancellable.cancel()
                                                acks -= seq
  }
  
  override def postStop() = if (!cancellable.isCancelled) cancellable.cancel

}
