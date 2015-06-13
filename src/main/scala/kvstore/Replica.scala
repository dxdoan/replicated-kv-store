package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.Cancellable

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // a map from the id of the currently being processed update to the state of the update
  var updateState = Map.empty[Long, (ActorRef, Cancellable, Set[ActorRef])]
  // seq no to communicate with the replicator as a secondary
  var expectedSeq = 0L
  
  var processing = false
  var cancellable: Cancellable = null
  var replicator: ActorRef = null

  val persistence = context.actorOf(persistenceProps)
  
  arbiter ! Join
  
  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart
  }
  
  def initiateUpdateByLeader(key: String, valueOption: Option[String], id: Long) = {
    val persistenceCancellable = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, persistence, Persist(key, valueOption, id))
    
    replicators.foreach(_ ! Replicate(key, valueOption, id))
    updateState += id -> (sender, persistenceCancellable, replicators)
    
    context.system.scheduler.scheduleOnce(1.second, self, TimeOut(id))
  }
  
  def processUpdateState(id: Long, replicator: ActorRef) = {
    var (client, persistenceCancellable, undoneReplicators) = updateState apply id
    
    // called from Persisted msg
    if (replicator == null) {
      if (!persistenceCancellable.isCancelled) persistenceCancellable.cancel
    } else { // called from Replicated msg
      undoneReplicators -= replicator
      updateState += id -> (client, persistenceCancellable, undoneReplicators)
    }
    
    if (persistenceCancellable.isCancelled && undoneReplicators.isEmpty) {
      updateState -= id
      client ! OperationAck(id)
    }
  }
  
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => kv += key -> value
                                   initiateUpdateByLeader(key, Some(value), id)
    case Remove(key, id) => kv -= key
                            initiateUpdateByLeader(key, None, id)
    case Get(key, id) => sender ! GetResult(key, kv get key, id)
    case Persisted(key, id) if (updateState contains id) => processUpdateState(id, null)
    case Replicated(key, id) if (updateState contains id) => processUpdateState(id, sender)
    case TimeOut(id) if (updateState contains id) => val (client, persistenceCancellable, undoneReplicators) = updateState apply id
                                                     if (!persistenceCancellable.isCancelled) persistenceCancellable.cancel
                                                     updateState -= id
                                                     client ! OperationFailed(id)
    case Replicas(replicas) => val currSecondaries = secondaries.keySet
                               val oldReplicas = currSecondaries &~ replicas
                               val oldReplicators = (secondaries.filterKeys(oldReplicas contains _) map {case(key, value) => value}).toSet
                               
                               val newReplicas = (replicas &~ currSecondaries) - self
                               var newReplicators = Set.empty[ActorRef]
                               var newReplicasMap = Map.empty[ActorRef, ActorRef]
                               newReplicas foreach { newReplica =>
                                 val newReplicator = context.actorOf(Props[Replicator](new Replicator(newReplica)))
                                 newReplicators += newReplicator
                                 newReplicasMap += newReplica -> newReplicator
                               }
                               
                               newReplicators.foreach{ newReplicator => kv.foreach{case (key, value) => newReplicator ! Replicate(key, Some(value), Long.MinValue) } }
                               oldReplicators.foreach(_ ! PoisonPill)
                               
                               updateState foreach {
                                 case (id, (client, persistenceCancellable, undoneReplicators)) => 
                                   val remainingReplicators = undoneReplicators &~ oldReplicators
                                   if (persistenceCancellable.isCancelled && remainingReplicators.isEmpty) {
                                     updateState -= id
                                     client ! OperationAck(id)
                                   } else updateState += id -> (client, persistenceCancellable, remainingReplicators)
                               }
                               
                               secondaries = secondaries.filterKeys(!oldReplicas.contains(_)) ++ newReplicasMap
                               replicators = replicators ++ newReplicators &~ oldReplicators
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv get key, id)
    case Snapshot(key, valueOption, seq) => if (seq > expectedSeq) ()
                                            else if (seq < expectedSeq) sender ! SnapshotAck(key, seq)
                                            else if (!processing) {
                                              processing = true
                                              replicator = sender
                                              // Update the local kv store
                                              valueOption match {
                                                case Some(value) => kv += key -> value
                                                case None => kv -= key
                                              }
                                              
                                              // Persist the update
                                              cancellable = context.system.scheduler.schedule(0.milliseconds, 100.milliseconds, persistence, Persist(key, valueOption, seq))
                                              context.system.scheduler.scheduleOnce(1.second, self, TimeOut(seq))
                                            }
    case Persisted(key, seq) if (seq == expectedSeq) => processing = false
                                                        expectedSeq += 1
                                                        if (cancellable != null && !cancellable.isCancelled) cancellable.cancel
                                                        replicator ! SnapshotAck(key, seq)
                                                        replicator = null
    case TimeOut(seq) if (seq == expectedSeq) => processing = false
                                                 expectedSeq += 1
                                                 if (cancellable != null && !cancellable.isCancelled) cancellable.cancel
                                                 replicator = null
                                
  }

}

