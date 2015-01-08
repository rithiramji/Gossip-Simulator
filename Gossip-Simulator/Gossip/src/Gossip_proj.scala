

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.math._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.RoundRobinRouter
import scala.util.Random
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.concurrent.Future
import akka.pattern.ask
import akka.actor.ActorContext
import akka.actor.Cancellable




object Topology {
def createTopology(nodenumber:Int,no_of_nodes:Int,topology:String) : ArrayBuffer[Int] = {
  var neighbors:ArrayBuffer[Int]=ArrayBuffer[Int]()
  if(topology=="full")
    for( n <- 0 until no_of_nodes)
    {
     if(nodenumber != n)
       neighbors+=n
    }
  else if(topology=="grid" || topology=="imperfect")
  {
    var dim:Int=sqrt(no_of_nodes).toInt
    if((nodenumber-dim)>=0)
      neighbors+=(nodenumber-dim)
    if((nodenumber+dim)<no_of_nodes)
      neighbors+=(nodenumber+dim)
    if((((nodenumber-1)%dim) != (dim-1))&& (nodenumber-1)>=0)
      neighbors+=(nodenumber-1)
    if(((nodenumber+1)%dim)!=0)
      neighbors+=(nodenumber+1)
     if(topology=="imperfect")
     {
       var rand:Int=Random.nextInt(no_of_nodes)
       while(neighbors.contains(rand))
         rand=Random.nextInt(no_of_nodes)
       neighbors+=rand
     }
  } 
  else if(topology=="line")
  {
    if((nodenumber-1)>=0)
      neighbors+=(nodenumber-1)
    if((nodenumber+1)<no_of_nodes)
      neighbors+=(nodenumber+1)
  }

  return neighbors   
}
  
}

case class pushSumStatus(workernumber:Int)
case class passStopList(stopList:ArrayBuffer[Int])
case class startGossip(message:String,numWorkers:Integer,topology:String,node:ActorRef)
case class ReceiveGossip(message:String,workernumber:Integer,topology:String,node:List[ActorRef],no_of_workers:Int,statusnode:ActorRef)
case class Status(workernumber:Integer,no_of_messages:Integer)
case class BeginGossip(no_of_workers:Integer,topology:String,node:List[ActorRef],message:String,statusnode:ActorRef)
case class startPushSum(no_of_nodes:Int,topology:String,statusnode:ActorRef)
case class BeginPushSum(no_of_workers:Int,topology:String,node:List[ActorRef],statusnode:ActorRef)
case class ReceivePushSum(workernumber:Int,topology:String,node:List[ActorRef],no_of_workers:Int,statusnode:ActorRef,sum:Double,weight:Double)
class Node extends Actor {
  
   var messagereceived=0 
   var gossipflag=false
   var pushsumflag=false
   var past_sum:ArrayBuffer[Double]=ArrayBuffer[Double]()
   var past_weight:ArrayBuffer[Double]=ArrayBuffer[Double]()
   var sum_ratio:ArrayBuffer[Double]=ArrayBuffer[Double]()  
   var neighbors=ArrayBuffer[Int]()
   val system = ActorSystem("Node")
   var s:Double = 0
   var w:Double = 1
   def chooseNeighbor(nodeNumber:Int,no_of_workers:Int,topology:String,node:List[ActorRef],message:String,statusnode:ActorRef,choice:Int)
   {
     if(neighbors.length==0)
     neighbors=Topology.createTopology(nodeNumber,no_of_workers,topology)
     var rand=Random.nextInt(neighbors.length)
     if(choice == 1)
     {
       node(neighbors(rand)) ! ReceiveGossip(message,neighbors(rand),topology,node,no_of_workers,statusnode)
     }
     else if(choice == 2)  
     {
       var parts = message.split(";")  
       if(parts.length>1)
       {
       var s=parts(0).toDouble
       var w=parts(1).toDouble
       }
       else
       {
         var s=0 
         var w=0
       }

       node(neighbors(rand)) ! ReceivePushSum(neighbors(rand),topology,node,no_of_workers,statusnode,s,w)
   
     }
   }
   def receive: PartialFunction[Any,Unit] = {
     case BeginGossip(no_of_workers,topology,node,message,statusnode) => {
       messagereceived+=1
      
      
       statusnode ! Status(0,messagereceived)
       gossipflag=true
       system.scheduler.schedule(0 seconds,0.005 seconds)(chooseNeighbor(0,no_of_workers,topology,node,message,statusnode,1))
     }
     case BeginPushSum(no_of_workers,topology,node,statusnode) => {
       var message = " "
       system.scheduler.schedule(0 seconds,0.005 seconds)(chooseNeighbor(0,no_of_workers,topology,node,message,statusnode,2))
     }
     case ReceiveGossip(message,workernumber,topology,node,no_of_workers,statusnode) => {
       messagereceived+=1
      if(messagereceived==10)
       {
        context.stop(self)
       }
       if(!gossipflag)
         statusnode ! Status(workernumber,messagereceived)
       gossipflag=true
       system.scheduler.schedule(0 seconds,0.005 seconds)(chooseNeighbor(workernumber,no_of_workers,topology,node,message,statusnode,1))
         
     }
     case ReceivePushSum(workernumber,topology,node,no_of_workers,statusnode,sum,weight) => {
        if(!pushsumflag)
        {
          s=workernumber
          pushsumflag=true
        }
        else
        {
          s=s+sum
          w=w+weight
        }
        s=s/2
        w=w/2
        past_sum += s
        past_weight += w
        var ratio=(s/w)
        sum_ratio+=ratio
        var i:Integer = 0
        if(sum_ratio.length == 3)
        {
            var diff1=sum_ratio(2) - sum_ratio(1)
            var diff2=sum_ratio(1) - sum_ratio(0)
            if( (abs(diff1) < pow(10,-10)) && (abs(diff2) < pow(10,-10)))     
            {
              println("convergence at node "+workernumber+" value= "+sum_ratio(0))
              statusnode ! pushSumStatus(workernumber)
              context.stop(self)
            }
 
         sum_ratio.remove(0)
         past_sum.remove(0)
         past_weight.remove(0)  
        }
        var message=s.toString()+";"+w.toString()
        system.scheduler.schedule(0 seconds,0.005 seconds)(chooseNeighbor(workernumber,no_of_workers,topology,node,message,statusnode,2)) 
     }
     
   }
   } 
   class statusnode extends Actor
   {
     var finished_count:Integer=0
     var startTime=System.currentTimeMillis
     var no_of_workers=0
     var node:List[ActorRef] = Nil
     def init(topology:String,no_of_nodes:Int) = {
      val system = ActorSystem("Node")
      no_of_workers=no_of_nodes
      if(topology=="grid" || topology=="imperfect"){
        var dim=sqrt(no_of_workers.toDouble)
        while((dim%1)!=0)
           {
             no_of_workers+=1
             dim=sqrt(no_of_workers.toDouble)  
           }
        println("num of workers "+no_of_workers)
       }
      startTime = System.currentTimeMillis 
      var i:Int = 0
      while(i<no_of_workers){ 
        node ::= system.actorOf(Props[Node])
        i=i+1
      }       
     }
     def receive = {
     case startGossip(message,no_of_nodes,topology,statusnode) => {
        init(topology,no_of_nodes)
        node(0) ! BeginGossip(no_of_workers,topology,node,message,statusnode) 
     }
     case Status(workernumber,messagereceived) => {
       finished_count+=1
       println("finished count"+finished_count+ " worker no "+workernumber)
       if(finished_count==no_of_workers){
         println("Time taken "+(System.currentTimeMillis-startTime))   
         context.system.shutdown()
      
       }   
     }
     case startPushSum(no_of_nodes,topology,statusnode) => { 
       init(topology,no_of_nodes)
       node(0)!BeginPushSum(no_of_workers,topology,node,statusnode)
     }
     case pushSumStatus(workernumber) => {
       finished_count+=1
       println("finished count"+finished_count+ " worker no "+workernumber)
       if(finished_count==no_of_workers){
         println("Time taken "+(System.currentTimeMillis-startTime))   
       context.system.shutdown()
       }         
     }
      
     }    
   }

   




object Gossip_proj extends App {
  
  val system = ActorSystem("node")
  var no_of_nodes=args(0).toInt
  var topology=args(1)
  var algorithm=args(2)
  val statusnode = system.actorOf(Props[statusnode],name="statusnode")
  if(args(2)=="gossip"){
  statusnode ! startGossip("gossip message",no_of_nodes,topology,statusnode)  
  }
  else{
  statusnode ! startPushSum(no_of_nodes,topology,statusnode)  
  }
}