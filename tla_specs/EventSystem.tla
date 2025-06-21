---------------------------- MODULE EventSystem ----------------------------
\* Example event-driven system using BroadcastChannel
\* Models a simple publish-subscribe system with event producers and consumers

EXTENDS BroadcastChannel, Integers, Sequences, FiniteSets, TLC

CONSTANTS 
    Producers,           \* Set of event producers
    MaxEvents           \* Maximum number of events to generate

VARIABLES
    eventCount,         \* Counter for generated events
    processedEvents     \* Set of events processed by each receiver

\* Combine variables
vars == <<channel, receiverPositions, eventCount, processedEvents>>

\* Event types
Events == {"UserLogin", "UserLogout", "OrderPlaced", "PaymentReceived"}

\* Override Messages from BroadcastChannel  
Messages == [type: Events, id: 1..MaxEvents]

Init ==
    /\ InitBroadcastChannel
    /\ eventCount = 0
    /\ processedEvents = [r \in ReceiverSet |-> {}]

\* Producer publishes an event
PublishEvent(producer, eventType) ==
    /\ eventCount < MaxEvents
    /\ Send(<<eventType, eventCount + 1>>)
    /\ eventCount' = eventCount + 1
    /\ UNCHANGED processedEvents

\* Consumer processes next event in their queue
ConsumeEvent(receiver) ==
    /\ HasMessages(receiver)
    /\ LET msg == PeekMessage(receiver)
       IN /\ Receive(receiver)
          /\ processedEvents' = [processedEvents EXCEPT 
                ![receiver] = @ \union {msg}]
    /\ UNCHANGED eventCount

\* System performs garbage collection
CleanupOldEvents ==
    /\ GarbageCollect
    /\ UNCHANGED <<eventCount, processedEvents>>

Next ==
    \/ \E p \in Producers, e \in Events: PublishEvent(p, e)
    \/ \E r \in ReceiverSet: ConsumeEvent(r)
    \/ CleanupOldEvents

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Properties and Invariants

TypeOK ==
    /\ TypeInvariant  \* From BroadcastChannel
    /\ eventCount \in 0..MaxEvents
    /\ processedEvents \in [ReceiverSet -> SUBSET Messages]

\* All published events are eventually available to all receivers
EventualDelivery ==
    \A r \in ReceiverSet: \A i \in 1..eventCount:
        \E msg \in Messages: 
            /\ msg[2] = i  \* Event with ID i
            /\ (receiverPositions[r] < i => msg \in UnconsumedMessages(r))

\* No receiver processes the same event twice
NoDuplicateProcessing ==
    \A r \in ReceiverSet: \A e1, e2 \in processedEvents[r]:
        e1 = e2 \/ e1[2] # e2[2]

\* Events are processed in order by each receiver
OrderedProcessing ==
    \A r \in ReceiverSet: \A e1, e2 \in processedEvents[r]:
        e1[2] < e2[2] => 
            \A e3 \in Messages: e3[2] > e1[2] /\ e3[2] < e2[2] => 
                e3 \in processedEvents[r]

\* Liveness: All events are eventually processed by all receivers
EventuallyAllProcessed ==
    <>[](eventCount = MaxEvents => 
        \A r \in ReceiverSet: Cardinality(processedEvents[r]) = eventCount)

=============================================================================