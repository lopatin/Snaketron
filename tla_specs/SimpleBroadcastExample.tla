---------------------------- MODULE SimpleBroadcastExample ----------------------------
\* Simple example using BroadcastChannel for an event system

EXTENDS BroadcastChannel, Integers, Sequences, FiniteSets, TLC

CONSTANTS 
    MaxEvents           \* Maximum number of events to generate

\* Override Messages to be simple integers representing event IDs
Messages == 1..MaxEvents

VARIABLES
    eventCount,         \* Counter for generated events
    consumed            \* Events consumed by each receiver

\* Combine all variables
vars == <<channel, receiverPositions, eventCount, consumed>>

Init ==
    /\ InitBroadcastChannel
    /\ eventCount = 0
    /\ consumed = [r \in ReceiverSet |-> {}]

\* Producer publishes an event
PublishEvent ==
    /\ eventCount < MaxEvents
    /\ Send(eventCount + 1)
    /\ eventCount' = eventCount + 1
    /\ UNCHANGED consumed

\* Consumer processes next event
ConsumeEvent(receiver) ==
    /\ HasMessages(receiver)
    /\ LET msg == PeekMessage(receiver)
       IN /\ Receive(receiver)
          /\ consumed' = [consumed EXCEPT ![receiver] = @ \union {msg}]
    /\ UNCHANGED eventCount

\* Garbage collection
Cleanup ==
    /\ GarbageCollect
    /\ UNCHANGED <<eventCount, consumed>>

Next ==
    \/ PublishEvent
    \/ \E r \in ReceiverSet: ConsumeEvent(r)
    \/ Cleanup

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants

TypeOK ==
    /\ TypeInvariant  \* From BroadcastChannel
    /\ eventCount \in 0..MaxEvents
    /\ consumed \in [ReceiverSet -> SUBSET (1..MaxEvents)]

\* Each receiver consumes events in order
OrderedConsumption ==
    \A r \in ReceiverSet: \A e1, e2 \in consumed[r]:
        e1 < e2 => \A e3 \in 1..MaxEvents: 
            (e1 < e3 /\ e3 < e2) => e3 \in consumed[r]

\* No receiver consumes the same event twice (implicit from set)
\* But let's verify the size matches unique events
UniqueConsumption ==
    \A r \in ReceiverSet: Cardinality(consumed[r]) = receiverPositions[r]

\* Channel never exceeds capacity
ChannelBounded ==
    Len(channel) <= ChannelCapacity

\* All receivers are independent
ReceiverIndependence ==
    \A r1, r2 \in ReceiverSet: r1 # r2 =>
        receiverPositions[r1] <= Len(channel) /\ 
        receiverPositions[r2] <= Len(channel)

=============================================================================