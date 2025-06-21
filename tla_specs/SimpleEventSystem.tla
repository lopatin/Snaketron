---------------------------- MODULE SimpleEventSystem ----------------------------
\* Example event-driven system using BroadcastChannelGeneric

EXTENDS BroadcastChannelGeneric, Integers, Sequences, FiniteSets, TLC

CONSTANTS 
    MaxEvents           \* Maximum number of events to generate

\* Define our message type - simple event records
EventType == [type: {"Login", "Logout", "Order"}, id: 1..MaxEvents]

VARIABLES
    eventCount,         \* Counter for generated events
    consumed            \* Events consumed by each receiver

\* Combine all variables
vars == <<channel, receiverPositions, eventCount, consumed>>

Init ==
    /\ InitBroadcastChannel
    /\ eventCount = 0
    /\ consumed = [r \in ReceiverSet |-> {}]

\* Producer publishes an event!
PublishEvent(eventType) ==
    /\ eventCount < MaxEvents
    /\ Send([type |-> eventType, id |-> eventCount + 1])
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
    \/ \E et \in {"Login", "Logout", "Order"}: PublishEvent(et)
    \/ \E r \in ReceiverSet: ConsumeEvent(r)
    \/ Cleanup
    \/ eventCount = MaxEvents /\ AllConsumed /\ UNCHANGED vars  \* Allow stuttering when done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants

TypeOK ==
    /\ ChannelTypeInvariant(EventType)
    /\ eventCount \in 0..MaxEvents
    /\ consumed \in [ReceiverSet -> SUBSET EventType]

\* Each receiver consumes events in order
OrderedConsumption ==
    \A r \in ReceiverSet: \A e1, e2 \in consumed[r]:
        e1.id < e2.id => \A e3 \in consumed[r]: 
            (e1.id < e3.id /\ e3.id < e2.id) => e3 \in consumed[r]

\* No receiver consumes duplicate events
UniqueConsumption ==
    \A r \in ReceiverSet: 
        \A e1, e2 \in consumed[r]: e1 = e2 \/ e1.id # e2.id

\* Channel respects capacity
ChannelBounded ==
    Len(channel) <= ChannelCapacity
!i
\* All published events exist in channel or have been consumed
EventAccountability ==
    \A i \in 1..eventCount:
        \/ \E j \in 1..Len(channel): channel[j].id = i
        \/ \E r \in ReceiverSet: \E e \in consumed[r]: e.id = i

\* Receivers process events independently
ReceiverProgress ==
    \A r1, r2 \in ReceiverSet: 
        receiverPositions[r1] # receiverPositions[r2] \/ r1 = r2

=============================================================================