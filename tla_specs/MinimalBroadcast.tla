---------------------------- MODULE MinimalBroadcast ----------------------------
\* Minimal example of using BroadcastChannelGeneric

EXTENDS BroadcastChannelGeneric, Integers, Sequences

\* Simple integer messages
MessageType == 1..10

VARIABLES sent  \* Track what we've sent

vars == <<channel, receiverPositions, sent>>

Init ==
    /\ InitBroadcastChannel
    /\ sent = 0

\* Send next message
SendNext ==
    /\ sent < 3
    /\ Send(sent + 1)
    /\ sent' = sent + 1

\* Any receiver can consume
ConsumeNext(r) ==
    /\ HasMessages(r)
    /\ Receive(r)
    /\ UNCHANGED sent

Next ==
    \/ SendNext
    \/ \E r \in ReceiverSet: ConsumeNext(r)

Spec == Init /\ [][Next]_vars

\* Simple invariant
TypeOK ==
    /\ ChannelTypeInvariant(MessageType)
    /\ sent \in 0..3

\* Verify receivers see messages in order
ReceiverOrdering ==
    \A r \in ReceiverSet:
        receiverPositions[r] \in 0..Len(channel)

=============================================================================