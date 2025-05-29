---------------------------- MODULE BroadcastChannel ----------------------------
\* A reusable TLA+ module modeling a Rust-style broadcast channel
\* Multiple receivers can independently consume messages from a shared channel

EXTENDS Sequences, Integers, FiniteSets, TLC

CONSTANTS 
    ChannelCapacity,     \* Maximum number of messages in channel
    ReceiverSet          \* Set of receiver identifiers

\* Channel state: sequence of messages
\* ReceiverPositions: function mapping each receiver to their position in the channel
VARIABLES 
    channel,             \* The broadcast channel buffer
    receiverPositions    \* Position of each receiver in the channel

\* Helper operators
TypeInvariant ==
    /\ channel \in Seq(Messages)
    /\ Len(channel) <= ChannelCapacity
    /\ receiverPositions \in [ReceiverSet -> Nat]
    /\ \A r \in ReceiverSet: receiverPositions[r] <= Len(channel)

\* Initialize with empty channel and all receivers at position 0
InitBroadcastChannel ==
    /\ channel = <<>>
    /\ receiverPositions = [r \in ReceiverSet |-> 0]

\* Send a message to the channel (returns TRUE if successful, FALSE if full)
Send(msg) ==
    /\ Len(channel) < ChannelCapacity
    /\ channel' = Append(channel, msg)
    /\ UNCHANGED receiverPositions

\* Check if a receiver has messages available
HasMessages(receiver) ==
    receiverPositions[receiver] < Len(channel)

\* Receive next message for a specific receiver
Receive(receiver) ==
    /\ HasMessages(receiver)
    /\ receiverPositions' = [receiverPositions EXCEPT ![receiver] = @ + 1]
    /\ UNCHANGED channel

\* Get the next message for a receiver (without consuming)
PeekMessage(receiver) ==
    IF HasMessages(receiver) 
    THEN channel[receiverPositions[receiver] + 1]
    ELSE "NoMessage"

\* Get all unconsumed messages for a receiver
UnconsumedMessages(receiver) ==
    SubSeq(channel, receiverPositions[receiver] + 1, Len(channel))

\* Number of messages behind for each receiver
MessageBacklog(receiver) ==
    Len(channel) - receiverPositions[receiver]

\* Check if all receivers have consumed all messages
AllConsumed ==
    \A r \in ReceiverSet: receiverPositions[r] = Len(channel)

\* Garbage collect: remove messages that all receivers have consumed
\* This models Rust broadcast channel's automatic cleanup
GarbageCollect ==
    LET minPos == CHOOSE pos \in Nat : 
        /\ \A r \in ReceiverSet: receiverPositions[r] >= pos
        /\ \A p \in Nat: (\A r \in ReceiverSet: receiverPositions[r] >= p) => p <= pos
    IN /\ minPos > 0
       /\ channel' = SubSeq(channel, minPos + 1, Len(channel))
       /\ receiverPositions' = [r \in ReceiverSet |-> receiverPositions[r] - minPos]

\* Useful properties
NoMessageLoss ==
    \* Every receiver can eventually consume every message sent
    \A r \in ReceiverSet: \A i \in 1..Len(channel):
        receiverPositions[r] < i => receiverPositions[r] < Len(channel)

IndependentReceivers ==
    \* One receiver's position doesn't affect another's ability to receive
    \A r1, r2 \in ReceiverSet: r1 # r2 =>
        HasMessages(r1) \/ ~HasMessages(r2) \/ receiverPositions[r1] = receiverPositions[r2]

\* For use in other specs - define Messages set
Messages == 1..10  \* Default message set, can be overridden

=============================================================================