# goFBP

A simple Flow Based Programming framework built on go. It is used to create and execute Networks. goFBP is a new, standalone library rather than an extension of or successor to natsFBP. It uses Go Channels as transport.

## Language

### Structure
**Port**:
A named point through which packets enter or leave a brick or network; either an inport or an outport.

**Inport**:
A port that receives packets.

**Outport**:
A port that sends packets out.

**Brick**:
A reusable unit of business logic that has one Inport and at least 2 Outports. It receives data from its Inport, processes it, and then sends data out through its Outports. It does not know its neighbours. The default Outports are Success and Error. It has a name and a description. The name need not be unique: within a Network, Nodes are told apart by their node name.
_Avoid_: Component, Block, Operator, Baustein

**Connection**:
One Outport can be connected to one or many Inports, so every time an Outport sends data, all connected Inports receive the data (Fan-out). If the Outport has no connection, no data is sent. The same applies to each Inport of a Network: it can be connected to the Inports of one or many of its Nodes, and each of them receives the data. A Connection to a nested Network names the Inport of that Network it leads to; a Connection to a Brick always leads to its only Inport. Connections must not form a cycle, neither within one Network nor across nested Networks.

**Packet**:
A single unit of data travelling along a connection from one port to the next, made of JSON data plus metadata that identifies its run.
_Avoid_: Message, Information Packet, IP

**Network**:
A Network has at least 1 Inport and at least 2 Outports. The default Inport is In; additional Inports can be added. Every Inport must be connected. The default Outports are Success and Error. It has a name and a description; the name is unique within its package path (see Service Subject structure). It defines a graph whose nodes can be a Brick or another Network, linked together by Connections. A Network must not contain itself as a Node, neither directly nor indirectly through a nested Network.
_Avoid_: Graph, Flow, Pipeline

**Node**:
In a Network, every Brick and nested Network has a node name that identifies the Node within the Network. If the same Brick or Network appears more than once in a Network, each occurrence needs a different node name.

**Concurrency**:
A property of a Brick that sets how many copies of it process packets in parallel within one Instance. All copies read from the same Inport, so each packet is processed by exactly one copy. The default is 1. With a Concurrency greater than 1, packets may leave the Node in a different order than they arrived, even within the same Run.
_Avoid_: Worker, Replica, Parallelism

**Run**:
Everything that results from a packet entering one of a Network's Inports, across all nodes and nesting levels. If a Packet enters Network without a run ID, it creates one. Packets that arrive carrying an existing run ID continue with that ID. A Run is finished when none of its Packets is buffered in an Inport or being processed by a Brick any more; by then every Packet of the Run that leaves the Network has left it.
_Avoid_: Process, Flow, Trace, Job

**Run ID**:
The identifier that every packet carries in its metadata. 
_Avoid_: ProcessId, Process ID, Correlation ID

**Instance**:
One running copy of a Network: its own set of Inport channels and goroutines for every Node, including all Nodes of nested Networks. A nested Network never gets an Instance of its own; it runs inside the Instance of the Network it is nested in. Each Service runs exactly one Instance. Parallel processing within an Instance is set by Concurrency; scaling beyond one Instance is done by running several Services.
_Avoid_: Worker, Replica, Process

### Observation
**Event**:
A record of one step of a Packet within an Instance: sent into a Network Inport, received by a Brick, processed by a Brick, emitted by a Brick, or output from the Network. It carries the Run ID, the Packet ID, the node path and the port.
_Avoid_: Span, Log entry

**Observer**:
A function that receives every Event of an Instance. It can be given when the Instance starts and be set, replaced or removed while it runs. Without an Observer, no Events and no Packet IDs are created.
_Avoid_: Tracer, Listener, Hook

**Packet ID**:
Identifies one Packet within an Instance. Sending into the Network and every emit create a new Packet ID; an emitted Packet refers to the Packet it was made from as its parent. The copies of a Fan-out keep the same Packet ID. Packet IDs exist only while the Instance has an Observer and do not cross Services.

**Service**:
A wrapper around exactly one Instance of a Network that makes the Network reachable via nats.io. There can be multiple Services of the same Network. When a packet is sent to the Network's Inport, only one Service processes it.


## Behaviour

Brick, Network and Port are go structs.

A Brick's business logic is a function stored in a field of the Brick struct, not a method of a separate type per Brick. The function holds no state of its own; configuration is captured when the Brick is built (e.g. `NewHTTPGet(url) *Brick`). Because Concurrency and multiple Instances run the same function in parallel, any state a Brick really needs must be made safe for concurrent use by the Brick itself.

Connections are based on Go channels:
- Every Inport of a Brick owns one buffered Go channel (default buffer size 10, configurable per Instance). All Connections leading to that Inport write into this channel.
- The ports of a nested Network have no channels of their own: each of its Inports forwards to the Inports of the connected inner Nodes, its Outports forward to the targets in the enclosing Network.
- An Outport keeps a list of the Inports it is connected to. For Fan-out, it sends a copy of each packet to every connected Inport, one after another.
- Packet data is copied when it enters the Network (Send) and when a Brick sends it (Emit). Every receiver owns the data it gets; nobody shares packet data.
- When an Inport's buffer is full, the sending Outport waits until there is space again.
- Inport channels are never closed. A Network is stopped by one shared stop signal (Go `context`); when it fires, all Bricks stop immediately. Packets still waiting in Inport buffers are not processed and are lost.
- A Packet sent through an Error Outport that has no Connection is logged instead of being dropped silently. The log record carries the size of the data and, for ErrorData, the node, Brick and message, but not the data itself.
- A Brick emits only while its function runs; an Emit after the function has returned is refused.
- An Observer is called synchronously from the goroutine that causes the Event and must return quickly. Only the Outports of the top-level Network create Output Events; the ports of nested Networks only forward and create none.
- When a Brick's function returns an error or panics, the Brick sends a Packet with the same Run ID through its Error Outport. Its data is a JSON object: `node` (node path within the top-level Network, e.g. `outer/inner/upper`), `brick` (Brick name), `message` (error text) and `input` (data of the Packet that failed).

Service:
- Services are implemented with nats.io.
- A Service receives Packets on one subject per Network Inport and publishes every Packet leaving a Network Outport on that Outport's subject, with the Packet's Run ID as the last token. There is no request/reply; consumers subscribe to `op.<outportname>.*` (all Runs) or `op.<outportname>.<runid>` (one Run).
- The Run ID travels in the NATS header `Fbp-Run-Id` (case-sensitive), the Packet data in the message body. A Run ID that is not a valid subject token or longer than 64 bytes is not passed to the Network; an error Packet is published on the Error subject under a new Run ID instead.
- Message data that is not valid JSON is not passed to the Network; an error Packet is published on the Error subject instead, under the message's Run ID unless that Run is going on in the Service, then under a new one.
- Observing is switched on and off via NATS for all Services of a Network at once (not only one of the queue group). While it is on, each Service publishes every Event of its Instance on the Trace subject, with an ID of the Service, because Packet IDs do not cross Services.
- When a Run is finished, the Service announces it on the Done subject with the number of Packets the Run published on Outport subjects, so a client knows when it has all results.
- Delivery is best effort, like inside a Network. When a Service cannot keep up, messages beyond its pending limit are dropped and not redelivered to another Service of the queue group; when the connection is down and the reconnect buffer is full, results are lost. Both cases are logged.

**Service Subject structure**
- fbp.<packagepath>.<networkname>.ip.in = NATS subject for the default Inport In of a Network
- fbp.<packagepath>.<networkname>.ip.<inportname> = NATS subject for every additional Inport of a Network; its name must not be `in` or `observe`
- fbp.<packagepath>.<networkname>.op.<outportname>.<runid> = NATS subject for an Outport of a network; the last token is the Run ID of the Packet
- fbp.<packagepath>.<networkname>.done.<runid> = NATS subject on which the Service announces that a Run is finished
- fbp.<packagepath>.<networkname>.observe = NATS subject that switches the Observer of every Service of the Network on (`true`) or off (`false`)
- fbp.<packagepath>.<networkname>.trace = NATS subject on which the Services publish the observed Events
- fbp.<packagepath>.<networkname>.log.<serviceid> = Log subject: NATS subject on which a Service publishes its log records
- fbp.<packagepath>.<networkname>.loghistory.<serviceid> = Log history subject: a request there is answered with the Service's recent log records
- <packagepath> is the Go package path of the Network. Every `/` separates a subject token; every `.` inside a path element becomes `_` (e.g. `github.com/acme/flows` → `github_com.acme.flows`).
