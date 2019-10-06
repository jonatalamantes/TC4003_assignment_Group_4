package chandy_lamport

import "log"

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	//This variable is going to store tne requested snapshots
	Snapshots map[int]*SnapshotInServer // key = snapshotId
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		make(map[int]*SnapshotInServer),
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	//Correctness of the messages on the snapshots
	//If there is already one snapshot done by this server
	//Store one "Invert" message with the same value
	//Since is not allowed new message after the snapshot
	for _, snap := range server.Snapshots {
		if snap.Done {
			newMessage := TokenMessage{-1 * numTokens}
			newMsg := SnapshotMessage{server.Id, dest, newMessage}
			snap.Menssages = append(snap.Menssages, &newMsg)
		}
	}
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {

	switch msg := message.(type) {

	case TokenMessage:

		//In case the snapshot is already done
		//Store this mensage to track the received menssages
		for _, snap := range server.Snapshots {
			if snap.Done {
				newMsg := SnapshotMessage{src, server.Id, message}
				snap.Menssages = append(snap.Menssages, &newMsg)
			}
		}

		//Update of tokens
		server.Tokens += msg.numTokens

	case MarkerMessage:

		//Start snapshot in case there is not one
		server.StartSnapshot(msg.snapshotId)

	}
}

//Bootstrap method that create the Snapshots in server
//When a snapshot message is received by the simulator
func (server *Server) InitSnap(snapshotId int) {
	server.Snapshots[snapshotId] = NewSnapshotInServer()
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {

	//Get the snapshot
	snap := server.Snapshots[snapshotId]

	if !snap.Done {

		//Do the snapshot
		snap.Tokens = server.Tokens
		snap.Done = true
		server.sim.NotifySnapshotComplete(server.Id, snapshotId)

		//Update the channel since CollectSnapshots is waiting
		go func() {
			snap.DoneChannel <- true
		}()

		//Send the marker to other servers
		snapMessage := MarkerMessage{snapshotId}
		server.SendToNeighbors(snapMessage)
	}
}
