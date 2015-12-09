package main

import "bytes"
import "log"
import "math/rand"
import "net"
import "sync"
import "time"

import "github.com/klauspost/reedsolomon"

type CASConnection struct {
	Conn net.Conn
	LastConnectionAttempt time.Time
	mu sync.Mutex
}

type CASClient struct {
	Servers []string
	Failures int // number of failures to tolerate

	// number of iterations to retry on failed servers after getting quorum
	// default is 0
	// only applies to writing data shards
	RetriesAfterDone int

	Connections *Connections
	k int // number of shards needed to reconstruct
	quorum int // number of servers constituting a quorum
	mu sync.Mutex
	enc reedsolomon.Encoder
	id uint32 // client identifier to break ties, randomly generated at startup
}

func (client *CASClient) Init() {
	client.k = len(client.Servers) - 2 * client.Failures
	client.quorum = len(client.Servers) - client.Failures
	client.id = rand.Uint32()

	if client.Connections == nil {
		client.Connections = new(Connections)
	}

	var err error
	client.enc, err = reedsolomon.New(client.k, len(client.Servers) - client.k)
	if err != nil {
		log.Fatalf("error initializing reed solomon encoder (n=%d, k=%d): %s", len(client.Servers), client.k, err.Error())
	}
}

func (client *CASClient) getMinWithPackets(min int, packets []*Packet) map[string]*Packet {
	return client.Connections.GetMinResponses(min, packets, client.Servers, false, 0)
}

func (client *CASClient) getMin(min int, header PacketHeader, key string, data []byte) map[string]*Packet {
	packet := new(Packet)
	packet.Header = header
	packet.Key = key
	packet.Data = data

	var packets []*Packet
	for _ = range client.Servers {
		packets = append(packets, packet)
	}

	return client.getMinWithPackets(min, packets)
}

// retriesAfterDone: number of retries to attempt after getting a quorum
//   we might want to retry writing data so that readers have more servers to choose from (can improve performance)
func (client *CASClient) getQuorumWithPackets(packets []*Packet, retriesAfterDone int) map[string]*Packet {
	return client.Connections.GetMinResponses(client.quorum, packets, client.Servers, false, retriesAfterDone)
}

func (client *CASClient) getQuorum(header PacketHeader, key string, data []byte) map[string]*Packet {
	return client.getMin(client.quorum, header, key, data)
}

func (client *CASClient) doQuery(key string) Tag {
	responses := client.getQuorum(PacketHeader{PacketID: CAS_PACKET_QUERY}, key, nil)
	var largestTag *Tag

	for _, packet := range responses {
		if largestTag == nil {
			largestTag = new(Tag)
			largestTag.Sequence = packet.Header.Sequence
			largestTag.Client = packet.Header.Client
		} else if largestTag.Less(Tag{packet.Header.Sequence, packet.Header.Client}) {
			largestTag.Sequence = packet.Header.Sequence
			largestTag.Client = packet.Header.Client
		}
	}

	return *largestTag
}

func (client *CASClient) Read(key string) []byte {
	tag := client.doQuery(key)
	responses := client.getMin(client.quorum, PacketHeader{
		PacketID: CAS_PACKET_READ_FIN,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}, key, nil)
	data := make([][]byte, len(client.Servers))
	var resultLen int
	var dataCount int
	for i, server := range client.Servers {
		if responses[server] != nil {
			data[i] = responses[server].Data
			if len(data[i]) > 0 {
				resultLen = len(data[i]) * client.k
				dataCount++
			}
		}
	}
	if dataCount < client.k {
		// probably due to garbage collection, need to retry
		return client.Read(key)
	}
	err := client.enc.Reconstruct(data)
	if err != nil {
		log.Fatalf("unexpected error while reconstructing: %s", err.Error())
	}
	buf := new(bytes.Buffer)
	err = client.enc.Join(buf, data, resultLen)
	if err != nil {
		log.Fatalf("unexpected error while joining: %s", err.Error())
	}
	return buf.Bytes()
}

func (client *CASClient) Write(key string, bytes []byte) {
	largestTag := client.doQuery(key)
	// pre: send shards to servers
	tag := Tag{largestTag.Sequence + 1, client.id}
	data, err := client.enc.Split(bytes)
	if err != nil {
		log.Fatalf("unexpected error while splitting: %s", err.Error())
	}
	packets := make([]*Packet, len(client.Servers))
	for i := range packets {
		packets[i] = new(Packet)
		packets[i].Header = PacketHeader{
			PacketID: CAS_PACKET_WRITE_PRE,
			Sequence: tag.Sequence,
			Client: tag.Client,
		}
		packets[i].Key = key
		packets[i].Data = data[i]
	}
	client.getQuorumWithPackets(packets, client.RetriesAfterDone)
	// fin: broadcast finalize
	client.getQuorum(PacketHeader{
		PacketID: CAS_PACKET_WRITE_FIN,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}, key, nil)
}
