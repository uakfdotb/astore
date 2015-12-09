package main

import "math/rand"
import "strings"

type LDRClient struct {
	Servers []string
	PreferredServers map[string]bool // can be nil to disable preference
	Failures int // number of failures to tolerate

	// read from all replicas simultaneously
	ReadAllSimultaneously bool

	// replicate across all servers instead of sharding in groups of 2f+1
	ReplicateAll bool

	Connections *Connections
	id uint32 // client identifier to break ties, randomly generated at startup
}

func (client *LDRClient) Init() {
	client.id = rand.Uint32()
	if client.Connections == nil {
		client.Connections = new(Connections)
	}
}

func (client *LDRClient) quorum() int {
	if client.ReplicateAll {
		return (len(client.Servers) + 2) / 2
	} else {
		return client.Failures + 1
	}
}

func (client *LDRClient) getMin(min int, header PacketHeader, key string, data []byte, minimizeCommunication bool) map[string]*Packet {
	packet := new(Packet)
	packet.Header = header
	packet.Key = key
	packet.Data = data

	var servers []string
	if client.ReplicateAll {
		servers = client.Servers
	} else {
		// determine the set of servers to communicate with
		// this is needed because we may have more than (2f+1) servers, but
		//    don't want to waste bandwidth doing queries with them additional ones
		// we checksum the key, get a first server index, and then take the (2f+1)
		//    following servers
		firstServerIndex := hash(key, len(client.Servers))
		for i := 0; i < 2 * client.Failures + 1; i++ {
			servers = append(servers, client.Servers[(firstServerIndex + i) % len(client.Servers)])
		}
	}

	var packets []*Packet
	for _ = range servers {
		packets = append(packets, packet)
	}

	return client.Connections.GetMinResponses(min, packets, servers, minimizeCommunication, 0)
}

func (client *LDRClient) getQuorum(header PacketHeader, key string, data []byte) map[string]*Packet {
	return client.getMin(client.quorum(), header, key, data, false)
}

func (client *LDRClient) doQuery(key string) (Tag, []string) {
	responses := client.getQuorum(PacketHeader{PacketID: LDR_PACKET_DIR_READ}, key, nil)
	var largestTag *Tag
	var upToDateServers []string

	for _, packet := range responses {
		if largestTag == nil {
			largestTag = new(Tag)
			largestTag.Sequence = packet.Header.Sequence
			largestTag.Client = packet.Header.Client
			upToDateServers = strings.Split(string(packet.Data), ",")
		} else if largestTag.Less(Tag{packet.Header.Sequence, packet.Header.Client}) {
			largestTag.Sequence = packet.Header.Sequence
			largestTag.Client = packet.Header.Client
			upToDateServers = strings.Split(string(packet.Data), ",")
		}
	}

	return *largestTag, upToDateServers
}

func (client *LDRClient) Read(key string) []byte {
	// rdr: read tag from quorum
	tag, upToDateServers := client.doQuery(key)
	// rdw: write tag to quorum
	client.getQuorum(PacketHeader{
		PacketID: LDR_PACKET_DIR_WRITE,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}, key, []byte(strings.Join(upToDateServers, ",")))
	// rrr: read from any replica
	packet := new(Packet)
	packet.Header = PacketHeader{
		PacketID: LDR_PACKET_REP_READ,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}
	packet.Key = key
	var response *Packet
	if client.ReadAllSimultaneously {
		responses := client.getMin(1, packet.Header, packet.Key, packet.Data, false)
		for _, oneResponse := range responses {
			if oneResponse != nil {
				response = oneResponse
				break
			}
		}
	} else {
		// in the default mode, we read from a single replica at a time to
		//   avoid unnecessarily wasting bandwidth
		// we proceed in three phases to minimize read time
		// phase 1: read from servers with active connection and preference bit
		// phase 2: read from servers with active connection
		// phase 3: randomly select servers to read from
		// we return whenever we successfully get a response from some server
		for phase := 1; phase <= 2 && response == nil; phase++ {
			for _, server := range upToDateServers {
				if phase == 2 || (client.PreferredServers != nil && client.PreferredServers[server]) {
					if client.Connections.IsActive(server) {
						response = client.Connections.Send(server, packet)
						if response != nil {
							break
						}
					}
				}
			}
		}
		for response == nil {
			server := upToDateServers[rand.Intn(len(upToDateServers))]
			response = client.Connections.Send(server, packet)
		}
	}
	if response.Data == nil { // maybe something got garbage collected
		return client.Read(key)
	}
	return response.Data
}

func (client *LDRClient) Write(key string, data []byte) {
	// wdr: read currently largest tag from quorum
	largestTag, _ := client.doQuery(key)
	// wrw: write to at least (f + 1) replicas
	tag := Tag{largestTag.Sequence + 1, client.id}
	responses := client.getMin(client.Failures + 1, PacketHeader{
		PacketID: LDR_PACKET_REP_WRITE,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}, key, data, true)
	// wdw1: write tag to directory
	var upToDateServers []string
	for server := range responses {
		upToDateServers = append(upToDateServers, server)
	}
	client.getQuorum(PacketHeader{
		PacketID: LDR_PACKET_DIR_WRITE,
		Sequence: tag.Sequence,
		Client: tag.Client,
	}, key, []byte(strings.Join(upToDateServers, ",")))
	// wdw2: secure replicas so they can garbage collect
	// this isn't critical, so don't even wait for responses before returning from write
	//  (and also don't bother retrying)
	for _, server := range upToDateServers {
		go func(server string) {
			packet := new(Packet)
			packet.Header = PacketHeader{
				PacketID: LDR_PACKET_REP_SECURE,
				Sequence: tag.Sequence,
				Client: tag.Client,
			}
			packet.Key = key
			client.Connections.Send(server, packet)
		}(server)
	}
}
