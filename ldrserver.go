package main

import "encoding/binary"
import "io"
import "fmt"
import "log"
import "net"
import "strings"
import "sync"

type LDREntry struct {
	Data []byte
	Secured bool
}

type LDRKeyData struct {
	// replica state
	entries map[Tag]*LDREntry
	latestSecuredTag *Tag

	// directory state
	latestTag Tag
	upToDateServers []string
}

type LDRGossip struct {
	Target string
	Packet *Packet
}

type LDRServer struct {
	Servers []string
	Gossip bool

	keys map[string]*LDRKeyData
	mu sync.Mutex

	connections *Connections
	gossipCh chan LDRGossip
}

func (server *LDRServer) Run(port int) {
	server.keys = make(map[string]*LDRKeyData)
	server.connections = new(Connections)

	if server.Gossip {
		server.gossipCh = make(chan LDRGossip, 256)
		for i := 0; i < len(server.Servers); i++ {
			go func() {
				for {
					gossipStruct := <- server.gossipCh
					server.connections.Send(gossipStruct.Target, gossipStruct.Packet)
				}
			}()
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("error listening on port %d: %s", port, err.Error())
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("error accepting connection: %s", err.Error())
		}
		go server.handleConnection(conn)
	}
}

// Helper function to retrieve key data, adding the data if not there already
func (server *LDRServer) getKeyData(key string) *LDRKeyData {
	if server.keys[key] == nil {
		server.keys[key] = &LDRKeyData{entries: make(map[Tag]*LDREntry)}
	}
	return server.keys[key]
}

func (server *LDRServer) directoryRead(key string) (Tag, []string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if keyData, ok := server.keys[key]; ok {
		return keyData.latestTag, keyData.upToDateServers
	} else {
		return Tag{}, nil
	}
}

func (server *LDRServer) directoryWrite(key string, tag Tag, servers []string) {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if keyData.latestTag.Less(tag) {
		keyData.latestTag = tag
		keyData.upToDateServers = servers
	}
}

func (server *LDRServer) replicaWrite(key string, tag Tag, data []byte) {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if _, ok := keyData.entries[tag]; !ok {
		keyData.entries[tag] = &LDREntry{Data: data}
	}
}

func (server *LDRServer) replicaSecure(key string, tag Tag) {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if _, ok := keyData.entries[tag]; ok {
		keyData.entries[tag].Secured = true
	}
	keyData.latestSecuredTag = &tag
	// delete anything less than the secured tag
	for otherTag := range keyData.entries {
		if otherTag.Less(tag) {
			delete(keyData.entries, otherTag)
		}
	}
}

func (server *LDRServer) replicaRead(key string, tag Tag) []byte {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if _, ok := keyData.entries[tag]; ok {
		return keyData.entries[tag].Data
	} else if keyData.latestSecuredTag != nil && keyData.entries[*keyData.latestSecuredTag] != nil {
		return keyData.entries[*keyData.latestSecuredTag].Data
	} else {
		return nil
	}
}

func (server *LDRServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	var header PacketHeader
	for {
		// read header, key, and optional data
		err := binary.Read(conn, binary.BigEndian, &header)
		if err != nil {
			log.Printf("error while reading from %s: %s", conn.RemoteAddr().String(), err.Error())
			break
		}
		keyBytes := make([]byte, header.KeyLength)
		io.ReadFull(conn, keyBytes)
		key := string(keyBytes)
		var data []byte
		if header.DataLength > 0 {
			data = make([]byte, header.DataLength)
			io.ReadFull(conn, data)
		}
		// process packet
		if header.PacketID == LDR_PACKET_DIR_READ {
			// respond with latest finalized tag
			latestTag, upToDateServers := server.directoryRead(key)
			upToDateStr := strings.Join(upToDateServers, ",")
			binary.Write(conn, binary.BigEndian, PacketHeader{
				PacketID: LDR_PACKET_RESPONSE,
				Sequence: latestTag.Sequence,
				Client: latestTag.Client,
				DataLength: uint16(len(upToDateStr)),
			})
			conn.Write([]byte(upToDateStr))
		} else if header.PacketID == LDR_PACKET_DIR_WRITE {
			tag := Tag{header.Sequence, header.Client}
			servers := strings.Split(string(data), ",")
			server.directoryWrite(key, tag, servers)
			binary.Write(conn, binary.BigEndian, PacketHeader{PacketID: LDR_PACKET_RESPONSE})
		} else if header.PacketID == LDR_PACKET_REP_WRITE {
			tag := Tag{header.Sequence, header.Client}
			server.replicaWrite(key, tag, data)
			binary.Write(conn, binary.BigEndian, PacketHeader{PacketID: LDR_PACKET_RESPONSE})
		} else if header.PacketID == LDR_PACKET_REP_SECURE || header.PacketID == LDR_PACKET_REP_GOSSIP {
			tag := Tag{header.Sequence, header.Client}
			server.replicaSecure(key, tag)
			binary.Write(conn, binary.BigEndian, PacketHeader{PacketID: LDR_PACKET_RESPONSE})
			if header.PacketID == LDR_PACKET_REP_SECURE && server.Gossip {
				for _, target := range server.Servers {
					server.gossipCh <- LDRGossip{target, &Packet{
						Header: PacketHeader{
							PacketID: LDR_PACKET_REP_GOSSIP,
							Sequence: tag.Sequence,
							Client: tag.Client,
						},
						Key: key,
					}}
				}
			}
		} else if header.PacketID == LDR_PACKET_REP_READ {
			tag := Tag{header.Sequence, header.Client}
			data := server.replicaRead(key, tag)
			binary.Write(conn, binary.BigEndian, PacketHeader{
				PacketID: LDR_PACKET_RESPONSE,
				DataLength: uint16(len(data)),
			})
			conn.Write(data)
		} else {
			log.Printf("invalid packet ID from %s", conn.RemoteAddr().String())
			break
		}
	}
}
