package main

import "encoding/binary"
import "io"
import "fmt"
import "log"
import "net"
import "sort"
import "sync"
import "time"

type CASEntry struct {
	Data []byte
	Finalized bool
	Time time.Time
}

type CASKeyData struct {
	latestFinalizedTag Tag
	entries map[Tag]*CASEntry
}

func (keyData *CASKeyData) GarbageCollect(delta int, timeLimit time.Duration) {
	// our garbage collection has two components
	// 1) delete all tags other than the highest (delta + 1) tags
	// 2) any tags less than latestFinalizedTag where time is below timeLimit
	// we also always preserve latestFinalizedTag
	if delta > 0 {
		var tags SortableTags
		for tag := range keyData.entries {
			tags = append(tags, tag)
		}
		sort.Sort(tags)
		if len(tags) > delta + 1 {
			cutoffTag := tags[len(tags) - delta - 2]
			for tag := range keyData.entries {
				if tag.Less(cutoffTag) && tag != keyData.latestFinalizedTag {
					delete(keyData.entries, tag)
				}
			}
		}
	}
	if timeLimit > 0 {
		for tag, entry := range keyData.entries {
			if tag.Less(keyData.latestFinalizedTag) && time.Now().After(entry.Time.Add(timeLimit)) {
				delete(keyData.entries, tag)
			}
		}
	}
}

type CASServer struct {
	keys map[string]*CASKeyData
	mu sync.Mutex

	// garbage collection parameters, see CASKeyData.GarbageCollect
	GCDelta int
	GCTimeLimit time.Duration
}

func (server *CASServer) Run(port int) {
	server.keys = make(map[string]*CASKeyData)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("error listening on port %d: %s", port, err.Error())
	}
	go func() {
		for {
			server.GarbageCollect()
			time.Sleep(time.Second)
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("error accepting connection: %s", err.Error())
		}
		go server.handleConnection(conn)
	}
}

func (server *CASServer) GarbageCollect() {
	server.mu.Lock()
	defer server.mu.Unlock()
	for _, keyData := range server.keys {
		keyData.GarbageCollect(server.GCDelta, server.GCTimeLimit)
	}
}

func (server *CASServer) getLatestFinalizedTag(key string) Tag {
	server.mu.Lock()
	defer server.mu.Unlock()
	if keyData, ok := server.keys[key]; ok {
		return keyData.latestFinalizedTag
	} else {
		return Tag{}
	}
}

// Helper function to retrieve key data, adding the data if not there already
func (server *CASServer) getKeyData(key string) *CASKeyData {
	if server.keys[key] == nil {
		server.keys[key] = &CASKeyData{entries: make(map[Tag]*CASEntry)}
	}
	return server.keys[key]
}

func (server *CASServer) insertPreTag(key string, tag Tag, data []byte) {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if _, ok := keyData.entries[tag]; !ok {
		keyData.entries[tag] = &CASEntry{Data: data, Time: time.Now()}
		keyData.GarbageCollect(server.GCDelta, server.GCTimeLimit)
	}
}

func (server *CASServer) finalizeTag(key string, tag Tag) {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if _, ok := keyData.entries[tag]; ok {
		keyData.entries[tag].Finalized = true
	}
	if keyData.latestFinalizedTag.Less(tag) {
		keyData.latestFinalizedTag = tag
		keyData.GarbageCollect(server.GCDelta, server.GCTimeLimit)
	}
}

func (server *CASServer) readTag(key string, tag Tag) []byte {
	server.mu.Lock()
	defer server.mu.Unlock()
	keyData := server.getKeyData(key)
	if keyData.latestFinalizedTag.Less(tag) {
		keyData.latestFinalizedTag = tag
	}
	if _, ok := keyData.entries[tag]; ok {
		keyData.entries[tag].Finalized = true
		return keyData.entries[tag].Data
	}
	return nil
}

func (server *CASServer) handleConnection(conn net.Conn) {
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
		if header.PacketID == CAS_PACKET_QUERY {
			// respond with latest finalized tag
			latestFinalizedTag := server.getLatestFinalizedTag(key)
			binary.Write(conn, binary.BigEndian, PacketHeader{
				PacketID: CAS_PACKET_RESPONSE,
				Sequence: latestFinalizedTag.Sequence,
				Client: latestFinalizedTag.Client,
			})
		} else if header.PacketID == CAS_PACKET_WRITE_PRE {
			tag := Tag{header.Sequence, header.Client}
			server.insertPreTag(key, tag, data)
			binary.Write(conn, binary.BigEndian, PacketHeader{PacketID: CAS_PACKET_RESPONSE})
		} else if header.PacketID == CAS_PACKET_WRITE_FIN {
			tag := Tag{header.Sequence, header.Client}
			server.finalizeTag(key, tag)
			binary.Write(conn, binary.BigEndian, PacketHeader{PacketID: CAS_PACKET_RESPONSE})
		} else if header.PacketID == CAS_PACKET_READ_FIN {
			tag := Tag{header.Sequence, header.Client}
			data := server.readTag(key, tag)
			binary.Write(conn, binary.BigEndian, PacketHeader{
				PacketID: CAS_PACKET_RESPONSE,
				DataLength: uint16(len(data)),
			})
			conn.Write(data)
		} else {
			log.Printf("invalid packet ID from %s", conn.RemoteAddr().String())
			break
		}
	}
}
