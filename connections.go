package main

import "encoding/binary"
import "io"
import "log"
import "net"
import "math/rand"
import "sync"
import "time"

// Connections object abstracts message sending code for ldr, cas clients

type PacketHeader struct {
	PacketID byte
	Sequence uint32
	Client uint32
	KeyLength uint8
	DataLength uint16
}

type Packet struct {
	Header PacketHeader
	Key string
	Data []byte
}

type Connection struct {
	Conn net.Conn
	LastConnectionAttempt time.Time
	mu sync.Mutex
}

type Connections struct {
	PacketLoss int
	connections map[string]*Connection
	mu sync.Mutex
}

func (cns *Connections) getConnection(server string) *Connection {
	cns.mu.Lock()
	defer cns.mu.Unlock()
	if cns.connections == nil {
		cns.connections = make(map[string]*Connection)
	}
	if cns.connections[server] == nil {
		cns.connections[server] = &Connection{}
	}
	return cns.connections[server]
}

func (cns *Connections) IsActive(server string) bool {
	connection := cns.getConnection(server)
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.Conn != nil
}

func (cns *Connections) Send(server string, packet *Packet) *Packet {
	if rand.Intn(100) < cns.PacketLoss {
		time.Sleep(2 * time.Second)
		return nil
	}

	packet.Header.KeyLength = uint8(len(packet.Key))
	packet.Header.DataLength = uint16(len(packet.Data))
	connection := cns.getConnection(server)
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.Conn == nil {
		// attempt to connect, but limit to one connection attempt per second
		if time.Now().Before(connection.LastConnectionAttempt.Add(time.Second)) {
			return nil
		}
		connection.LastConnectionAttempt = time.Now()
		var err error
		connection.Conn, err = net.DialTimeout("tcp", server, time.Second)
		if err != nil {
			log.Printf("warning: error connecting to %s: %s", server, err.Error())
			return nil
		}
	}
	binary.Write(connection.Conn, binary.BigEndian, packet.Header)
	connection.Conn.Write([]byte(packet.Key))
	if len(packet.Data) > 0 {
		connection.Conn.Write(packet.Data)
	}

	response, err := func() (*Packet, error) {
		response := new(Packet)
		connection.Conn.SetReadDeadline(time.Now().Add(time.Second))
		err := binary.Read(connection.Conn, binary.BigEndian, &response.Header)
		if err != nil {
			return nil, err
		}
		keyBytes := make([]byte, response.Header.KeyLength)
		_, err = io.ReadFull(connection.Conn, keyBytes)
		if err != nil {
			return nil, err
		}
		response.Key = string(keyBytes)
		if response.Header.DataLength > 0 {
			response.Data = make([]byte, response.Header.DataLength)
			_, err = io.ReadFull(connection.Conn, response.Data)
			if err != nil {
				return nil, err
			}
		}
		return response, nil
	}()
	if err != nil {
		log.Printf("warning: error reading from %s: %s", server, err.Error())
		connection.Conn.Close()
		connection.Conn = nil
		return nil
	}
	return response
}

func (cns *Connections) GetMinResponses(min int, packets []*Packet, servers []string, minimizeCommunication bool, retriesAfterDone int) map[string]*Packet {
	// repeatedly try contacting all servers that we haven't gotten a response from yet
	//   until we have responses from at least min servers
	// if minimizeCommunication, then only try to get min iteration on next round
	type ServerResponse struct {
		Server string
		Packet *Packet
	}

	responses := make(map[string]*Packet)

	for len(responses) < min {
		numWaiting := 0
		doneChan := make(chan ServerResponse)
		for _, i := range rand.Perm(len(servers)) {
			server := servers[i]
			if responses[server] != nil {
				continue
			}
			numWaiting++
			go func(server string, packet *Packet) {
				response := cns.Send(server, packet)
				doneChan <- ServerResponse{server, response}
			}(server, packets[i])
			if minimizeCommunication && len(responses) + numWaiting >= min {
				break
			}
		}
		for i := 0; i < numWaiting; i++ {
			response := <- doneChan
			if response.Packet != nil {
				responses[response.Server] = response.Packet

				// don't wait for additional responses if we have enough packets
				if len(responses) >= min && retriesAfterDone == 0 {
					go func(numWaiting int) {
						for i := 0; i < numWaiting; i++ {
							<- doneChan
						}
					}(numWaiting - i)
					break
				}
			}
		}
	}

	for i := 0; i < retriesAfterDone; i++ {
		numWaiting := 0
		doneChan := make(chan ServerResponse)
		for _, server := range servers {
			if responses[server] != nil {
				continue
			}
			numWaiting++
			go func(server string, packet *Packet) {
				response := cns.Send(server, packet)
				doneChan <- ServerResponse{server, response}
			}(server, packets[i])
		}
		for i := 0; i < numWaiting; i++ {
			response := <- doneChan
			if response.Packet != nil {
				responses[response.Server] = response.Packet
			}
		}
	}

	return responses
}
