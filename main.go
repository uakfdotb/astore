package main

import crand "crypto/rand"
import "encoding/binary"
import "flag"
import "log"
import "math/rand"
import "runtime"
import "strings"
import "time"

func main() {
	seedBytes := make([]byte, 8)
	_, err := crand.Read(seedBytes)
	if err != nil {
		log.Fatalf("failed to seed math/rand: %s", err.Error())
	} else {
		seed, _ := binary.Varint(seedBytes)
		rand.Seed(seed)
	}

	algorithm := flag.String("algorithm", "ldr", "algorithm to use; one of ldr, cas")
	mode := flag.String("mode", "server", "mode to run, either bench or server")
	servers := flag.String("servers", "127.0.0.1:9224", "comma-separated list of servers, e.g. 127.0.0.1:9224,127.0.0.1:9225")
	benchPreferred := flag.String("preferred", "", "bench mode: for ldr algorithm, optional comma-separated list of nearby servers")
	benchFailures := flag.Int("failures", 2, "bench mode: failures to tolerate")
	benchWritePercentage := flag.Int("writeperc", 5, "bench mode: percentage of writes")
	benchPacketLoss := flag.Int("packetloss", 0, "bench mode: percentage of messages to drop")
	benchLDRReadAllSimultaneously := flag.Bool("ldrreadall", false, "bench mode: for ldr algorithm, whether to read all replicas simultaneously")
	benchLDRReplicateAll := flag.Bool("ldrreplicateall", false, "bench mode: for ldr algorithm, whether to replicate across all servers instead of sharding")
	benchCASRetriesAfterDone := flag.Int("casretries", 0, "bench mode: for cas algorithm, number of extra retries to attempt")
	benchNumKeys := flag.Int("numkeys", 1000, "bench mode: number of keys")
	benchKeySize := flag.Int("keysize", 8192, "bench mode: size of data to write")
	benchDistribution := flag.String("distribution", "zipfian", "bench mode: key distribution, either zipfian or uniform")
	serverPort := flag.Int("port", DEFAULT_SERVER_PORT, "server mode: TCP port to listen for connections on")
	serverLDRGossip := flag.Bool("ldrgossip", false, "server mode: for ldr algorithm, whether to enable secure tag gossiping")
	serverCASGCDelta := flag.Int("casgcdelta", 3, "server mode: for cas algorithm, garbage collection delta")
	serverCASGCExpire := flag.Int("casgcexpire", 1000, "server mode: for cas algorithm, garbage collection expiration time in milliseconds")
	flag.Parse()

	if *algorithm != "ldr" && *algorithm != "cas" {
		log.Fatalf("invalid algorithm %s", *algorithm)
	} else if *benchDistribution != "zipfian" && *benchDistribution != "uniform" {
			log.Fatalf("invalid distribution %s", *benchDistribution)
	}

	if *mode == "bench" {
		var client Client
		if *algorithm == "ldr" {
			ldrClient := &LDRClient{
				Servers: strings.Split(*servers, ","),
				Failures: *benchFailures,
				Connections: &Connections{PacketLoss: *benchPacketLoss},
				ReadAllSimultaneously: *benchLDRReadAllSimultaneously,
				ReplicateAll: *benchLDRReplicateAll,
			}
			if *benchPreferred != "" {
				ldrClient.PreferredServers = make(map[string]bool)
				for _, preferredServer := range strings.Split(*benchPreferred, ",") {
					ldrClient.PreferredServers[preferredServer] = true
				}
			}
			ldrClient.Init()
			client = ldrClient
		} else if *algorithm == "cas" {
			casClient := &CASClient{
				Servers: strings.Split(*servers, ","),
				Failures: *benchFailures,
				Connections: &Connections{PacketLoss: *benchPacketLoss},
				RetriesAfterDone: *benchCASRetriesAfterDone,
			}
			casClient.Init()
			client = casClient
		}
		done := make(chan bool)
		go resourceAccounting(done)
		throughput, write99, read99 := benchmark(client, *benchDistribution == "zipfian", *benchWritePercentage, *benchNumKeys, *benchKeySize)
		log.Printf("bench; throughput=%.4f; writelatency=%.4f; readlatency=%.4f", throughput, float64(write99) / float64(time.Millisecond), float64(read99) / float64(time.Millisecond))
		done <- true
	} else if *mode == "server" {
		if *algorithm == "ldr" {
			server := &LDRServer{
				Servers: strings.Split(*servers, ","),
				Gossip: *serverLDRGossip,
			}
			go server.Run(*serverPort)
		} else if *algorithm == "cas" {
			server := &CASServer{
				GCDelta: *serverCASGCDelta,
				GCTimeLimit: time.Duration(*serverCASGCExpire) * time.Millisecond,
			}
			go server.Run(*serverPort)
		}
		done := make(chan bool)
		resourceAccounting(done)
	} else {
		log.Fatalf("invalid mode %s", *mode)
	}
}

func resourceAccounting(done chan bool) {
	initialNetworkUsage := getNetworkUsage()
	for {
		runtime.GC()
		memStats := new(runtime.MemStats)
		runtime.ReadMemStats(memStats)
		memoryUsage := memStats.Alloc

		networkUsage := getNetworkUsage() - initialNetworkUsage

		log.Printf("accounting; memory=%d; network=%d", memoryUsage, networkUsage)

		select {
			case <- done:
				return
			case <- time.After(time.Second):
		}
	}
}
