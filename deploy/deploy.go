package main

import "fmt"
import "log"
import "sync"
import "time"

const NUM_TRIALS = 5

var cfg *Config

func main() {
	cfg = LoadConfig("deploy.cfg")
	factory := MakeServerFactory()
	templates := getTemplates()

	executions := make(map[string]*Execution)

	// LDR compare sharding across groups of 2f+1 vs replicating across entire cluster
	for _, packetLoss := range []int{0, 1, 3, 6, 10} {
		for mode := 0; mode < 2; mode++ {
			key := fmt.Sprintf("ldrshard.%d.%d", packetLoss, mode)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates["ldr"],
				MultiRegion: "",
				NumServer: 8,
				NumClient: 4,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				ActualFailures: 0,
				PacketLoss: packetLoss,
				LDRReadAllSimultaneously: false,
				LDRReplicateAll: mode == 1,
				LDRGossip: false,
				NoPreferences: false,
			}
		}
	}

	// LDR see if gossiping helps
	for _, keySize := range []int{128, 1024, 8192, 65535} {
		for mode := 0; mode < 2; mode++ {
			key := fmt.Sprintf("ldrgossip.%d.%d", keySize, mode)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates["ldr"],
				NumServer: 8,
				NumClient: 4,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				LDRGossip: mode == 1,
				KeySize: keySize,
			}
		}
	}

	// LDR see how much preference-based reads can improve
	for _, n := range []int{4, 8, 16, 32} {
		for mode := 0; mode < 2; mode++ {
			for _, keySize := range []int{8192, 65535} {
				key := fmt.Sprintf("ldrpref.%d.%d.%d", n, mode, keySize)
				executions[key] = &Execution{
					ID: "deploy-" + key,
					Template: templates["ldr"],
					MultiRegion: "mix",
					NumServer: n,
					NumClient: n / 2,
					ClientThreads: 4,
					WritePercentage: 5,
					Failures: (n - 1) / 2,
					NoPreferences: mode == 1,
					KeySize: keySize,
				}
			}
		}
	}

	// LDR compare reading from all replicas simultaneously to trying to read from one
	for _, packetLoss := range []int{0, 1, 3, 6, 10} {
		for mode := 0; mode < 2; mode++ {
			key := fmt.Sprintf("ldrreadall.%d.%d", packetLoss, mode)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates["ldr"],
				NumServer: 8,
				NumClient: 4,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				PacketLoss: packetLoss,
				LDRReadAllSimultaneously: mode == 1,
			}
		}
	}

	// CAS see effect of retrying on failed servers
	for _, packetLoss := range []int{0, 1, 3, 6, 10} {
		for mode := 0; mode < 3; mode++ {
			key := fmt.Sprintf("casretry.%d.%d", packetLoss, mode)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates["cas"],
				NumServer: 8,
				NumClient: 4,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				PacketLoss: packetLoss,
				CASRetriesAfterDone: mode,
			}
		}
	}

	// CAS change garbage collection settings
	for _, gcdelta := range []int{1, 2, 3, 4, 5, 6, 7, -1} {
		key := fmt.Sprintf("casgc.%d", gcdelta)
		executions[key] = &Execution{
			ID: "deploy-" + key,
			Template: templates["cas"],
			KeySize: 65535,
			NumServer: 8,
			NumClient: 4,
			ClientThreads: 4,
			WritePercentage: 5,
			Failures: 2,
			CASGCDelta: gcdelta,
			CASGCExpire: 30000,
			NumKeys: 1000,
		}
	}

	// comparison failure-free
	for _, algorithm := range []string{"ldr", "cas"} {
		for _, failures := range []int{2} {
			for _, n := range []int{8, 12, 16, 20} {
				key := fmt.Sprintf("comparenormal.%s.%d.%d", algorithm, failures, n)
				executions[key] = &Execution{
					ID: "deploy-" + key,
					Template: templates[algorithm],
					NumServer: n,
					NumClient: n / 2,
					ClientThreads: 4,
					WritePercentage: 25,
					Failures: failures,
					LDRGossip: true,
					CASGCDelta: -1,
					KeySize: 65535,
				}
			}
		}
	}

	// comparison failures
	for _, algorithm := range []string{"ldr", "cas"} {
		for _, actualFailures := range []int{0, 1, 2, 3} {
			for _, n := range []int{16} {
				key := fmt.Sprintf("comparefail.%s.%d.%d", algorithm, actualFailures, n)
				executions[key] = &Execution{
					ID: "deploy-" + key,
					Template: templates[algorithm],
					NumServer: n,
					NumClient: n / 2,
					ClientThreads: 4,
					WritePercentage: 5,
					Failures: 3,
					ActualFailures: actualFailures,
					LDRGossip: true,
				}
			}
		}
	}

	// comparison increased communication latency
	for _, algorithm := range []string{"ldr", "cas"} {
		for _, n := range []int{8, 12, 16, 20} {
			key := fmt.Sprintf("comparedivide.%s.%d", algorithm, n)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates[algorithm],
				NumServer: n,
				NumClient: n / 2,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				MultiRegion: "divide",
				LDRGossip: true,
			}
		}
	}

	// comparison multi-region
	for _, algorithm := range []string{"ldr", "cas"} {
		for _, n := range []int{8, 12, 16, 20} {
			key := fmt.Sprintf("comparemix.%s.%d", algorithm, n)
			executions[key] = &Execution{
				ID: "deploy-" + key,
				Template: templates[algorithm],
				NumServer: n,
				NumClient: n / 2,
				ClientThreads: 4,
				WritePercentage: 5,
				Failures: 2,
				MultiRegion: "mix",
				LDRGossip: true,
			}
		}
	}

	serversInUse := 0
	availableServers := 100
	var mu sync.Mutex

	type ResultCount struct {
		Count int
		Result *Result
	}
	resultMap := make(map[string]*ResultCount)

	log.Printf("%d executions with %d trials", len(executions), NUM_TRIALS)

	for trial := 0; trial < NUM_TRIALS; trial++ {
		log.Printf("starting trial %d of %d", trial + 1, NUM_TRIALS)
		for key, execution := range executions {
			// restrict number of simultaneous runs
			for {
				mu.Lock()
				if serversInUse <= availableServers - execution.NumServer - execution.NumClient {
					serversInUse += execution.NumServer + execution.NumClient
					mu.Unlock()
					break
				}
				mu.Unlock()
				time.Sleep(time.Second)
			}

			go func(key string, execution *Execution) {
				defer func() {
					mu.Lock()
					serversInUse -= execution.NumServer + execution.NumClient
					mu.Unlock()
				}()
				result, err := execution.Run(factory)
				if err != nil {
					log.Printf("execution failed: %s", err.Error())
					return
				}

				if resultMap[key] == nil {
					resultMap[key] = &ResultCount{1, result}
				} else {
					resultMap[key].Count++
					resultMap[key].Result.Storage += result.Storage
					resultMap[key].Result.Communication += result.Communication
					resultMap[key].Result.Throughput += result.Throughput
					resultMap[key].Result.WriteLatency += result.WriteLatency
					resultMap[key].Result.ReadLatency += result.ReadLatency
				}
			}(key, execution)
		}
	}

	// wait for executions to complete
	for {
		mu.Lock()
		if serversInUse == 0 {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(time.Second)
	}

	for key, result := range resultMap {
		count := result.Count
		result := result.Result
		log.Printf("[%s] storage: %d; communication: %d; throughput: %.2f/s; write latency: %.2f; read latency: %.2f", key, result.Storage / uint64(count), result.Communication / uint64(count), result.Throughput / float64(count), result.WriteLatency / float64(count), result.ReadLatency / float64(count))
	}
}
