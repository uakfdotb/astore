package main

import "fmt"
import "log"
import "math/rand"
import "strconv"
import "strings"
import "time"

type Execution struct {
	ID string
	Template *Template
	MultiRegion string // empty to disable, "divide" to split clients and servers, "mix" to evenly distribute clients and servers
	NumServer int
	ServerThreads int
	NumClient int
	ClientThreads int
	WritePercentage int
	Failures int
	ActualFailures int
	PacketLoss int
	NumKeys int
	KeySize int
	Distribution string

	LDRReadAllSimultaneously bool
	LDRReplicateAll bool
	LDRGossip bool

	CASRetriesAfterDone int
	CASGCDelta int
	CASGCExpire int

	NoPreferences bool // disables setting preferred servers in same region
}

type Result struct {
	Storage uint64
	Communication uint64
	Throughput float64
	WriteLatency float64
	ReadLatency float64
}

func (execution *Execution) Run(factory *ServerFactory) (*Result, error) {
	servers, clients, err := factory.Provision(execution)
	if err != nil {
		log.Printf("[%s] [run] provision failed: %s", execution.ID, err.Error())
		return nil, err
	}

	var instances []*Server
	instances = append(servers, servers...)
	instances = append(servers, clients...)
	defer func(instances []*Server) {
		for _, instance := range instances {
			instance.Delete()
		}
	}(instances)

	time.Sleep(5 * time.Second)

	log.Printf("[%s] [run] warming up servers", execution.ID)
	warmupResult, err := execution.round(servers, clients[0:1], true)
	if err != nil {
		return nil, err
	}
	log.Printf("[%s] [run] running benchmark", execution.ID)
	result, err := execution.round(servers, clients, false)

	// correct communication
	// 1) subtract server communication used doing warmup
	// 2) normalize to 100 transactions / second
	result.Communication -= warmupResult.Communication
	result.Communication = uint64(float64(result.Communication) * 100 / result.Throughput)

	if err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func (execution *Execution) round(servers []*Server, clients []*Server, warmup bool) (*Result, error) {
	writePercentage := execution.WritePercentage
	actualFailures := execution.ActualFailures
	packetLoss := execution.PacketLoss
	if warmup {
		writePercentage = 100
		actualFailures = 0
		packetLoss = 0
	}


	// get server location string, both global and per-region
	var serverLocations []string
	regionLocations := make(map[int][]string)
	for _, server := range servers {
		serverLocations = append(serverLocations, server.IP + ":9224")
		regionLocations[server.Region] = append(regionLocations[server.Region], server.IP + ":9224")
	}

	// run
	for _, client := range clients {
		for threadIndex := 0; threadIndex < execution.ClientThreads; threadIndex++ {
			cmd := execution.Template.ClientCommand
			cmd += fmt.Sprintf(" -servers=%s", strings.Join(serverLocations, ","))
			if len(regionLocations[client.Region]) > 0 && !execution.NoPreferences {
				cmd += fmt.Sprintf(" -preferred=%s", strings.Join(regionLocations[client.Region], ","))
			}
			cmd += fmt.Sprintf(" -failures %d", execution.Failures)
			cmd += fmt.Sprintf(" -packetloss %d", packetLoss)
			cmd += fmt.Sprintf(" -writeperc %d", writePercentage)
			if execution.KeySize != 0 {
				cmd += fmt.Sprintf(" -keysize %d", execution.KeySize)
			}
			if execution.NumKeys != 0 {
				cmd += fmt.Sprintf(" -numkeys %d", execution.NumKeys)
			}
			if execution.Distribution != "" {
				cmd += fmt.Sprintf(" -distribution %s", execution.Distribution)
			}
			if execution.LDRReadAllSimultaneously {
				cmd += " -ldrreadall=true"
			}
			if execution.LDRReplicateAll {
				cmd += " -ldrreplicateall=true"
			}
			if execution.CASRetriesAfterDone > 0 {
				cmd += fmt.Sprintf(" -casretries=%d", execution.CASRetriesAfterDone)
			}
			cmd += " &> /home/ubuntu/outTHREAD_INDEX.txt"
			cmd = strings.Replace(cmd, "THREAD_INDEX", fmt.Sprintf("%d", threadIndex), -1)
			client.Run(fmt.Sprintf("screen -d -m -S run bash -c '%s'", cmd))
		}
	}

	time.Sleep(10 * time.Second)

	// for next five minutes (benchmark duration), fail servers as needed
	startTime := time.Now()

	if actualFailures > 0 {
		failedServers := make(map[int]bool)
		for time.Now().Before(startTime.Add(2 * time.Minute)) {
			if len(failedServers) < actualFailures && rand.Intn(2) == 0 {
				var randIndex int
				for {
					randIndex = rand.Intn(len(servers))
					if !failedServers[randIndex] {
						break
					}
				}
				servers[randIndex].Run("sudo iptables -A INPUT -p tcp --destination-port 9224 -j DROP")
				failedServers[randIndex] = true
			} else if len(failedServers) > 0 {
				var selectedIndex int
				for index := range failedServers {
					selectedIndex = index
					break
				}
				servers[selectedIndex].Run("sudo iptables -D INPUT 1")
				delete(failedServers, selectedIndex)
			}
			time.Sleep(3 * time.Second)
		}
		for serverIndex := range failedServers {
			servers[serverIndex].Run("sudo iptables -D INPUT 1")
		}
	}

	// aggregate the benchmarks
	var result Result

	for _, client := range clients {
		for threadIndex := 0; threadIndex < execution.ClientThreads; threadIndex++ {
			var currentResult *Result
			for time.Now().Before(startTime.Add(10 * time.Minute)) {
				contents := strings.TrimSpace(string(client.Run(fmt.Sprintf("grep 'bench;' /home/ubuntu/out%d.txt | tail -n 1", threadIndex))))
				if strings.Contains(contents, "bench;") {
					log.Printf("got result from %s-%d: %s", client.IP, threadIndex, contents)
					currentResult = new(Result)
					sections := strings.Split(contents, "; ")
					for _, section := range sections {
						parts := strings.Split(section, "=")
						if parts[0] == "throughput" {
							currentResult.Throughput, _ = strconv.ParseFloat(parts[1], 64)
						} else if parts[0] == "writelatency" {
							currentResult.WriteLatency, _ = strconv.ParseFloat(parts[1], 64)
						} else if parts[0] == "readlatency" {
							currentResult.ReadLatency, _ = strconv.ParseFloat(parts[1], 64)
						}
					}
					break
				}
				time.Sleep(3 * time.Second)
			}
			if currentResult == nil {
				return nil, fmt.Errorf("failed to obtain out%d.txt on %s after 10 minutes", threadIndex, client.IP)
			}
			result.Throughput += currentResult.Throughput
			result.WriteLatency += currentResult.WriteLatency
			result.ReadLatency += currentResult.ReadLatency
		}
	}

	result.WriteLatency /= float64(len(clients) * execution.ClientThreads)
	result.ReadLatency /= float64(len(clients) * execution.ClientThreads)

	// we only include client storage/communication on the actual benchmark
	// this is ignored for warmup because we will subtract warmup communication
	//    from the communication reported after actual benchmark
	if !warmup {
		for _, client := range clients {
			for threadIndex := 0; threadIndex < execution.ClientThreads; threadIndex++ {
				var currentResult *Result
				for time.Now().Before(startTime.Add(10 * time.Minute)) {
					contents := strings.TrimSpace(string(client.Run(fmt.Sprintf("grep 'accounting;' /home/ubuntu/out%d.txt | tail -n 1", threadIndex))))
					if strings.Contains(contents, "accounting") {
						log.Printf("got result from %s: %s", client.IP, contents)
						currentResult = new(Result)
						sections := strings.Split(contents, "; ")
						for _, section := range sections {
							parts := strings.Split(section, "=")
							if parts[0] == "memory" {
								currentResult.Storage, _ = strconv.ParseUint(parts[1], 10, 64)
							} else if parts[0] == "network" {
								currentResult.Communication, _ = strconv.ParseUint(parts[1], 10, 64)
							}
						}
						break
					}
					time.Sleep(3 * time.Second)
				}
				if currentResult == nil {
					return nil, fmt.Errorf("failed to obtain out%d.txt on %s after 10 minutes", threadIndex, client.IP)
				}
				result.Storage += currentResult.Storage
				result.Communication += currentResult.Communication
			}
		}
	}

	for _, server := range servers {
		var currentResult *Result
		for time.Now().Before(startTime.Add(10 * time.Minute)) {
			contents := strings.TrimSpace(string(server.Run("grep 'accounting;' /home/ubuntu/log.txt | tail -n 10")))
			if strings.Contains(contents, "accounting") {
				currentResult = new(Result)
				// take maximum memory and last network from the lines
				// we want maximum memory because server may have garbage collected after benchmark finished
				for _, line := range strings.Split(contents, "\n") {
					sections := strings.Split(line, "; ")
					for _, section := range sections {
						parts := strings.Split(section, "=")
						if parts[0] == "memory" {
							storage, _ := strconv.ParseUint(parts[1], 10, 64)
							if storage > currentResult.Storage {
								currentResult.Storage = storage
							}
						} else if parts[0] == "network" {
							currentResult.Communication, _ = strconv.ParseUint(parts[1], 10, 64)
						}
					}
				}
				break
			}
			time.Sleep(3 * time.Second)
		}
		if currentResult == nil {
			return nil, fmt.Errorf("failed to obtain log.txt on %s after 10 minutes", server.IP)
		}
		result.Storage += currentResult.Storage
		result.Communication += currentResult.Communication
	}

	return &result, nil
}
