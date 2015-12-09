package main

import "fmt"
import "io/ioutil"
import "log"
import "strconv"
import "strings"
import "sync"
import "time"

import "golang.org/x/crypto/ssh"

type Server struct {
	factory *ServerFactory
	IP string
	Client *ssh.Client
	Region int
}

func (server *Server) Run(cmd string) []byte {
	session, err := server.Client.NewSession()
	if err != nil {
		log.Fatalf("error while running [%s]: %s", cmd, err.Error())
	}
	bytes, err := session.Output(cmd)
	if err != nil {
		log.Fatalf("error while running [%s]: %s", cmd, err.Error())
	}
	session.Close()
	return bytes
}

func (server *Server) Clean() {
	server.Run("rm -f /home/ubuntu/*")
	server.Run("sudo bash -c 'killall screen || true'")
	server.Run("sudo bash -c 'iptables -F INPUT || true'")
}

func (server *Server) Delete() {
	server.Clean()
	server.Client.Conn.Close()
	server.factory.mu.Lock()
	server.factory.freeServers[server.Region] = append(server.factory.freeServers[server.Region], server.IP)
	server.factory.mu.Unlock()
}

type ServerFactory struct {
	freeServers map[int][]string // map from region to IP addresses
	mu sync.Mutex
}

func MakeServerFactory() *ServerFactory {
	factory := new(ServerFactory)
	factory.freeServers = make(map[int][]string)
	for _, serverString := range cfg.Servers.Servers {
		parts := strings.Split(serverString, ":")
		if len(parts) == 2 {
			region, _ := strconv.Atoi(parts[0])
			factory.freeServers[region] = append(factory.freeServers[region], parts[1])
		}

	}
	return factory
}

func (factory *ServerFactory) getServer(region int) (*Server, error) {
	var ip string
	for {
		factory.mu.Lock()
		if len(factory.freeServers[region]) > 0 {
			ip = factory.freeServers[region][0]
			factory.freeServers[region] = factory.freeServers[region][1:]
			factory.mu.Unlock()
			break
		}
		factory.mu.Unlock()
		time.Sleep(time.Second)
	}
	log.Printf("... attempting to SSH to %s", ip)
	startTime := time.Now()
	for time.Now().Before(startTime.Add(5 * time.Minute)) {
		client, err := func() (*ssh.Client, error) {
			pemBytes, err := ioutil.ReadFile(cfg.SSH.PrivateKeyPath)
			if err != nil {
				return nil, err
			}
			signer, err := ssh.ParsePrivateKey(pemBytes)
			if err != nil {
				return nil, err
			}
			config := &ssh.ClientConfig{
				User: "ubuntu",
				Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
			}
			client, err := ssh.Dial("tcp", ip + ":22", config)
			if err != nil {
				return nil, err
			}
			return client, nil
		}()
		if err != nil {
			log.Printf("SSH error to %s: %s", ip, err.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		server := &Server{
			factory: factory,
			IP: ip,
			Client: client,
			Region: region,
		}
		server.Clean()
		return server, nil
	}
	return nil, fmt.Errorf("failed to SSH after five minutes")
}

func (factory *ServerFactory) Provision(execution *Execution) ([]*Server, []*Server, error) {
	template := execution.Template

	type AsyncResult struct {
		Server *Server
		Error error
	}

	// servers
	log.Printf("[%s] [provision] creating servers", execution.ID)
	var servers []*Server
	var serverLocations []string
	serverResults := make(chan AsyncResult)

	for i := 0; i < execution.NumServer; i++ {
		go func(i int) {
			region := 0
			if execution.MultiRegion == "mix" && i >= execution.NumServer / 2 {
				region = 1
			}
			server, err := factory.getServer(region)
			if err != nil {
				serverResults <- AsyncResult{nil, err}
				return
			}

			time.Sleep(2 * time.Second)
			server.Run("wget -O archive.zip " + template.ArchiveURL)
			server.Run("unzip archive.zip")

			serverResults <- AsyncResult{server, nil}
		}(i)
	}

	for i := 0; i < execution.NumServer; i++ {
		result := <- serverResults
		if result.Error != nil {
			return nil, nil, result.Error
		}
		servers = append(servers, result.Server)
		serverLocations = append(serverLocations, result.Server.IP + ":9224")
	}

	for _, server := range servers {
		cmd := template.ServerCommand
		cmd += fmt.Sprintf(" -servers=%s", strings.Join(serverLocations, ","))
		if execution.LDRGossip {
			cmd += " -ldrgossip=true"
		}
		if execution.CASGCDelta != 0 {
			cmd += fmt.Sprintf(" -casgcdelta=%d", execution.CASGCDelta)
		}
		if execution.CASGCExpire > 0 {
			cmd += fmt.Sprintf(" -casgcexpire=%d", execution.CASGCExpire)
		}
		cmd += " &> /home/ubuntu/log.txt"
		server.Run(fmt.Sprintf("screen -d -m -S run bash -c '%s'", cmd))
	}

	// client servers
	log.Printf("[%s] [provision] creating client servers", execution.ID)
	var clients []*Server
	clientResults := make(chan AsyncResult)

	for i := 0; i < execution.NumClient; i++ {
		go func(i int) {
			region := 0
			if execution.MultiRegion == "divide" || (execution.MultiRegion == "mix" && i >= execution.NumClient / 2) {
				region = 1
			}
			server, err := factory.getServer(region)
			if err != nil {
				clientResults <- AsyncResult{nil, err}
				return
			}

			time.Sleep(2 * time.Second)
			server.Run("wget -O archive.zip " + template.ArchiveURL)
			server.Run("unzip archive.zip")

			clientResults <- AsyncResult{server, nil}
		}(i)
	}

	for i := 0; i < execution.NumClient; i++ {
		result := <- clientResults
		if result.Error != nil {
			return nil, nil, result.Error
		}
		clients = append(clients, result.Server)
	}

	return servers, clients, nil
}
