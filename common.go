package main

import "hash/crc32"
import "io/ioutil"
import "log"
import "strconv"
import "strings"

const DEFAULT_SERVER_PORT = 9224

type Client interface {
	Read(key string) []byte
	Write(key string, bytes []byte)
}

// hashes a string to natural numbers up to and excluding n
func hash(s string, n int) int {
	hasher := crc32.NewIEEE()
	hasher.Write([]byte(s))
	return int(hasher.Sum32() % uint32(n))
}

func getNetworkUsage() uint64 {
	var networkUsage uint64
	for _, filename := range []string{"/sys/class/net/eth0/statistics/rx_bytes", "/sys/class/net/eth0/statistics/tx_bytes"} {
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Printf("error reading %s for network stats: %s", filename, err.Error())
			continue
		}
		count, _ := strconv.ParseUint(strings.TrimSpace(string(contents)), 10, 64)
		networkUsage += count
	}
	return networkUsage
}
