package main

import "fmt"
import "math/rand"
import "sort"
import "time"

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type Durations []time.Duration

func (durations Durations) Len() int {
	return len(durations)
}

func (durations Durations) Less(i, j int) bool {
	return durations[i] < durations[j]
}

func (durations Durations) Swap(i, j int) {
	tmp := durations[i]
	durations[i] = durations[j]
	durations[j] = tmp
}

func latency99(array Durations) time.Duration {
	if len(array) > 0 {
		sort.Sort(array)
		return array[int(float64(len(array)) * 0.99)]
	} else {
		return 0
	}
}

func benchmark(client Client, zipfian bool, writePercent int, numKeys int, keySize int) (float64, time.Duration, time.Duration) {
	// if write percentage is 100, we actually just want warmup mode
	if writePercent == 100 {
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%d", i)
			client.Write(key, []byte(randStr(keySize)))
		}
		return 0, 0, 0
	}

	zipf := rand.NewZipf(rand.New(rand.NewSource(0)), 1.1, 1.0, uint64(numKeys - 1))
	benchStart := time.Now()
	var iterations int
	var writeDurations []time.Duration
	var readDurations []time.Duration
	for time.Now().Before(benchStart.Add(2 * time.Minute)) {
		iterationStart := time.Now()
		var key string
		if zipfian {
			key = fmt.Sprintf("key%d", zipf.Uint64())
		} else {
			key = fmt.Sprintf("key%d", rand.Intn(numKeys))
		}
		isWrite := rand.Intn(100) < writePercent
		if isWrite {
			data := []byte(randStr(keySize))
			client.Write(key, data)
		} else {
			client.Read(key)
		}
		iterationDuration := time.Now().Sub(iterationStart)
		if isWrite {
			writeDurations = append(writeDurations, iterationDuration)
		} else {
			readDurations = append(readDurations, iterationDuration)
		}
		iterations++
	}
	benchDuration := time.Now().Sub(benchStart)
	throughput := float64(iterations) / benchDuration.Seconds()
	return throughput, latency99(writeDurations), latency99(readDurations)
}
