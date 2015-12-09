// Helper script to provision instances for use with deploy tool

package main

import "flag"
import "fmt"
import "log"
import "time"

import "github.com/LunaNode/lobster/vmi/lunanode"

const CONCURRENT_PROVISIONS = 10

func main() {
	count := flag.Int("count", 5, "number of instances to provision")
	apiId := flag.String("id", "", "LunaNode API ID")
	apiKey := flag.String("key", "", "LunaNode API key")
	imageID := flag.Int("image", 0, "source image ID")
	planID := flag.Int("plan", 1, "plan ID to use")
	region := flag.String("region", "toronto", "target region")
	startIndex := flag.Int("startindex", 1, "instances are labeled s{INDEX}; this is the index of the first server")
	flag.Parse()

	if *count == 0 {
		flag.Usage()
		log.Fatalf("instance count is required")
	} else if *apiId == "" || *apiKey == "" {
		flag.Usage()
		log.Fatalf("API details are required")
	} else if *imageID == 0 {
		flag.Usage()
		log.Fatalf("image ID is required")
	}

	api, err := lunanode.MakeAPI(*apiId, *apiKey)
	if err != nil {
		log.Fatalf("api initialization error: %s", err.Error())
	}

	// provision CONCURRENT_PROVISIONS at a time
	for i := 0; i < *count; i += CONCURRENT_PROVISIONS {
		num := CONCURRENT_PROVISIONS
		if *count - i < num {
			num = *count - i
		}
		ch := make(chan string)
		for j := 0; j < num; j++ {
			go func(index int) {
				vmID, err := api.VmCreateImage(*region, fmt.Sprintf("s%d", index), *planID, *imageID)
				if err != nil {
					ch <- "provision error: " + err.Error()
					return
				}
				startTime := time.Now()
				for time.Now().Before(startTime.Add(10 * time.Minute)) {
					time.Sleep(time.Minute)
					info, err := api.VmInfo(vmID)
					if err != nil {
						continue
					} else if info.Ip != "" {
						ch <- info.Ip
					}
				}
			}(*startIndex + i + j)
		}
		for j := 0; j < num; j++ {
			ip := <- ch
			fmt.Println(ip)
		}
	}
}
