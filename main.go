package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const insertTestKey = "benchmark:insert"

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	cleanup(client)
	runInsertBenchmark(client)
}

func cleanup(client *redis.Client) {
	for _, k := range []string{insertTestKey} {
		var cursor uint64
		for {
			var keys []string
			var err error
			keys, cursor, err = client.Scan(cursor, fmt.Sprintf("%s:*", k), 1000).Result()
			if err != nil {
				panic(err)
			}
			client.Del(keys...)
			if cursor == 0 {
				break
			}
		}
	}
}

func buildRandomBlob(size int64) []byte {
	blob := make([]byte, size)
	rand.Read(blob)
	return blob
}

func runInsertBenchmark(client *redis.Client) {
	const numberOfItems = 100000
	const appendVsPrependPct = 30
	const blobSize = 1000
	fmt.Printf("Running insert benchmark by inserting %d items (%d byte blobs), %d%% prepend\n", numberOfItems, blobSize, appendVsPrependPct)

	var wg sync.WaitGroup
	wg.Add(numberOfItems)

	start := time.Now()
	ts := time.Now().UnixNano()
	for i := 0; i < numberOfItems; i++ {
		metaKey := fmt.Sprintf("%s:meta", insertTestKey)
		blobKey := fmt.Sprintf("%s:payload:%d", insertTestKey, ts)
		payload := []byte(fmt.Sprintf("%d %s", ts, fmt.Sprintf("coalesce_%d", i)))
		isPrepend := i%100 < appendVsPrependPct

		go func() {
			defer wg.Done()

			// Append/prepend the metadata
			if isPrepend {
				if err := client.LPush(metaKey, payload).Err(); err != nil {
					panic(err)
				}
			} else {
				if err := client.RPush(metaKey, payload).Err(); err != nil {
					panic(err)
				}
			}

			// Insert the binary data
			if err := client.Set(blobKey, buildRandomBlob(blobSize), 0).Err(); err != nil {
				fmt.Printf("blob err: %v\n", err)
				panic(err)
			}

		}()

		ts += 100
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Inserting %d items took %s\n", numberOfItems, elapsed)
}
