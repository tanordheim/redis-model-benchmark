package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

const keyBaseName = "benchmark"
const numberOfItems = 100000
const appendVsPrependPct = 30
const blobSize = 1000
const itemsToRemoveByKey = 100

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	cleanup(client)

	runAppendAndPrependBenchmark(client)
	fmt.Printf("\n")
	runRemoveByCoalesceKeyBenchmark(client)
}

func cleanup(client *redis.Client) {
	client.FlushDb()
}

func buildRandomBlob(size int64) []byte {
	blob := make([]byte, size)
	rand.Read(blob)
	return blob
}

func runAppendAndPrependBenchmark(client *redis.Client) {
	fmt.Printf("Running append/prepend benchmark by inserting %d items (%d byte blobs), %d%% prepends\n", numberOfItems, blobSize, appendVsPrependPct)

	start := time.Now()
	ts := time.Now().UnixNano()

	for i := 0; i < numberOfItems; i++ {
		coalesceKey := fmt.Sprintf("coalesce_%d", i)
		coalesceKeyName := fmt.Sprintf("%s:coalesce:%s", keyBaseName, coalesceKey)
		payload := buildRandomBlob(blobSize)
		isPrepend := i%100 < appendVsPrependPct
		setSuffix := "append"
		if isPrepend {
			setSuffix = "prepend"
		}
		setName := fmt.Sprintf("%s:%s", keyBaseName, setSuffix)

		_, err := client.ZAdd(setName, redis.Z{float64(ts), payload}).Result()
		if err != nil {
			panic(err)
		}

		_, err = client.Set(coalesceKeyName, fmt.Sprintf("%s:%d", setSuffix, ts), 0).Result()
		if err != nil {
			panic(err)
		}
		ts += 10000
	}

	elapsed := time.Since(start)
	fmt.Printf("Appending/prepending %d items took %s, average duration was %s\n", numberOfItems, elapsed, elapsed/numberOfItems)
}

func runRemoveByCoalesceKeyBenchmark(client *redis.Client) {
	fmt.Printf("Running remove by coalesce key benchmark by removing %d items in random locations\n", itemsToRemoveByKey)
	start := time.Now()
	currentCount := numberOfItems

	for i := 0; i < itemsToRemoveByKey; i++ {
		removeIdx := rand.Intn(currentCount)
		coalesceKeyName := fmt.Sprintf("%s:coalesce:coalesce_%d", keyBaseName, removeIdx)

		val, err := client.Get(coalesceKeyName).Result()
		if err != nil {
			panic(err)
		}

		parts := strings.Split(val, ":")
		setName := fmt.Sprintf("%s:%s", keyBaseName, parts[0])
		rangeVal := parts[1]
		removed, err := client.ZRemRangeByScore(setName, rangeVal, rangeVal).Result()
		if err != nil {
			panic(err)
		}
		if removed != 1 {
			fmt.Printf("WARN: Expected 1 item to be removed with range %s from %s, got %d\n", rangeVal, setName, removed)
		}

		currentCount--
	}

	elapsed := time.Since(start)
	fmt.Printf("Removing %d items by coalesce key took %s, average duration was %s\n", itemsToRemoveByKey, elapsed, elapsed/itemsToRemoveByKey)

	appCount, err := client.ZCount(fmt.Sprintf("%s:append", keyBaseName), "-inf", "+inf").Result()
	if err != nil {
		panic(err)
	}
	prepCount, err := client.ZCount(fmt.Sprintf("%s:prepend", keyBaseName), "-inf", "+inf").Result()
	if err != nil {
		panic(err)
	}
	if appCount+prepCount != int64(currentCount) {
		fmt.Printf("WARN: Expected %d items to be left, got %d (%d append and %d prepend)\n", currentCount, appCount+prepCount, appCount, prepCount)
	}
}
