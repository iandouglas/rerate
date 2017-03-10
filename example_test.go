package streamgorate_test

import (
	"fmt"
	"time"

	"github.com/GetStream/go-ratelimiting"
	"gopkg.in/redis.v5"
)

func NewRedisPool(server, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func ExampleCounter() {
	redis := NewRedisPool("localhost:6379", "")
	key := "pv-home"
	// pv count in 5s, try to release per 0.5s
	counter := streamgorate.NewCounter(redis, "getstream:test:count", 5*time.Second, 500*time.Millisecond)
	counter.Reset(key)

	ticker := time.NewTicker(1000 * time.Millisecond)
	go func() {
		for _ = range ticker.C {
			counter.Inc(key)
		}
	}()

	time.Sleep(4500 * time.Millisecond)
	ticker.Stop()
	total, _ := counter.Count(key)
	his, _ := counter.Histogram(key)
	fmt.Println("total:", total, ", histogram:", his)
	//Output: total: 4 , histogram: [0 1 0 1 0 1 0 1 0 0]
}

func ExampleLimiter() {
	redis := NewRedisPool("localhost:6379", "")
	key := "pv-dashboard"
	// rate limit to 10/2s, release interval 0.2s
	limiter := streamgorate.NewLimiter(redis, "getstream:test:limit", 2*time.Second, 200*time.Millisecond, 10)
	limiter.Reset(key)

	ticker := time.NewTicker(200 * time.Millisecond)

	go func() {
		for _ = range ticker.C {
			limiter.Inc(key)
			if exceed, _ := limiter.Exceeded(key); exceed {
				ticker.Stop()
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 20; i++ {
		time.Sleep(200 * time.Millisecond)

		if exceed, _ := limiter.Exceeded(key); exceed {
			fmt.Println("exceeded")
		} else {
			rem, _ := limiter.Remaining(key)
			fmt.Println("remaining", rem)
		}
	}
	//Output:
	// remaining 9
	// remaining 8
	// remaining 7
	// remaining 6
	// remaining 5
	// remaining 4
	// remaining 3
	// remaining 2
	// remaining 1
	// exceeded
	// remaining 1
	// remaining 2
	// remaining 3
	// remaining 4
	// remaining 5
	// remaining 6
	// remaining 7
	// remaining 8
	// remaining 9
	// remaining 10
}
