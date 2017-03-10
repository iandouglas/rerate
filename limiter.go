package streamgorate

import (
	"time"
	"gopkg.in/redis.v5"
)

// Limiter a redis-based ratelimiter
type Limiter struct {
	Client *redis.Client
	Counter
	max    int64
}

// NewLimiter create a new redis-based ratelimiter
// the Limiter limits the rate to max times per period
func NewLimiter(client *redis.Client, pfx string, period, interval time.Duration, max int64) *Limiter {
	return &Limiter{
		Client:  client,
		Counter: *NewCounter(client, pfx, period, interval),
		max:     max,
	}
}

// Remaining return the number of requests left for the time window
func (l *Limiter) Remaining(id string) (int64, error) {
	occurs, err := l.Count(id)
	if err != nil {
		return 0, err
	}
	return l.max - occurs, nil
}

// Exceeded is exceeded the rate limit or not
func (l *Limiter) Exceeded(id string) (bool, error) {
	rem, err := l.Remaining(id)
	if err != nil {
		return false, err
	}
	return rem <= 0, nil
}
