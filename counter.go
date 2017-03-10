package streamgorate

import (
	"bytes"
	"strconv"
	"time"

	"gopkg.in/redis.v5"

	"fmt"
	"errors"
)

// Counter count total occurs during a period,
// it will store occurs during every time slice interval: (now ~ now - interval), (now - interval ~ now - 2*interval)...
type Counter struct {
	client   *redis.Client
	pfx      string
	period   time.Duration
	interval time.Duration
	bkts     int
}

// NewCounter create a new Counter
func NewCounter(client *redis.Client, prefix string, period, interval time.Duration) *Counter {
	return &Counter{
		client:   client,
		pfx:      prefix,
		period:   period,
		interval: interval,
		bkts:     int(period/interval) * 2,
	}
}

// hash a time to n buckets(n=c.bkts)
func (c *Counter) hash(t int64) int {
	return int(t/int64(c.interval)) % c.bkts
}

func (c *Counter) key(id string) string {
	buf := bytes.NewBufferString(c.pfx)
	buf.WriteString(":")
	buf.WriteString(id)
	return buf.String()
}

// increment count in specific bucket
func (c *Counter) inc(id string, bucket int) error {
	keysToDelete := make([]string, int(c.bkts/2))
	for i := 0; i < c.bkts/2; i++ {
		keyToDelete := strconv.Itoa(int(bucket+i+1) % c.bkts)
		keysToDelete[i] = keyToDelete
	}

	pipe := c.client.TxPipeline()
	defer pipe.Close()

	pipe.HIncrBy(c.key(id), strconv.Itoa(bucket), 1)
	pipe.HDel(c.key(id), keysToDelete...)
	expiry := int64(c.period)
	pipe.PExpire(c.key(id), time.Duration(expiry))
	_, err := pipe.Exec()
	pipe.Save()

	return err
}

// Inc increment id's occurs with current timestamp,
// the count before period will be cleanup
func (c *Counter) Inc(id string) error {
	now := time.Now().UnixNano()
	bucket := c.hash(now)
	return c.inc(id, bucket)
}

// return available buckets
func (c *Counter) buckets(from int) []int {
	len := c.bkts / 2
	rs := make([]int, len)
	for i := 0; i < len; i++ {
		rs[i] = (c.bkts + from - i) % c.bkts
	}
	return rs
}

func (c *Counter) histogram(id string, from int) ([]int64, error) {
	buckets := c.buckets(from)
	args := make([]string, len(buckets))
	for i, v := range buckets {
		args[i] = strconv.Itoa(v)
	}

	result, err := c.client.HMGet(c.key(id), args...).Result()
	vals, err := Strings(result, err)
	if err != nil {
		return []int64{}, err
	}

	ret := make([]int64, len(buckets))
	for i, val := range vals {
		v, err := strconv.ParseInt(val, 10, 64)
		if err == nil {
			ret[i] = v
		} else {
			ret[i] = 0
		}
	}
	return ret, nil
}

// Histogram return count histogram in recent period, order by time desc
func (c *Counter) Histogram(id string) ([]int64, error) {
	now := time.Now().UnixNano()
	from := c.hash(now)
	return c.histogram(id, from)
}

// Count return total occurs in recent period
func (c *Counter) Count(id string) (int64, error) {
	h, err := c.Histogram(id)
	if err != nil {
		return 0, err
	}

	total := int64(0)
	for _, v := range h {
		total += v
	}
	return total, nil
}

// Reset cleanup occurs, set it to zero
func (c *Counter) Reset(id string) error {
	_, err := c.client.Del(c.key(id)).Result()
	return err
}


// all code below copied from github.com/garyburd/redigo/redis/
var ErrNil = errors.New("streamgorate: nil returned")
type Error string
func (err Error) Error() string { return string(err) }

func Strings(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	switch reply := reply.(type) {
	case []interface{}:
		result := make([]string, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			//p, ok := reply[i].([]byte)
			//if !ok {
			//	fmt.Println("Strings return 2")
			//	return nil, fmt.Errorf("go-ratelimiting-counter-Strings: unexpected element type for Strings, got type %T", reply[i])
			//}
			result[i] = reply[i].(string)
		}
		return result, nil
	case nil:
		return nil, ErrNil
	case Error:
		return nil, reply
	}
	return nil, fmt.Errorf("go-ratelimiting-counter-Strings: unexpected type for Strings, got type %T", reply)
}
