package streamgorate_test

import (
	"testing"
	"time"

	. "github.com/GetStream/go-ratelimiting"
	"fmt"
	"strconv"
	"bytes"
	"math/rand"
)

func init() {
	RedisClient = NewRedisPool("localhost:6379", "")
}

func key2string(prefix, id string) string {
	buf := bytes.NewBufferString(prefix)
	buf.WriteString(":")
	buf.WriteString(id)
	return buf.String()
}


func TestIncr(t *testing.T) {

	prefix := "stream:test:counter:counter"
	counter := NewCounter(RedisClient, prefix, 1*time.Minute, 1*time.Second)

	randomKey := randkey()
	if err := counter.Reset(randomKey); err != nil {
		t.Fatal("can not reset counter", err)
	}

	randValue := rand.Int()
	cmd := RedisClient.Set(randomKey, randValue, 2*time.Second)
	if cmd.Err() != nil {
		t.Fatal(cmd.Err())
	}
	value1, err := RedisClient.Get(randomKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("value 1", value1)
	if value1 != strconv.Itoa(randValue) {
		t.Fatal("didn't get back the right number")
	}

	now := time.Now().UnixNano()
	bucket := int(now/int64(1)) % 120
	bucketStr := strconv.Itoa(bucket)
	//fmt.Println(bucket)
	keyStr := key2string(prefix, randomKey)

	pipe := RedisClient.TxPipeline()
	defer pipe.Close()
	pipe.HSet(keyStr, bucketStr, randValue)
	pipe.PExpire(keyStr, 5*time.Second)
	_, err = pipe.Exec()
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("val", intCmd.Val())
	getCmd := RedisClient.HGetAll(keyStr)
	value2, err := getCmd.Result()
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("value 2", value2[bucketStr])


	pipe2 := RedisClient.TxPipeline()
	defer pipe2.Close()
	pipe2.HIncrBy(keyStr, bucketStr, 1)
	pipe.PExpire(keyStr, 5*time.Second)
	_, err = pipe2.Exec()
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("val", intCmd2.Val())

	getCmd2 := RedisClient.HGetAll(keyStr)
	value3, err := getCmd2.Result()
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Println("value 3", value3[bucketStr])

	if value1 == value3[bucketStr] {
		fmt.Printf("value1 == value2 (%#v == %#v)", value1, value2)
		t.Fatal()
	}
}
