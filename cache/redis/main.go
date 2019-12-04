package main

import (
	"fmt"

	pgredis "github.com/clhjoe/distributed_cache/cache/redis"

	"github.com/go-redis/redis"
)

var (
	//Ring is used in general purpose cache
	Ring *pgredis.Ring
)

func init() {
	rings := map[int]*pgredis.Client{}
	for i := 0; i < 4; i++ {
		rings[i] = pgredis.NewClient(&pgredis.Options{
			Addr: "127.0.0.1:6379",
		})
	}
	Ring = pgredis.NewRing(rings, "pg:")

}
func main() {
	res, err := Ring.PipelineMultiZRangeByScoreWithScores([]string{"key1", "key2", "key9", "key9", "non_key"}, redis.ZRangeBy{Min: "2", Max: "+inf", Offset: 0, Count: 10})
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(res)
}
