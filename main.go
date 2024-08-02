package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

//index		数据类型	        key
//sitePv	Str	            bsz:site_pv:md5(host)
//siteUv	set				bsz:site_uv:md5(host)
//pagePv	zset	        bsz:page_pv:md5(host) / md5(path)
//pageUv	set				bsz:site_uv:md5(host):md5(host&path)

// to

// index		数据类型	        key
// sitePv	Str	            bsz:site_pv:md5(host)
// siteUv	HyperLogLog		bsz:site_uv:md5(host)
// pagePv	zset	        bsz:page_pv:md5(host) / md5(path)
// pageUv	HyperLogLog		bsz:site_uv:md5(host):md5(host&path)

var RDB *redis.Client

func main() {
	// 询问 是否备份数据了
	var backup string
	fmt.Print("您是否已经备份了数据 (dump.rdb), 继续操作可能造成未知错误或影响 (yes/no): ")
	_, err := fmt.Scan(&backup)
	if err != nil {
		log.Fatalf("[ERROR] input failed: %v", err)
	}
	if backup != "yes" {
		log.Fatalf("[ERROR] 请先备份您的数据!")
	}

	initConfig()
	initRedis()

	// Transaction
	// 转移 siteUV
	var cur uint64 = 0
	var keys []string
	for {
		key := fmt.Sprintf("%s:site_uv:*", viper.GetString("redis.prefix"))
		result := RDB.Scan(context.Background(), cur, key, 100)
		if result.Err() != nil {
			log.Printf("[ERROR] Redis scan failed: %v", result.Err())
			continue
		}

		keys, cur = result.Val()
		// get siteUv keys
		if len(keys) > 0 {
			for _, ks := range keys {
				// log.Printf("[INFO] Redis scan keys: %v \r\n", keys)

				// check if set
				keyType := RDB.Type(context.Background(), ks)
				if keyType.Err() != nil {
					log.Printf("[ERROR] Redis Type failed: %v", keyType.Err())
					continue
				}
				if keyType.Val() != "set" {
					log.Printf("[INFO] Redis key %v is not set \r\n", ks)
					continue
				}

				// get siteUvs (set)
				pageUVs, err := RDB.SMembers(context.Background(), ks).Result()
				if err != nil {
					log.Printf("[ERROR] Redis SMembers failed: %v", err)
					continue
				}
				// transfer pageUV to pageUV (HyperLogLog)
				log.Printf("[INFO] transfer %v to HyperLogLog with %d members\r\n", ks, len(pageUVs))
				for _, pageUV := range pageUVs {
					k := strings.ReplaceAll(ks, viper.GetString("redis.prefix"), viper.GetString("redis.ToPrefix"))

					if err := RDB.PFAdd(context.Background(), k, pageUV).Err(); err != nil {
						log.Printf("[ERROR] Redis PFAdd failed: %v", err)
						continue
					}
				}
			}
		}

		if cur == 0 {
			break
		}
	}

	cur = 0
	// 转移 pageUV
	for {
		key := fmt.Sprintf("%s:page_uv:*", viper.GetString("redis.prefix"))
		result := RDB.Scan(context.Background(), cur, key, 100)
		if result.Err() != nil {
			log.Printf("[ERROR] Redis scan failed: %v", result.Err())
			continue
		}

		keys, cur = result.Val()
		// get pageUV keys
		if len(keys) > 0 {
			for _, ks := range keys {
				// log.Printf("[INFO] Redis scan keys: %v \r\n", keys)

				// check if set
				keyType := RDB.Type(context.Background(), ks)
				if keyType.Err() != nil {
					log.Printf("[ERROR] Redis Type failed: %v", keyType.Err())
					continue
				}
				if keyType.Val() != "set" {
					log.Printf("[INFO] Redis key %v is not set \r\n", ks)
					continue
				}

				// get pageUVs (set)
				pageUVs, err := RDB.SMembers(context.Background(), ks).Result()
				if err != nil {
					log.Printf("[ERROR] Redis SMembers failed: %v", err)
					continue
				}
				// transfer pageUV to pageUV (HyperLogLog)
				log.Printf("[INFO] transfer %v to HyperLogLog with %d members\r\n", ks, len(pageUVs))
				for _, pageUV := range pageUVs {
					k := strings.ReplaceAll(ks, viper.GetString("redis.prefix"), viper.GetString("redis.ToPrefix"))

					if err := RDB.PFAdd(context.Background(), k, pageUV).Err(); err != nil {
						log.Printf("[ERROR] Redis PFAdd failed: %v", err)
						continue
					}
				}
			}
		}

		if cur == 0 {
			break
		}
	}

	// 转移 sitePV (str)
	cur = 0
	for {
		key := fmt.Sprintf("%s:site_pv:*", viper.GetString("redis.prefix"))
		result := RDB.Scan(context.Background(), cur, key, 100)
		if result.Err() != nil {
			log.Printf("[ERROR] Redis scan failed: %v", result.Err())
			continue
		}

		keys, cur = result.Val()
		// get sitePV keys
		if len(keys) > 0 {
			for _, ks := range keys {
				// log.Printf("[INFO] Redis scan keys: %v \r\n", keys)

				// check if str
				keyType := RDB.Type(context.Background(), ks)
				if keyType.Err() != nil {
					log.Printf("[ERROR] Redis Type failed: %v", keyType.Err())
					continue
				}
				if keyType.Val() != "string" {
					log.Printf("[INFO] Redis key %v is not string \r\n", ks)
					continue
				}

				// get sitePV (str)
				res := RDB.Get(context.Background(), ks)
				if res.Err() != nil {
					log.Printf("[ERROR] Redis Get failed: %v", res.Err())
					continue
				}
				// transfer sitePV to sitePV (str)
				k := strings.ReplaceAll(ks, viper.GetString("redis.prefix"), viper.GetString("redis.ToPrefix"))
				log.Printf("[INFO] transfer %v %s\r\n", ks, res.Val())
				if err := RDB.Set(context.Background(), k, res.Val(), 0).Err(); err != nil {
					log.Printf("[ERROR] Redis Set failed: %v", err)
					continue
				}
			}
		}

		if cur == 0 {
			break
		}
	}

	// 转移 pagePV (zset)
	cur = 0
	for {
		key := fmt.Sprintf("%s:page_pv:*", viper.GetString("redis.prefix"))
		result := RDB.Scan(context.Background(), cur, key, 100)
		if result.Err() != nil {
			log.Printf("[ERROR] Redis scan failed: %v", result.Err())
			continue
		}

		keys, cur = result.Val()
		// get pagePV keys
		if len(keys) > 0 {
			for _, ks := range keys {
				// log.Printf("[INFO] Redis scan keys: %v \r\n", keys)

				// check if zset
				keyType := RDB.Type(context.Background(), ks)
				if keyType.Err() != nil {
					log.Printf("[ERROR] Redis Type failed: %v", keyType.Err())
					continue
				}
				if keyType.Val() != "zset" {
					log.Printf("[INFO] Redis key %v is not zset \r\n", ks)
					continue
				}

				// get pagePV (zset)
				res := RDB.ZRangeWithScores(context.Background(), ks, 0, -1)
				if res.Err() != nil {
					log.Printf("[ERROR] Redis ZRangeWithScores failed: %v", res.Err())
					continue
				}
				// transfer pagePV to pagePV (zset)
				k := strings.ReplaceAll(ks, viper.GetString("redis.prefix"), viper.GetString("redis.ToPrefix"))
				log.Printf("[INFO] transfer %v with %v\r\n", ks, res.Val())
				for _, v := range res.Val() {
					if err := RDB.ZAdd(context.Background(), k, redis.Z{Score: v.Score, Member: v.Member.(string)}).Err(); err != nil {
						log.Printf("[ERROR] Redis ZAdd failed: %v", err)
						continue
					}
				}
			}
		}

		if cur == 0 {
			break
		}
	}

	log.Printf("[INFO] 数据已完成迁移, 新的 redis 前缀为: %s , 在验证迁移成功后, 您可以稍后手动删除旧数据\r\n", viper.GetString("redis.ToPrefix"))
	log.Println("[INFO] 请参考 readme 进行后续操作")
}

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[INFO] Config init success %v", viper.AllSettings())
}

func initRedis() {
	log.Printf("[INFO] Redis trying connect to tcp://%s/%d", viper.GetString("redis.address"), viper.GetInt("redis.database"))
	option := &redis.Options{
		Addr:            viper.GetString("redis.address"),
		Password:        viper.GetString("redis.password"),
		DB:              viper.GetInt("redis.database"),
		MinIdleConns:    5,
		MaxIdleConns:    20,
		MaxRetries:      5,
		MaxActiveConns:  20,
		ConnMaxLifetime: 5 * time.Minute,
	}
	rdb := redis.NewClient(option)

	RDB = rdb

	// test redis
	pong, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("[ERROR] Redis ping failed: %v", err)
	}

	log.Printf("[INFO] Redis init success, pong: %s ", pong)

}
