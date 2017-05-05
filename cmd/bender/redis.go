package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"net/url"
)

var redisURL = "redis://localhost:6379"

var redisPool *redis.Pool

func init() {
	u, err := url.Parse(redisURL)
	if err != nil {
		log.Fatal(err)
	}
	auth := ""
	if u.User != nil {
		auth, _ = u.User.Password()
	}
	redisPool = &redis.Pool{Dial: func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", u.Host)
		if err != nil {
			return nil, err
		}
		if auth != "" {
			_, err = c.Do("AUTH", auth)
			if err != nil {
				c.Close()
				return nil, err
			}
		}
		return c, err
	}}
}
