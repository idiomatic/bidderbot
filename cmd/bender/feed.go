package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"github.com/gorilla/websocket"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"strconv"
	"sync"
	"time"
)

var (
	ExchangeTime gdax.Time
	FeedBackoff  = time.Second
)

type Feed struct {
	Connection              chan bool
	conn                    *websocket.Conn
	mutex                   sync.Mutex
	subscriptions           map[string]chan gdax.Message
	secret, key, passphrase string
}

func NewFeed(secret, key, passphrase string) *Feed {
	feed := &Feed{
		Connection:    make(chan bool, 1),
		subscriptions: make(map[string]chan gdax.Message),
		secret:        secret,
		key:           key,
		passphrase:    passphrase,
	}

	go func() {
		for ; ; time.Sleep(FeedBackoff) {
			var dialer websocket.Dialer
			conn, _, err := dialer.Dial("wss://ws-feed.gdax.com", nil)
			if err != nil {
				log.Println(err)
				continue
			}
			feed.conn = conn

			feed.mutex.Lock()
			for productId := range feed.subscriptions {
				_ = feed.SendSubscribe(productId)
			}
			feed.mutex.Unlock()

			feed.SetConnected(true)

			for {
				message := gdax.Message{}
				if err := feed.conn.ReadJSON(&message); err != nil {
					log.Print(err)
					break
				}

				if message.Time.Time().After(ExchangeTime.Time()) {
					ExchangeTime = message.Time
				}

				feed.mutex.Lock()
				ch, ok := feed.subscriptions[message.ProductID]
				feed.mutex.Unlock()

				if ok {
					ch <- message
					// XXX if channel closed, unsubscribe
				}
			}

			feed.SetConnected(false)

		}
	}()

	return feed
}

func (feed *Feed) SetConnected(mode bool) {
	// flush the channel
	select {
	case <-feed.Connection:
	default:
	}

	// non-blockingly send
	select {
	case feed.Connection <- mode:
	default:
	}
}

func (feed *Feed) Close() {
	// XXX close()?
	feed.conn.Close()
}

func (feed *Feed) Subscribe(productId string, ch chan gdax.Message) error {
	feed.mutex.Lock()
	defer feed.mutex.Unlock()

	feed.subscriptions[productId] = ch

	if feed.conn != nil {
		if err := feed.SendSubscribe(productId); err != nil {
			return err
		}
	}

	return nil
}

func (feed *Feed) SendSubscribe(productId string) error {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	signature, err := GenerateSignature(timestamp+"GET/users/self", feed.secret)
	if err != nil {
		return err
	}

	subscribe := map[string]string{
		"type":       "subscribe",
		"product_id": productId,
		"key":        feed.key,
		"passphrase": feed.passphrase,
		"timestamp":  timestamp,
		"signature":  signature,
	}
	if err := feed.conn.WriteJSON(subscribe); err != nil {
		return err
	}

	return nil
}

func GenerateSignature(message, secret string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}

	signature := hmac.New(sha256.New, key)
	_, err = signature.Write([]byte(message))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(signature.Sum(nil)), nil
}
