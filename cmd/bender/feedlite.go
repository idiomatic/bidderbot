package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	ws "github.com/gorilla/websocket"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"strconv"
	"time"
)

func DialAndSubscribe(secret, key, passphrase, productId string) (chan gdax.Message, chan bool, error) {
	ch := make(chan gdax.Message, 10)
	cx := make(chan bool, 1)

	go func() {
		for ; ; time.Sleep(time.Second) {
			var wsDialer ws.Dialer
			wsConn, _, err := wsDialer.Dial("wss://ws-feed.gdax.com", nil)
			if err != nil {
				log.Println(err)
				continue
			}

			// XXX exchange.GetTime()
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)

			signature, err := GenerateSig(timestamp+"GET/users/self", secret)
			if err != nil {
				log.Println(err)
				continue
			}

			subscribe := map[string]string{
				"type":       "subscribe",
				"product_id": productId,
				"key":        key,
				"passphrase": passphrase,
				"timestamp":  timestamp,
				"signature":  signature,
			}
			if err := wsConn.WriteJSON(subscribe); err != nil {
				log.Println(err)
				continue
			}

			// flush the channel
			select {
			case <-cx:
			default:
			}

			select {
			case cx <- true:
			default:
			}

			for {
				message := gdax.Message{}
				if err := wsConn.ReadJSON(&message); err != nil {
					log.Print(err)
					break
				}
				ch <- message
			}

			// flush the channel
			select {
			case <-cx:
			default:
			}

			select {
			case cx <- false:
			default:
			}
		}
	}()

	return ch, cx, nil
}

func GenerateSig(message, secret string) (string, error) {
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
