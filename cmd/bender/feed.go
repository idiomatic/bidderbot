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

type Message struct {
	gdax.Message
	UserId         string `json:"user_id"`
	TakerUserId    string `json:"taker_user_id"`
	ProfileId      string `json:"profile_id"`
	TakerProfileId string `json:"taker_profile_id"`
}

func Subscribe(secret, key, passphrase, productId string) (chan Message, error) {
	ch := make(chan Message)

	go func() {
		for ; ; time.Sleep(time.Second) {
			log.Println(YellowBackground(Bold("connecting")))

			var wsDialer ws.Dialer
			wsConn, _, err := wsDialer.Dial("wss://ws-feed.gdax.com", nil)
			if err != nil {
				log.Println(err)
				continue
			}

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

			for {
				message := Message{}
				if err := wsConn.ReadJSON(&message); err != nil {
					log.Print(err)
					break
				}
				ch <- message
			}
		}
	}()

	return ch, nil
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
