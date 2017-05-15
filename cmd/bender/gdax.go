package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

// XXX for debugging
func CancelOrder(c *gdax.Client, id string) error {
	var err error
	var data []byte
	var body = bytes.NewReader(make([]byte, 0))

	var method = "DELETE"
	var url = fmt.Sprintf("/orders/%s", id)

	var fullURL = fmt.Sprintf("%s%s", c.BaseURL, url)
	req, err := http.NewRequest(method, fullURL, body)
	if err != nil {
		return err
	}

	var timestamp = strconv.FormatInt(time.Now().Unix(), 10)

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "Baylatent Bot 2.0")
	req.Header.Add("CB-ACCESS-KEY", c.Key)
	req.Header.Add("CB-ACCESS-PASSPHRASE", c.Passphrase)
	req.Header.Add("CB-ACCESS-TIMESTAMP", timestamp)

	var message = fmt.Sprintf("%s%s%s%s", timestamp, method, url,
		string(data))

	sig, err := GenerateSignature(message, c.Secret)
	if err != nil {
		return err
	}
	req.Header.Add("CB-ACCESS-SIGN", sig)

	var client = http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		var coinbaseError = gdax.Error{}

		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}

		err = json.Unmarshal(body, &coinbaseError)
		if err != nil {
			return errors.New(fmt.Sprintf("%s: %s", err.Error(), body))
		}

		return error(coinbaseError)
	}

	return nil
}
