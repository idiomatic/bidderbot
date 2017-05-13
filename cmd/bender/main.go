package main

// BidderBot places buy "bids" and sell "asks" according to some
// simple rules:
//
// * not within the spread between the highest bid and lowest ask
// * such that the ask will net a minimum of profit
// * applying compound "profit" exponential growth curve
//
// This sixth version does away with having multiple concurrent asks,
// under the assumption that it is responsive enough to list
// subsequent positions as needed.
//
// This version also leverages channels and fewer goroutines rather
// than mutexes and more goroutines for concurrency.  This version
// also reduces boilerplatey object-orientation of previous versions.

import (
	"errors"
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	secret     = os.Getenv("COINBASE_SECRET")
	key        = os.Getenv("COINBASE_KEY")
	passphrase = os.Getenv("COINBASE_PASSPHRASE")

	// XXX product specific
	rate, _       = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_RATE"), "0.01"), 64)
	investRate, _ = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_INVEST_RATE"), "1.00"), 64)
	minProfit, _  = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_MIN_PROFIT"), "0.01"), 64)

	symbols = map[string]string{
		"ETH-USD": "Ξ",
		"BTC-USD": "฿",
		"LTC-USD": "Ł",
	}

	exchange         *gdax.Client
	exchangeThrottle = time.Tick(time.Second / 2)
	exchangeTime     gdax.Time
)

func ifBlank(value, fallback string) string {
	if value == "" {
		value = fallback
	}
	return value
}

func Poke(c chan bool) {
	// non-blocking send idiom
	select {
	case c <- true:
	default:
	}
}

func GetTime() (gdax.Time, error) {
	<-exchangeThrottle
	exchangeNow, err := exchange.GetTime()
	if err != nil {
		return gdax.Time{}, err
	}
	now, err := time.Parse(time.RFC3339, exchangeNow.ISO)
	if err != nil {
		return gdax.Time{}, err
	}
	return gdax.Time(now), nil
}

func GetProduct(productId string) (*gdax.Product, error) {
	<-exchangeThrottle
	products, err := exchange.GetProducts()
	if err != nil {
		return nil, err
	}

	for _, p := range products {
		if p.Id == productId {
			return &p, nil
		}
	}
	return nil, errors.New("product not found")
}

type Buy struct {
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Time      gdax.Time `json:"time"`
	objective float64
}

type Product struct {
	Id             string
	FiatSymbol     string // XXX rename Fiat
	Symbol         string
	PriceDivisions float64
	SizeDivisions  float64
	Rate           float64
	MinPrice       float64
	MinSize        float64
	InvestRate     float64 // $/minute
	MinProfit      float64
	MarketBid      float64
	MarketAsk      float64
	BidCost        float64
	OutOfFiat      bool
	OutOfCoin      bool
	BidId          string `redis:"benderlite:bid:{product.Id}"`
	BidPrice       float64
	BidSize        float64
	BidMutex       sync.Mutex
	AskId          string `redis:"benderlite:ask:{product.Id}"`
	AskPrice       float64
	AskSize        float64
	AskMutex       sync.Mutex
	Buys           []Buy `redis:"benderlite:buys:{product.Id}"`
	BuysMutex      sync.Mutex
	PlaceBid       chan bool
	ReplaceBid     chan bool
	ReplaceAsk     chan bool
}

type byObjective []Buy

func (a byObjective) Len() int           { return len(a) }
func (a byObjective) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byObjective) Less(i, j int) bool { return a[i].objective < a[j].objective }

func ByObjective(product *Product) byObjective {
	product.updateObjectives()
	return byObjective(product.Buys)
}

func (product *Product) FloorCost(cost float64) float64 {
	// USD penny resolution
	return math.Floor(cost*100) / 100
}

func (product *Product) CeilCost(cost float64) float64 {
	return math.Ceil(cost*100) / 100
}

func (product *Product) CeilPrice(price float64) float64 {
	return math.Ceil(price*product.PriceDivisions) / product.PriceDivisions
}

func (product *Product) FloorSize(size float64) float64 {
	return math.Floor(size*product.SizeDivisions) / product.SizeDivisions
}

func (product *Product) RoundSize(size float64) float64 {
	return product.FloorSize(size + 1/product.SizeDivisions/2)
}

func (buy *Buy) Marshal() string {
	var t = buy.Time.Time().Format(time.RFC3339)
	return fmt.Sprintf("%.2f|%.8f|%s", buy.Price, buy.Size, t)
}

func UnmarshalBuy(s string) *Buy {
	var (
		price, size float64
		t           string
	)
	_, err := fmt.Sscanf(s, "%f|%f|%s", &price, &size, &t)
	if err != nil {
		log.Panic(err)
	}
	buyTime, _ := time.Parse(time.RFC3339, t)
	return &Buy{
		Price: price,
		Size:  size,
		Time:  gdax.Time(buyTime),
	}
}

func (buy *Buy) Objective(product *Product) float64 {
	var dt = exchangeTime.Time().Sub(buy.Time.Time())
	return product.CeilPrice(
		buy.Price * math.Pow(1+product.Rate, dt.Hours()/24))
}

func NewProduct(productId string) *Product {
	p, err := GetProduct(productId)
	if err != nil {
		log.Panic(err)
	}

	sizeDivisions := 1e4
	// XXX could make it relative to GetTicker and InvestRate
	if productId == "BTC-USD" {
		sizeDivisions = 1e6
	}

	return &Product{
		Id:             productId,
		Symbol:         symbols[productId],
		FiatSymbol:     "$",
		PriceDivisions: math.Trunc(1/p.QuoteIncrement + 0.5),
		SizeDivisions:  sizeDivisions,
		Rate:           rate,
		MinPrice:       p.QuoteIncrement,
		MinSize:        p.BaseMinSize,
		InvestRate:     investRate,
		MinProfit:      minProfit,
		PlaceBid:       make(chan bool, 1),
		ReplaceBid:     make(chan bool, 1),
		ReplaceAsk:     make(chan bool, 1),
	}
}

func IsNotFound(err error) bool {
	return err != nil && (err.Error() == "order not found" ||
		err.Error() == "NotFound")
}

func IsDone(err error) bool {
	return err != nil && err.Error() == "Order already done"
}

func IsBroke(err error) bool {
	return err != nil && err.Error() == "Insufficient funds"
}

func IsExpiredRequest(err error) bool {
	// go-coinbase-exchange should tap GetTime() or WebSocket messages for server time
	return err != nil && err.Error() == "request timestamp expired"
}

func (product *Product) PxS(style, subject string, args ...float64) string {
	var (
		price, size, cost                              float64
		priceStr, timesStr, sizeStr, equalStr, costStr string
	)

	if len(args) >= 1 {
		price = args[0]
	}
	if len(args) >= 2 {
		size = args[1]
	}
	if len(args) >= 3 {
		cost = args[2]
	} else {
		cost = price * size
	}

	if price != 0.0 {
		priceStr = fmt.Sprintf("%.2f[2m%s/%s[0;%sm", price, product.FiatSymbol, product.Symbol, style)
	}
	if size != 0.0 {
		sizeStr = fmt.Sprintf("%.6f[2m%s[0;%sm", size, product.Symbol, style)
		if price != 0.0 {
			timesStr = "⨯"
		}
	}
	if cost != 0.0 {
		costStr = fmt.Sprintf("[2m%s[0;%sm%.2f", product.FiatSymbol, style, cost)
		equalStr = "≈"
	}
	return fmt.Sprintf("[%sm %-24s %13s %1s %11s %1s %11s [m",
		style, subject, priceStr, timesStr, sizeStr, equalStr, costStr)
}

func (product *Product) Load() error {
	state := redisPool.Get()
	defer state.Close()

	id, err := redigo.String(state.Do("GET", "benderlite:bid:"+product.Id))
	if err != nil {
		if err != redigo.ErrNil {
			return err
		}
	}
	product.BidId = id

	id, err = redigo.String(state.Do("GET", "benderlite:ask:"+product.Id))
	if err != nil {
		if err != redigo.ErrNil {
			return err
		}
	}
	product.AskId = id

	buysData, err := redigo.Strings(state.Do("SMEMBERS", "benderlite:buys:"+product.Id))
	if err != nil {
		if err != redigo.ErrNil {
			return err
		}
	}

	product.BuysMutex.Lock()
	for _, buyData := range buysData {
		product.Buys = append(product.Buys, *UnmarshalBuy(buyData))
	}
	product.BuysMutex.Unlock()

	// XXX check filled orders to see if bid or ask completed.

	return nil
}

func (product *Product) updateObjectives() {
	for i := range product.Buys {
		var buy = &product.Buys[i]
		buy.objective = buy.Objective(product)
	}
}

func (product *Product) Bought(price, size float64, boughtAt gdax.Time) {
	// XXX partial bought?

	product.BidMutex.Lock()
	product.BidSize -= size
	product.BidMutex.Unlock()

	var buy = Buy{
		Price: price,
		Size:  size,
		Time:  boughtAt,
	}
	product.BuysMutex.Lock()

	// ASSERT: possible collision for partial purchases of the same size/time/price
	for i := 0; i < len(product.Buys); i++ {
		priorBuy := product.Buys[i]
		if priorBuy.Time != boughtAt {
			break
		}
		if priorBuy.Price == buy.Price &&
			priorBuy.Size == buy.Size {
			log.Println(product.PxS("43;32;1", "buy indistinguishable", priorBuy.Price, priorBuy.Size))
		}
	}

	product.Buys = append(product.Buys, buy)
	product.BuysMutex.Unlock()

	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("SADD", "benderlite:buys:"+product.Id, buy.Marshal())
	if err != nil {
		log.Panic("buy store error: ", err)
	}

	log.Println(product.PxS("42", "buy", price, size,
		product.CeilCost(price*size)))
}

func (product *Product) Sold(price, size float64, soldAt gdax.Time) {
	// eliminate lowest priced buy(s)
	var (
		err             error
		stateKey        = "benderlite:buys:" + product.Id
		stateRemoveArgs = []interface{}{stateKey}
		stateAddArgs    = []interface{}{stateKey}
	)

	product.BuysMutex.Lock()
	sort.Sort(ByObjective(product))
	var remainSize = size
	for remainSize > 0.0 && len(product.Buys) > 0 {
		var buy = &product.Buys[0]
		if buy.Price > price {
			// XXX some way to express price & cost delta
			log.Println(product.PxS("32;1", "buy loss", buy.Price, buy.Size))
		}
		if roundSize := product.RoundSize(buy.Size - remainSize); roundSize > 0 {
			// keep a portion
			stateRemoveArgs = append(stateRemoveArgs, buy.Marshal())
			buy.Size = roundSize
			stateAddArgs = append(stateAddArgs, buy.Marshal())
			log.Println(product.PxS("32;2", "buy split", buy.Price, buy.Size))
			break
		} else {
			// the entirety of this buy is sold
			stateRemoveArgs = append(stateRemoveArgs, buy.Marshal())
			remainSize -= buy.Size
			product.Buys = product.Buys[1:]
			//log.Println(product.PxS("32;2", "buy remove", buy.Price, buy.Size))
		}
	}
	product.BuysMutex.Unlock()

	state := redisPool.Get()
	defer state.Close()

	// XXX alternatively, redigo.Args{}.Add(stateKey).AddFlat(stateRemove)...
	if len(stateRemoveArgs) > 1 {
		_, err = state.Do("SREM", stateRemoveArgs...)
		if err != nil {
			log.Panic("buy remove error: ", err)
		}
	}

	if len(stateAddArgs) > 1 {
		_, err = state.Do("SADD", stateAddArgs...)
		if err != nil {
			log.Panic("buy update error: ", err)
		}
	}

	log.Println(product.PxS("41", "sell", price, size,
		product.FloorCost(price*size)))

	product.AskMutex.Lock()
	product.AskId = ""
	product.AskPrice = 0.0
	product.AskMutex.Unlock()
}

// RESTful liaison
func (product *Product) BidLoop() {
	<-exchangeThrottle
	ticker, err := exchange.GetTicker(product.Id)
	if err != nil {
		log.Panic(err)
	}
	product.MarketBid = ticker.Bid
	product.MarketAsk = ticker.Ask
	log.Println(product.PxS("32", "market bid", product.MarketBid))
	log.Println(product.PxS("31", "market ask", product.MarketAsk))

	// HACK to eventually detect influx of funds in absence of churn
	go func() {
		for range time.Tick(time.Minute * 10) {
			product.OutOfFiat = false // XXX product agnostic
			product.OutOfCoin = false
		}
	}()

	go func() {
		product.BidCost = product.InvestRate
		for range time.Tick(time.Minute) {
			product.BidCost = product.InvestRate
		}
	}()

	for {
		select {

		// XXX need to throttle
		case <-product.ReplaceBid:
			// either periodically invest or the bid market moved up
			var (
				marketBid = product.MarketBid // copy for concurrent access
				price     = marketBid
				size      = product.FloorSize(product.BidCost / marketBid)
			)

			// invest no further
			if product.BidCost == 0 {
				continue
			}

			product.BidMutex.Lock()
			var (
				bidId    = product.BidId
				bidPrice = product.BidPrice
				bidSize  = product.BidSize
			)

			// no change necessary
			if price == bidPrice && size == bidSize {
				product.BidMutex.Unlock()
				continue
			}

			if bidId == "" && product.OutOfFiat {
				product.BidMutex.Unlock()
				continue
			}

			product.BidId = ""
			product.BidMutex.Unlock()

		bid_cancel_retry:
			if bidId != "" {
				<-exchangeThrottle
				err := exchange.CancelOrder(bidId)
				if err != nil {
					if IsExpiredRequest(err) {
						log.Println(product.PxS("32", "bid cancel retry", bidPrice, bidSize))
						goto bid_cancel_retry
					} else if IsNotFound(err) || IsDone(err) {
						log.Println(product.PxS("32", "bid cancel nothing", bidPrice, bidSize))
					} else {
						log.Panic("bid cancel error: ", err)
					}
				} else {
					log.Println(product.PxS("32", "bid cancel", bidPrice, bidSize))
				}
			}

			if price < product.MinPrice || size < product.MinSize {
				log.Println(product.PxS("32;2", "bid trivial", price, size))
				continue
			}

		bid_create_retry:
			<-exchangeThrottle
			confirm, err := exchange.CreateOrder(&gdax.Order{
				Type:      "limit",
				Side:      "buy",
				ProductId: product.Id,
				Price:     price,
				Size:      size,
				PostOnly:  true,
			})
			if err != nil {
				if IsExpiredRequest(err) {
					log.Println(product.PxS("32", "bid retry", price, size))
					goto bid_create_retry
				} else if IsBroke(err) {
					log.Println(product.PxS("32", "bid broke", price, size))
					product.OutOfFiat = true
					continue
				} else {
					log.Panic("bid error: ", err)
				}
			}
			product.BidMutex.Lock()
			product.BidId = confirm.Id
			product.BidPrice = confirm.Price
			product.BidSize = confirm.Size
			product.BidMutex.Unlock()

			{
				state := redisPool.Get()
				_, err = state.Do("SET", "benderlite:bid:"+product.Id, confirm.Id)
				if err != nil {
					log.Panic("bid store error: ", err)
				}
				state.Close()
			}

			log.Println(product.PxS("32", "bid", price, size))
		}
	}
}

// RESTful liaison
func (product *Product) AskLoop() {
	for {
		select {

		case <-product.ReplaceAsk:
			product.BuysMutex.Lock()
			if len(product.Buys) == 0 {
				product.BuysMutex.Unlock()
				continue
			}

			product.updateObjectives()

			/*
				for _, buy := range product.Buys {
					log.Println(product.PxS("2;33", "pick buy", buy.Price, buy.Size))
				}
			*/

			// Picks the lowest price point for the next Ask that
			// includes buys that are outperforming rate.
			var (
				marketAsk = product.MarketAsk // copy for concurrent access
				price     = math.Max(product.Buys[0].objective, marketAsk)
			)
		pick_candidates:
			var (
				size      = 0.0
				basisCost = 0.0
			)

			for _, buy := range product.Buys {
				//log.Println(product.PxS("31;2", "ask pick", buy.objective, buy.Size))
				var objective = math.Max(buy.objective, marketAsk)
				if objective > price {
					// buy disqualified on a previous pass
					continue
				}
				if price > objective {
					// start over and (possibly) pick fewer candidates
					price = objective
					goto pick_candidates
				}
				basisCost += product.CeilCost(buy.Price * buy.Size)
				size += buy.Size
			}
			product.BuysMutex.Unlock()

			size = product.RoundSize(size)
			price = product.CeilPrice(math.Max(price, (basisCost+product.MinProfit)/size))

			// XXX could keep adding until reached a local minimum accounting for minProfit and penny shavings
			// XXX raise price to conserve fractional pennies at small sizes?

			// nothing to invest
			if basisCost == 0.0 {
				continue
			}

			product.AskMutex.Lock()
			var (
				askId    = product.AskId
				askPrice = product.AskPrice
				askSize  = product.AskSize
			)

			// no change
			if price == askPrice && size == askSize {
				product.AskMutex.Unlock()
				continue
			}

			if askId == "" && product.OutOfCoin {
				product.AskMutex.Unlock()
				continue
			}

			//log.Println(product.PxS("31;2", "ask picked", price, size))

			product.AskId = ""
			// XXX product.AskPrice = 0.0
			product.AskMutex.Unlock()

		ask_cancel_retry:
			if askId != "" {
				<-exchangeThrottle
				err := exchange.CancelOrder(askId)
				if err != nil {
					if IsExpiredRequest(err) {
						log.Println(product.PxS("31", "ask cancel retry", askPrice, askSize))
						goto ask_cancel_retry
					} else if IsNotFound(err) || IsDone(err) {
						log.Println(product.PxS("31", "ask cancel nothing", askPrice, askSize))
					} else {
						log.Panic("ask cancel error: ", err)
					}
				} else {
					log.Println(product.PxS("31", "ask cancel", askPrice, askSize))
				}
			}

			if price < product.MinPrice || size < product.MinSize {
				continue
			}

		ask_create_retry:
			<-exchangeThrottle
			confirm, err := exchange.CreateOrder(&gdax.Order{
				Type:      "limit",
				Side:      "sell",
				ProductId: product.Id,
				Price:     price,
				Size:      size,
				PostOnly:  true,
			})
			if err != nil {
				if IsExpiredRequest(err) {
					log.Println(product.PxS("31", "ask retry", price, size))
					goto ask_create_retry
				} else if IsBroke(err) {
					log.Println(product.PxS("31", "ask broke", price, size))
					product.OutOfCoin = true
					continue
				} else {
					log.Panic("ask error: ", err)
				}
			}

			product.AskMutex.Lock()
			product.AskId = confirm.Id
			product.AskPrice = confirm.Price
			product.AskSize = confirm.Size
			product.AskMutex.Unlock()

			log.Println(product.PxS("31", "ask", confirm.Price, confirm.Size))

			{
				state := redisPool.Get()
				_, err := state.Do("SET", "benderlite:ask:"+product.Id, confirm.Id)
				if err != nil {
					log.Panic("ask store error: ", err)
				}
				state.Close()
			}

		case <-time.After(time.Second * 10):
			var err error

			<-exchangeThrottle
			exchangeTime, err = GetTime()
			if err != nil {
				continue
			}
			//log.Printf("[2m time  %s[m\n", exchangeTime.Time().Format(time.RFC3339))
		}
	}
}

// WebSocket liaison
func FeedLoop(products map[string]*Product) {
	<-exchangeThrottle

	// HACK until Dial/Subscribe split
	var productId string
	for _, product := range products {
		productId = product.Id
		break
	}

	// XXX split into Dial() and Subscribe()
	messages, connected, err := DialAndSubscribe(secret, key, passphrase, productId)
	if err != nil {
		log.Panic("websocket error: ", err)
	}

	for {
		select {

		case <-time.After(time.Second * 10):
			log.Println("[43m websocket idle [m")

		case message := <-messages:
			product, ok := products[message.ProductId]
			if !ok {
				continue
			}

			// XXX not all messages have Time
			exchangeTime = message.Time

			var (
				price = message.Price
				size  = message.Size
			)

			switch message.Type {
			case "received":
				if message.OrderType != "limit" {
					continue
				}

				switch message.Side {
				case "buy":
					// ignore weirdly-high or irrelevantly-below market bids
					if product.MarketAsk > price && price > product.MarketBid {
						product.MarketBid = price
						//log.Println(product.PxS("2", "market bid", price))
						Poke(product.ReplaceBid)
					}

				case "sell":
					if product.MarketAsk > price && price > product.MarketBid {
						product.MarketAsk = price
						//log.Println(product.PxS("2", "market ask", price))
					}
				}

			case "match":
				switch message.Side {
				case "buy":
					product.MarketBid = price
					//log.Println(product.PxS("2", "market bid via match", price))

					if message.MakerOrderId != product.BidId {
						// make a less frugal bid to follow market
						Poke(product.ReplaceBid)
						continue
					}

					product.Bought(price, size, message.Time)

					// no longer broke: sell more
					product.OutOfCoin = false
					Poke(product.ReplaceAsk)

				case "sell":
					product.MarketAsk = price
					//log.Println(product.PxS("2", "market ask via match", price))

					if message.MakerOrderId != product.AskId {
						continue
					}

					product.Sold(price, size, message.Time)

					// no longer broke: buy more
					product.OutOfFiat = false
					Poke(product.ReplaceAsk)
				}
			}

		case up := <-connected:
			if up {
				log.Println("[43m connected [m")
			} else {
				log.Println("[43m disconnected [m")
			}

		}
	}
}

func main() {
	var err error

	exchange = gdax.NewClient(secret, key, passphrase)

	<-exchangeThrottle
	exchangeTime, err = GetTime()
	if err != nil {
		log.Panic(err)
	}
	log.Printf("[2m time  %s[m\n", exchangeTime.Time().Format(time.RFC3339))

	var products = make(map[string]*Product)

	products["ETH-USD"] = NewProduct("ETH-USD")
	//products["BTC-USD"] = NewProduct("BTC-USD")
	//products["LTC-USD"] = NewProduct("LTC-USD")

	for _, product := range products {
		// XXX foreach product
		Poke(product.ReplaceBid)
		Poke(product.ReplaceAsk)

		// recover state from Redis
		// XXX foreach product
		err = product.Load()
		if err != nil {
			log.Panic(err)
		}

		// unnecesary unless external transfers happen
		// XXX foreach product
		/*
			go func() {
				for range time.Tick(time.Second * 6) {
					// gotta spend money to make money
					Poke(product.ReplaceBid)
					Poke(product.ReplaceAsk)
				}
			}()
		*/

		go product.BidLoop()
		go product.AskLoop()
	}

	FeedLoop(products)
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
}
