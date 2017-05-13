package main

// BidderBot places buy "bids" and sell "asks" according to some
// simple rules:
//
// * not within the spread between the highest bid and lowest ask
// * such that the ask will net a minimum of profit
// * applying compound "profit" exponential growth curve
//
// This version refactors buy and sell and reduces code size by 25%.

import (
	"errors"
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	productId = "ETH-USD"
	rate      = 0.01
)

var (
	secret     = os.Getenv("COINBASE_SECRET")
	key        = os.Getenv("COINBASE_KEY")
	passphrase = os.Getenv("COINBASE_PASSPHRASE")

	exchange         *gdax.Client
	exchangeThrottle = time.Tick(time.Second / 2)
	exchangeTime     gdax.Time

	buyPerMinute = 1.00
	minProfit    = 0.01
)

type Market struct {
	Bid, Ask               float64
	BidChanged, AskChanged chan float64
}

func NewMarket(productId string) *Market {
	<-exchangeThrottle
	ticker, err := exchange.GetTicker(productId)
	if err != nil {
		log.Panic(err)
	}

	return &Market{
		Bid:        ticker.Bid,
		Ask:        ticker.Ask,
		BidChanged: make(chan float64, 1),
		AskChanged: make(chan float64, 1),
	}
}

func sendFloat(value float64, ch chan float64) {
	// non-blocking
	select {
	case ch <- value:
	default:
	}
}

func (market *Market) SetBid(bid float64) {
	if market.Bid != bid {
		market.Bid = bid
		sendFloat(bid, market.BidChanged)
	}
}

func (market *Market) SetAsk(ask float64) {
	if market.Ask != ask {
		market.Ask = ask
		sendFloat(ask, market.AskChanged)
	}
}

type Order struct {
	Price, Size float64
}

type Fill struct {
	Price, Size float64
	Time        gdax.Time
	objective   float64
}

func Objective(price float64, dt time.Duration, rate, resolution float64) float64 {
	return math.Ceil(price*math.Pow(1+rate, dt.Hours()/24)/resolution) * resolution
}

func (fill *Fill) Objective(product *Product) float64 {
	var dt = exchangeTime.Time().Sub(fill.Time.Time())
	return Objective(fill.Price, dt, product.Rate, product.QuoteIncrement)
}

func DecodeFill(s string) (*Fill, error) {
	var (
		price, size float64
		t           string
	)
	_, err := fmt.Sscanf(s, "%f|%f|%s", &price, &size, &t)
	if err != nil {
		return nil, err
	}
	buyTime, err := time.Parse(time.RFC3339, t)
	if err != nil {
		return nil, err
	}
	return &Fill{price, size, gdax.Time(buyTime), 0}, nil
}

func (fill *Fill) Encode() string {
	var t = fill.Time.Time().Format(time.RFC3339)
	return fmt.Sprintf("%.2f|%.8f|%s", fill.Price, fill.Size, t)
}

type Buys struct {
	Product *Product
	Fills   []Fill
	Mutex   sync.Mutex
}

func (buys *Buys) StateKey() string {
	return "bender7:buys:" + buys.Product.Id
}

// Restore buys from Redis
func (buys *Buys) Restore() error {
	state := redisPool.Get()
	defer state.Close()

	buysData, err := redigo.Strings(state.Do("SMEMBERS", buys.StateKey()))
	if err != nil {
		if err == redigo.ErrNil {
			return nil
		}
		return err
	}

	for _, data := range buysData {
		fill, err := DecodeFill(data)
		if err != nil {
			return err
		}
		buys.Fills = append(buys.Fills, *fill)
	}

	return nil
}

func (buys *Buys) NextAsk(atLeast float64) Order {
	buys.Mutex.Lock()
	defer buys.Mutex.Unlock()

	sort.Sort(ByObjective(buys))

	var (
		price     = atLeast
		size      = 0.0
		basisCost = minProfit
		qi        = buys.Product.QuoteIncrement
		iqi       = Round(1 / qi)
		isi       = Round(1 / buys.Product.SizeIncrement)
	)

	// sum buys below or equal atLeast (market bid), otherwise below
	// or equal to lowest price above atLeast.
	for _, buy := range buys.Fills {
		if buy.objective > price {
			if size > 0.0 {
				break
			}
			price = buy.objective
		}
		basisCost += Round(buy.Price*buy.Size*iqi) / iqi
		size += buy.Size
	}

	if size == 0.0 {
		return Order{0.0, 0.0}
	}

	price = math.Max(price, basisCost/size)
	size = Round(size*isi) / isi
	price = math.Ceil(price*iqi) / iqi

	return Order{price, size}
}

func (buys *Buys) Bought(fill Fill) {
	buys.Mutex.Lock()

	// aggregate to an existing equivalent price/time buy
	for i := range buys.Fills {
		var f = &buys.Fills[i]
		if fill.Price == f.Price && fill.Time == f.Time {
			f.Size += fill.Size
			// break and skip "else"
			goto added
		}
	}
	// else
	buys.Fills = append(buys.Fills, fill)

added:
	buys.Mutex.Unlock()

	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("SADD", buys.StateKey(), fill.Encode())
	if err != nil {
		log.Panic("buy store error: ", err)
	}

	buys.Product.Log("42", "bought", fill.Price, fill.Size)
}

func (buys *Buys) Sold(fill Fill) float64 {
	buys.Mutex.Lock()

	var (
		Log          = buys.Product.Log
		remainder    = fill.Size
		removeBefore = len(buys.Fills)
		partial      *Fill
		srem         = []interface{}{buys.StateKey()}
	)

	sort.Sort(ByObjective(buys))

	for i, buy := range buys.Fills {
		if remainder <= 0.0 {
			break
		}

		srem = append(srem, buy.Encode())

		if buy.Size < remainder {
			buy.Size -= remainder
			removeBefore = i
			partial = &buy
			Log("41", "partially sold", buy.Price, remainder)
			break
		}

		Log("41", "sold", buy.Price, buy.Size)
		remainder -= buy.Size
	}

	buys.Fills = buys.Fills[removeBefore:]

	Log("41", "total", fill.Price, fill.Size)
	buys.Mutex.Unlock()

	state := redisPool.Get()
	defer state.Close()

	if len(srem) > 1 {
		_, err := state.Do("SREM", srem...)
		if err != nil {
			log.Panic("buy remove error: ", err)
		}
	}

	if partial != nil {
		_, err := state.Do("SADD", buys.StateKey())
		if err != nil {
			log.Panic("buy update error: ", err)
		}
	}

	return remainder
}

type byObjective Buys

func (buys byObjective) Len() int           { return len(buys.Fills) }
func (buys byObjective) Swap(i, j int)      { buys.Fills[i], buys.Fills[j] = buys.Fills[j], buys.Fills[i] }
func (buys byObjective) Less(i, j int) bool { return buys.Fills[i].objective < buys.Fills[j].objective }

func ByObjective(buys *Buys) byObjective {
	for i := range buys.Fills {
		buys.Fills[i].objective = buys.Fills[i].Objective(buys.Product)
	}
	return byObjective(*buys)
}

type Product struct {
	gdax.Product
	Rate               float64
	Symbol, FiatSymbol string
	SizeIncrement      float64
}

var logPadding = strings.Repeat(" ", 80)

func (product *Product) Log(style, subject string, price, size float64, costArg ...float64) {
	var cost = price * size
	if len(costArg) > 0 {
		cost = costArg[0]
	}

	ifZero := func(v float64, s1, s2 string) string {
		if v == 0.0 {
			return s1
		}
		return s2
	}

	// overcomplicated because len(string) measures vt100 escape sequences
	log.Printf("[%sm %-24s %s %1s %s %1s %s [0m\n",
		style,
		subject,
		ifZero(price, logPadding[:10],
			fmt.Sprintf("%s[2m%s[0;%sm%.2f[2m/%s[0;%sm",
				logPadding[len(fmt.Sprintf("%.2f", price)):10],
				product.FiatSymbol, style, price, product.Symbol, style)),
		ifZero(price*size, "", "⨯"),
		ifZero(size, logPadding[:10],
			fmt.Sprintf("%10.6f[2m%s[0;%sm", size, product.Symbol, style)),
		ifZero(price*size, "", "≈"),
		ifZero(cost, logPadding[:10],
			fmt.Sprintf("%s[2m%s[0;%sm%.2f",
				logPadding[len(fmt.Sprintf("%.2f", cost)):10],
				product.FiatSymbol, style, cost)))
}

type OrderWrangler struct {
	Product     *Product
	Side        string
	Price, Size float64 // XXX embed an Order (not *Order)?
	Id          string
	Style       string
	Substyle    string
	Replace     chan Order
	Filled      chan Fill
	Mutex       sync.Mutex
}

func (product *Product) OrderWrangler(side string) *OrderWrangler {
	var style string
	switch side {
	case "buy":
		style = "32"
	case "sell":
		style = "31"
	}

	wrangler := &OrderWrangler{
		Product:  product,
		Side:     side,
		Style:    style,
		Substyle: style + ";2",
		Replace:  make(chan Order, 3),
		Filled:   make(chan Fill, 3),
	}

	go func() {
		for order := range wrangler.Replace {
			var (
				qi    = product.QuoteIncrement
				iqi   = Round(1 / qi)
				isi   = Round(1 / product.SizeIncrement)
				bms   = product.BaseMinSize
				price = Round(order.Price*iqi) / iqi
				size  = Round(order.Size*isi) / isi
			)

			wrangler.Mutex.Lock()
			unchanged := wrangler.Id != "" &&
				price == wrangler.Price &&
				size == wrangler.Size
			wrangler.Mutex.Unlock()

			if unchanged {
				continue
			}

			err := wrangler.Cancel()
			if err != nil {
				log.Panic(err)
			}

			// too small to matter
			if price < qi || size < bms {
				//wrangler.Product.Log(wrangler.Substyle, wrangler.Side + " too small", price, size)
				continue
			}

		create_order:
			<-exchangeThrottle
			confirm, err := exchange.CreateOrder(&gdax.Order{
				Type:      "limit",
				Side:      side,
				ProductId: product.Id,
				Price:     price,
				Size:      size,
				PostOnly:  true,
			})
			if IsExpiredRequest(err) {
				// retry
				wrangler.Product.Log(wrangler.Substyle, wrangler.Side+" create retry", 0, 0)
				goto create_order
			} else if IsBroke(err) {
				wrangler.Product.Log(wrangler.Substyle, wrangler.Side+" broke", price, size)
				continue
			} else if err != nil && !IsNotFound(err) && !IsDone(err) {
				wrangler.Product.Log(wrangler.Substyle, wrangler.Side+" broke", price, size)
				log.Panic(err)
			}

			wrangler.Mutex.Lock()
			wrangler.Id = confirm.Id
			wrangler.Price = confirm.Price
			wrangler.Size = confirm.Size
			wrangler.Mutex.Unlock()

			wrangler.Product.Log(wrangler.Style, wrangler.Side, confirm.Price, confirm.Size)
		}
	}()

	return wrangler
}

func (wrangler *OrderWrangler) Cancel() error {
	wrangler.Mutex.Lock()
	var (
		id    = wrangler.Id
		price = wrangler.Price
		size  = wrangler.Size
	)
	wrangler.Id = ""
	wrangler.Mutex.Unlock()

	if id != "" {

	cancel_order:
		<-exchangeThrottle
		err := exchange.CancelOrder(id)
		if IsExpiredRequest(err) {
			// retry
			wrangler.Product.Log(wrangler.Substyle, wrangler.Side+" cancel retry", 0, 0)
			goto cancel_order
		} else if err != nil && !IsNotFound(err) && !IsDone(err) {
			return err
		}

		wrangler.Product.Log(wrangler.Style, wrangler.Side+" cancel", price, size)
	}

	return nil
}

func (wrangler *OrderWrangler) Fill(message gdax.Message) {
	wrangler.Mutex.Lock()
	defer wrangler.Mutex.Unlock()

	Log := wrangler.Product.Log

	if message.MakerOrderId == wrangler.Id {
		// may be partial
		wrangler.Size -= message.Size
		if wrangler.Size <= 0.0 {
			wrangler.Size = 0.0
			wrangler.Id = ""
			Log(wrangler.Style, wrangler.Side+" fill", message.Price, message.Size)
		} else {
			Log(wrangler.Style, wrangler.Side+" partial fill", message.Price, message.Size)
			Log(wrangler.Substyle, wrangler.Side+" remainder", wrangler.Price, wrangler.Size)
		}
		// XXX non-zero objective?
		wrangler.Filled <- Fill{message.Price, message.Size, message.Time, 0}
	}
}

func main() {
	exchange = gdax.NewClient(secret, key, passphrase)

	<-exchangeThrottle
	feed := NewFeed(secret, key, passphrase)

	var (
		p, _    = GetProduct(productId)
		product = &Product{
			Product:       *p,
			Symbol:        "Ξ",
			FiatSymbol:    "$",
			SizeIncrement: 1 / 1e4,
		}
		buys   = Buys{Product: product}
		market = NewMarket(productId)
		bidder = product.OrderWrangler("buy")
		asker  = product.OrderWrangler("sell")
	)

	product.Log("32;2", "market bid", market.Bid, 0)
	product.Log("31;2", "market ask", market.Ask, 0)

	err := buys.Restore()
	if err != nil {
		log.Panic(err)
	}

	messages := make(chan gdax.Message, 10)
	err = feed.Subscribe(productId, messages)
	if err != nil {
		log.Panic(err)
	}

	bidder.Replace <- Order{market.Bid, buyPerMinute / market.Bid}
	asker.Replace <- buys.NextAsk(market.Ask)

	// periodically invest more
	go func() {
		for range time.Tick(time.Minute) {
			bidder.Mutex.Lock()
			var (
				marketBid = market.Bid
				basis     = bidder.Price * bidder.Size
			)
			bidder.Mutex.Unlock()

			bidder.Replace <- Order{marketBid, (basis + buyPerMinute) / marketBid}
		}
	}()

	// adjust bid when the market goes up
	go func() {
		for bid := range market.BidChanged {
			bidder.Mutex.Lock()
			var basis = bidder.Price * bidder.Size
			bidder.Mutex.Unlock()

			product.Log("32;2", "market bid", bid, 0)
			bidder.Replace <- Order{bid, basis / bid}
		}
	}()

	// reprice if gap is shrinking and it's still profitable
	go func() {
		for ask := range market.AskChanged {
			product.Log("31;2", "market ask", ask, 0)
			asker.Replace <- buys.NextAsk(market.Ask)
		}
	}()

	// record bid-fill and maybe lower the ask order
	go func() {
		for buy := range bidder.Filled {
			buys.Bought(buy)

			/*
				objective := buy.Objective(product)
				if objective < asker.Price || asker.Id == "" {
					// sell this buy prior to incumbent ask,
					// or no ask order is (likely) placed
					asker.Replace <- Order{objective, buy.Size}
				} else if objective == asker.Price {
					// add this buy to existing placed ask
					product.Log("2", "more reorder", objective, asker.Size+buy.Size)
					asker.Replace <- Order{objective, asker.Size + buy.Size}
				}
			*/
			asker.Replace <- buys.NextAsk(market.Ask)
		}
	}()

	// remove completed investments and place next higher priced one
	go func() {
		for sell := range asker.Filled {
			remainder := buys.Sold(sell)
			if remainder > 0.0 {
				log.Println("warning: unattributed ask fill", remainder)
			} else {
				asker.Replace <- buys.NextAsk(market.Ask)
			}
		}
	}()

	// catch ^C
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	go func() {
		<-interrupt
		signal.Stop(interrupt)
		println()
		log.Println("[43m cancelling [m")
		buyPerMinute = 0.0
		bidder.Cancel()
		asker.Cancel()
		close(messages)
		os.Exit(1)
	}()

	go func() {
		for up := range feed.Connection {
			if up {
				log.Println("[43m connected [m")
			} else {
				log.Println("[43m disconnected [m")
			}
		}
	}()

	// dispatch messages from the websocket
	for message := range messages {
		var price = message.Price

		switch message.Type {

		case "received":
			if message.OrderType == "limit" {
				switch message.Side {
				case "buy":
					// only if shrinking the gap
					// XXX should differentiate between limit and match to ignore limit probes
					if market.Ask > price && price > market.Bid {
						market.SetBid(price)
					}
				case "sell":
					if market.Ask > price && price > market.Bid {
						market.SetAsk(price)
					}
				}
			}

		case "match":
			switch message.Side {
			case "buy":
				bidder.Fill(message)
				market.SetBid(price)
			case "sell":
				asker.Fill(message)
				market.SetAsk(price)
			}

		}
	}
}

// gdax.go

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

func Round(v float64) float64 {
	return math.Trunc(v + 0.5)
}
