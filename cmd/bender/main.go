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
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	secret     = os.Getenv("COINBASE_SECRET")
	key        = os.Getenv("COINBASE_KEY")
	passphrase = os.Getenv("COINBASE_PASSPHRASE")

	exchange         *gdax.Client
	exchangeThrottle = time.Tick(time.Second / 2)
	exchangeTime     gdax.Time

	productId       = ifBlank(os.Getenv("BENDER_PRODUCT_ID"), "ETH-USD")
	rate, _         = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_RATE"), "0.01"), 64)
	buyPerMinute, _ = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_BUY_PER_MINUTE"), "1.00"), 64)
	minProfit, _    = strconv.ParseFloat(ifBlank(os.Getenv("BENDER_MIN_PROFIT"), "0.01"), 64)
)

func ifBlank(value, fallback string) string {
	if value == "" {
		value = fallback
	}
	return value
}

func ifZero(v float64, zero, otherwise string) string {
	if v == 0.0 {
		return zero
	}
	return otherwise
}

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
	// non-blocking; consumer is often in io wait
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

// Objective computes the desired ask price considering safer
// investment lost oppportunity.
func Objective(price float64, dt time.Duration, rate float64) float64 {
	return price * math.Pow(1+rate, dt.Hours()/24)
}

func (fill *Fill) Objective(product *Product) float64 {
	var dt = exchangeTime.Time().Sub(fill.Time.Time())
	return Objective(fill.Price, dt, product.Rate)
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

// NextAsk picks buys ripe to sell.  NextAsk sum buys below or equal
// atLeast (i.e., market bid), otherwise below or equal to lowest
// price above atLeast.
func (buys *Buys) NextAsk(atLeast float64) Order {
	buys.Mutex.Lock()
	defer buys.Mutex.Unlock()

	sort.Sort(ByObjective(buys))

	var (
		price = atLeast
		size  = 0.0
		basis = minProfit
		qi    = buys.Product.QuoteIncrement
		iqi   = Round(1 / qi)
		isi   = Round(1 / buys.Product.SizeIncrement)
	)

	for _, buy := range buys.Fills {
		if buy.objective > price {
			// already have a suitable price above ask; do not go higher
			if size > 0.0 {
				break
			}
			// raise the price until we match or exceed ask
			price = math.Ceil(buy.objective*iqi) / iqi

		}
		basis += Round(buy.Price*buy.Size*iqi) / iqi
		size += buy.Size
	}

	if size == 0.0 {
		return Order{0.0, 0.0}
	}

	price = math.Max(price, basis/size)
	size = Round(size*isi) / isi
	price = math.Ceil(price*iqi) / iqi

	return Order{price, size}
}

// Bought records a (continuing partial) purchase.
func (buys *Buys) Bought(fill Fill) {
	buys.Mutex.Lock()
	buys.Fills = append(buys.Fills, fill)
	buys.Mutex.Unlock()

	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("SADD", buys.StateKey(), fill.Encode())
	if err != nil {
		log.Panic("buy store error: ", err)
	}

	buys.Product.Log("42", "bought", fill.Price, fill.Size)
}

// Sold removes outstanding buy(s), possibly fractionally.
func (buys *Buys) Sold(fill Fill) float64 {
	buys.Mutex.Lock()

	var (
		Log       = buys.Product.Log
		remainder = fill.Size
		partial   *Fill
		srem      = []interface{}{buys.StateKey()}
		basis     = 0.0
	)

	sort.Sort(ByObjective(buys))

	for _, buy := range buys.Fills {
		if remainder <= 0.0 {
			break
		}

		srem = append(srem, buy.Encode())

		if buy.Size > remainder {
			Log("41", "partially sold", buy.Price, remainder)
			basis += buy.Price * remainder
			buy.Size -= remainder
			remainder = 0.0
			partial = &buy
			break
		}

		Log("41", "sold", buy.Price, buy.Size)
		basis += buy.Price * buy.Size
		remainder -= buy.Size

		// HACK
		var isi = Round(1 / buys.Product.SizeIncrement)
		remainder = Round(remainder*isi) / isi

		buys.Fills = buys.Fills[1:]
	}

	if len(srem) > 2 {
		Log("41", "total", fill.Price, fill.Size)
	}
	var profit = fill.Price*fill.Size - basis
	Log("41", "profit", profit/fill.Size, fill.Size, profit)

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
		_, err := state.Do("SADD", buys.StateKey(), partial.Encode())
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

var logPadding = strings.Repeat(" ", 13)

func (product *Product) Log(style, subject string, price, size float64, costArg ...float64) {
	var cost = price * size
	if len(costArg) > 0 {
		cost = costArg[0]
	}

	// overcomplicated because len(string) measures vt100 escape sequences
	log.Printf("[%sm %-24s %s %1s %s %1s %s [0m\n",
		style,
		subject,
		ifZero(price, logPadding[:13],
			fmt.Sprintf("%s[2m%s[0;%sm%.2f[2m/%s[0;%sm",
				logPadding[len(fmt.Sprintf("%.2f", price)):10],
				product.FiatSymbol, style, price, product.Symbol, style)),
		ifZero(price*size, "", "⨯"),
		ifZero(size, logPadding[:11],
			fmt.Sprintf("%10.6f[2m%s[0;%sm", size, product.Symbol, style)),
		ifZero(price*size, "", "≈"),
		ifZero(cost, logPadding[:11],
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
		var Log = wrangler.Product.Log

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
			var unchanged = (price == wrangler.Price && size == wrangler.Size)
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
				Log(wrangler.Substyle, wrangler.Side+" too small", price, size)
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
				Log(wrangler.Substyle, wrangler.Side+" create retry", 0, 0)
				goto create_order
			} else if IsMalformedResponse(err) {
				Log(wrangler.Substyle, wrangler.Side+" malformed create response", 0, 0)
				goto create_order
			} else if IsBroke(err) {
				Log(wrangler.Substyle, wrangler.Side+" broke", price, size)
				continue
			} else if err != nil && !IsNotFound(err) && !IsDone(err) {
				Log(wrangler.Substyle, wrangler.Side+" create failed", price, size)
				log.Panic(err)
			}

			wrangler.Mutex.Lock()
			wrangler.Id = confirm.Id
			wrangler.Price = confirm.Price
			wrangler.Size = confirm.Size
			wrangler.Mutex.Unlock()

			Log(wrangler.Style, wrangler.Side, confirm.Price, confirm.Size)
		}
	}()

	return wrangler
}

func (wrangler *OrderWrangler) Cancel() error {
	var Log = wrangler.Product.Log

	wrangler.Mutex.Lock()
	var (
		id    = wrangler.Id
		price = wrangler.Price
		size  = wrangler.Size
	)
	wrangler.Size = 0.0 // for zeroing outstanding basis computations
	wrangler.Mutex.Unlock()

	if id != "" {

	cancel_order:
		<-exchangeThrottle
		//err := exchange.CancelOrder(id)
		err := CancelOrder(exchange, id)

		if IsExpiredRequest(err) {
			// retry
			Log(wrangler.Substyle, wrangler.Side+" cancel retry", 0, 0)
			goto cancel_order
		} else if IsMalformedResponse(err) {
			Log(wrangler.Substyle, wrangler.Side+" malformed cancel response", 0, 0)
			goto cancel_order
		} else if err != nil && !IsNotFound(err) && !IsDone(err) {
			Log(wrangler.Substyle, wrangler.Side+" cancel failed", 0, 0)
			return err
		}

		Log(wrangler.Style, wrangler.Side+" cancel", price, size)
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
	var err error

	exchange = gdax.NewClient(secret, key, passphrase)

	exchangeTime, err = GetTime()

	<-exchangeThrottle
	var feed = NewFeed(secret, key, passphrase)

	var (
		p, _    = GetProduct(productId)
		product = &Product{
			Product:       *p,
			Symbol:        "Ξ",
			FiatSymbol:    "$",
			SizeIncrement: 1 / 1e4,
			Rate:          rate,
		}
		buys   = Buys{Product: product}
		market = NewMarket(productId)
		bidder = product.OrderWrangler("buy")
		asker  = product.OrderWrangler("sell")
	)

	product.Log("32;2", "market bid", market.Bid, 0)
	product.Log("31;2", "market ask", market.Ask, 0)

	err = buys.Restore()
	if err != nil {
		log.Panic(err)
	}

	var messages = make(chan gdax.Message, 10)
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
			if bidder.Size > 0.0 {
				product.Log("32;2", "market bid", bid, 0)
				bidder.Replace <- Order{bid, basis / bid}
			}
			bidder.Mutex.Unlock()
		}
	}()

	// reprice if gap is shrinking and it's still profitable
	// also has the side effect of occasionally inflating
	go func() {
		for ask := range market.AskChanged {
			asker.Mutex.Lock()
			if asker.Size > 0.0 {
				product.Log("31;2", "market ask", ask, 0)
				asker.Replace <- buys.NextAsk(market.Ask)
			}
			asker.Mutex.Unlock()
		}
	}()

	// record bid-fill and maybe lower the ask order
	go func() {
		for buy := range bidder.Filled {
			buys.Bought(buy)
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
	var interrupt = make(chan os.Signal, 1)
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
		if !message.Time.Time().IsZero() {
			exchangeTime = message.Time
		}

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

func IsMalformedResponse(err error) bool {
	return err != nil && err.Error() == "invalid character '<' looking for beginning of value"
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
