package main

import (
	"fmt"
	redigo "github.com/garyburd/redigo/redis"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ⨯ multiply
// → yields/becomes
// ≈ approx equal
// ∨ maximum
// ∧ minimum
// Δ difference

// string bender:bid id
// set/zset bender:flips size|bidPrice|bidFee|bidTime askPrice
// list bender:asks [id]

const (
	// XXX argv[]
	productId = "ETH-USD"

	bidMoreInterval   = time.Minute
	bidAdjustInterval = time.Minute / 10
	askAdjustInterval = time.Minute / 10
)

var (
	secret     = os.Getenv("COINBASE_SECRET")
	key        = os.Getenv("COINBASE_KEY")
	passphrase = os.Getenv("COINBASE_PASSPHRASE")

	exchange         *gdax.Client
	exchangeThrottle = time.Tick(time.Second / 2)

	//Products map[string]Product
	product Product

	fiatBroke = false
)

/*
// XXX need product-pointing (or format/places copying) structs for format & places
type Numeric struct {
	Value  float64
	Places int
	Format string
}

func (n Numeric) String() string {
	f := n.Format
	if f != "" {
		f = "%.4f"
	}
	return fmt.Sprintf(f, n)
}
*/

type Price float64
type Size float64
type Cost float64

/*
type Price Numeric
type Size Numeric
type Cost Numeric
*/

func (p Price) String() string { return fmt.Sprintf(product.PriceFormat, p) }
func (s Size) String() string  { return fmt.Sprintf(product.SizeFormat, s) }
func (c Cost) String() string  { return fmt.Sprintf(product.FiatFormat, c) }

func PxS(price Price, size Size) Cost {
	return Cost(float64(price) * float64(size))
}

type ByPrice []Price

func (a ByPrice) Len() int           { return len(a) }
func (a ByPrice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPrice) Less(i, j int) bool { return a[i] < a[j] }

type Feed struct{}

type Product struct {
	Id             string
	FiatFormat     string // %.2f
	PriceFormat    string // %.2f$/Ξ
	SizeFormat     string // %.4fΞ
	Rate           float64
	Bid            Bid // XXX []Bid for re-up CancelOrder() optimization
	BidMutex       sync.Mutex
	BidCost        Cost
	BidIncrement   Cost
	MaxBid         Cost
	MinProfit      Cost
	PricePlaces    int
	SizePlaces     int
	MinPrice       Price
	MinSize        Size
	MarketBid      Price
	MarketAsk      Price
	MarketMutex    sync.Mutex
	Flips          []Flip
	FlipsMutex     sync.Mutex
	Asks           map[string]Ask
	AsksMutex      sync.Mutex
	MarketBidMoved chan Price
	MarketAskMoved chan Price
	AsksInvalidate chan bool
}

type Bid gdax.Order

func (product *Product) LoadBid() error {
	state := redisPool.Get()
	defer state.Close()

	bidId, err := redigo.String(state.Do("GET", "bender:bid:"+product.Id))
	if err != nil {
		if err == redigo.ErrNil {
			return nil
		}
		return err
	}

	if bidId == "" {
		return nil
	}

	<-exchangeThrottle
	order, err := exchange.GetOrder(bidId)
	if err != nil {
		if err.Error() == "NotFound" {
			return nil
		} else {
			return err
		}
	}

	product.BidMutex.Lock()
	defer product.BidMutex.Unlock()

	product.Bid = Bid(order)

	return nil
}

func (product *Product) LoadFlips() error {
	state := redisPool.Get()
	defer state.Close()

	flipsData, err := redigo.Strings(state.Do("ZRANGE", "bender:flips:"+product.Id, 0, -1))
	if err != nil {
		return err
	}

	product.FlipsMutex.Lock()
	defer product.FlipsMutex.Unlock()

	for _, flipData := range flipsData {
		flip, err := DecodeFlip(flipData, *product)
		if err != nil {
			return err
		}
		product.Flips = append(product.Flips, flip)
		log.Println(Dim("flip load    %s", flip.Encode()))
	}

	return nil
}

func (product *Product) LoadAsks() error {
	product.AsksMutex.Lock()
	defer product.AsksMutex.Unlock()

	// XXX insert a channel between Subscribe() to act on the Flips model?

	log.Fatal("NYI")

	return nil
}

func (bid Bid) Save() error {
	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("SET", "bender:bid:"+bid.ProductId, bid.Id)
	if err != nil {
		return err
	}

	return nil
}

type Ask gdax.Order

type Flip struct {
	ProductId string    `json:"product_id"`
	Size      Size      `json:"size"`
	BidPrice  Price     `json:"bid_price"`
	BidFee    Cost      `json:"bid_fee"`
	BidTime   gdax.Time `json:"bid_time"`
	AskPrice  Price     `json:"ask_price"`
}

func (flip *Flip) CollectionKey() string {
	return "bender:flips:" + flip.ProductId
}

func (flip *Flip) Encode() string {
	bidTime := flip.BidTime.Time().Format(time.RFC3339)
	return fmt.Sprintf("%.4f|%.2f|%.2f|%s",
		flip.BidPrice, flip.Size, flip.BidFee, bidTime)
}

func DecodeFlip(data string, product Product) (Flip, error) {
	s := strings.SplitN(data, "|", 4)
	price, err := strconv.ParseFloat(strings.TrimSpace(s[0]), 64)
	if err != nil {
		return Flip{}, err
	}
	size, err := strconv.ParseFloat(strings.TrimSpace(s[1]), 64)
	if err != nil {
		return Flip{}, err
	}
	// HACK switch order of values
	if price < 1.0 && 80 < size && size < 100 {
		price, size = size, price
	}

	fee, err := strconv.ParseFloat(strings.TrimSpace(s[2]), 64)
	if err != nil {
		return Flip{}, err
	}
	bidTime, err := time.Parse(time.RFC3339, strings.TrimSpace(s[3]))
	if err != nil {
		return Flip{}, err
	}
	return Flip{
		ProductId: product.Id,
		Size:      Size(size),
		BidPrice:  Price(price),
		BidFee:    Cost(fee),
		BidTime:   gdax.Time(bidTime),
	}, nil
}

func (flip *Flip) Save() error {
	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("ZADD", flip.CollectionKey(),
		Round(float64(flip.AskPrice), product.PricePlaces), flip.Encode())
	if err != nil {
		return err
	}

	return nil
}

func (flip *Flip) Delete() error {
	state := redisPool.Get()
	defer state.Close()

	_, err := state.Do("ZREM", flip.CollectionKey(), flip.Encode())
	if err != nil {
		return err
	}

	return nil
}

//type Flips []Flip
type ByFlipAskPrice []Flip

func (a ByFlipAskPrice) Len() int           { return len(a) }
func (a ByFlipAskPrice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFlipAskPrice) Less(i, j int) bool { return a[i].AskPrice < a[j].AskPrice }

type ByAsksPrice []Ask

func (a ByAsksPrice) Len() int           { return len(a) }
func (a ByAsksPrice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAsksPrice) Less(i, j int) bool { return a[i].Price < a[j].Price }

func (flip *Flip) InflatingAskPrice(when gdax.Time) Price {
	feeFrac := float64(flip.BidFee) / float64(flip.Size)
	minAskCost := PxS(flip.BidPrice, flip.Size) + flip.BidFee + product.MinProfit
	minAskPrice := Price(Ceil(float64(minAskCost)/float64(flip.Size), product.PricePlaces))
	dt := when.Time().Sub(flip.BidTime.Time())
	inflatedPrice := Price(float64(flip.BidPrice)*Inflation(dt, product.Rate) + feeFrac)

	product.MarketMutex.Lock()
	marketAsk := product.MarketAsk
	product.MarketMutex.Unlock()

	max := Price(Round(
		math.Max(
			math.Max(
				float64(marketAsk),
				float64(minAskPrice)),
			float64(inflatedPrice)),
		product.PricePlaces))

	/*
		log.Println(Dim("inflating    %.4f %.4f %.4f → %s %s %s → %s",
			float64(marketAsk),
			float64(minAskPrice),
			float64(inflatedPrice),
			marketAsk,
			minAskPrice,
			inflatedPrice,
			max))
	*/
	return max
}

func (product *Product) InflateAll() (bool, error) {
	var changed bool

	<-exchangeThrottle
	now, err := GetServerTime()
	if err != nil {
		return changed, err
	}

	product.FlipsMutex.Lock()
	defer product.FlipsMutex.Unlock()

	for i := range product.Flips {
		flip := &product.Flips[i]
		price := flip.InflatingAskPrice(now)
		if price != flip.AskPrice {
			/*
				log.Println(Dim("inflating     %10s → %10s  ⨯             %8s  ≈            %7s",
					flip.AskPrice, price, flip.Size,
					PxS(flip.AskPrice, flip.Size)))
			*/
			flip.AskPrice = price
			changed = true

			err := flip.Save()
			if err != nil {
				return changed, err
			}
		}
	}
	sort.Sort(ByFlipAskPrice(product.Flips))

	return changed, nil
}

func (product *Product) Desires() map[Price]Size {
	var desires = make(map[Price]Size)

	product.FlipsMutex.Lock()
	for _, flip := range product.Flips {
		desires[flip.AskPrice] += flip.Size
	}
	product.FlipsMutex.Unlock()

	return desires
}

func (product *Product) Asking() map[Price]Size {
	var asking = make(map[Price]Size)

	product.AsksMutex.Lock()
	for _, ask := range product.Asks {
		asking[Price(ask.Price)] += Size(ask.Size)
	}
	product.AsksMutex.Unlock()

	return asking
}

func (product *Product) BidMore() {
	go func() {
		if product.BidIncrement <= 0 {
			log.Println(Green(Dim("bid increment                                                               %7s", Cost(product.BidIncrement))))
			return
		}

		for range time.Tick(bidMoreInterval) {
			product.BidMutex.Lock()
			product.BidCost += product.BidIncrement
			if product.BidCost > product.MaxBid {
				product.BidCost = product.MaxBid
			}
			product.BidMutex.Unlock()
		}
	}()
}

func (product *Product) AdjustBid() {
	go func() {
		if product.BidCost <= 0 {
			log.Println(Green(Dim("bid cost                                                                    %7s", Cost(0))))
			return
		}

		for {
			if fiatBroke {
				time.Sleep(bidAdjustInterval)
			} else {
				select {
				case <-product.MarketBidMoved:
				case <-time.After(bidAdjustInterval):
				}
			}

			product.MarketMutex.Lock()
			price := product.MarketBid
			product.MarketMutex.Unlock()

			size := Size(Floor(
				float64(product.BidCost)/float64(price), product.SizePlaces))

			product.BidMutex.Lock()
			var (
				bidPrice = product.Bid.Price
				bidSize  = product.Bid.Size
				bidId    = product.Bid.Id
			)
			product.BidMutex.Unlock()

			if price == Price(bidPrice) && size == Size(bidSize) {
				continue
			}

			var msg string

			if bidId != "" {
				// XXX monitor external order cancels?
				msg = Green("bid adjust    %10s → %10s  ⨯  %8s → %8s  ≈  %7s → %7s",
					Price(product.Bid.Price), price,
					Size(product.Bid.Size), size,
					PxS(Price(product.Bid.Price), Size(product.Bid.Size)),
					PxS(price, size))

			cancel_retry:
				<-exchangeThrottle
				err := exchange.CancelOrder(bidId)
				if err != nil {
					if err.Error() == "request timestamp expired" {
						// try again later
						log.Println(Green(Bold("bid cancel   retry")))
						goto cancel_retry
					} else if err.Error() == "Order already done" ||
						err.Error() == "order not found" ||
						err.Error() == "NotFound" {
					} else {
						log.Panic(err)
					}
				}

				product.BidMutex.Lock()
				if product.Bid.Id == bidId {
					product.Bid.Id = ""
				}
				product.BidMutex.Unlock()
			} else {
				msg = Green("bid new                    %10s  ⨯             %8s  ≈            %7s",
					price, size, PxS(price, size))
			}

			if price < product.MinPrice || size < product.MinSize {
				continue
			}

			bid, err := CreateBid(price, size)
			if err != nil {
				if err.Error() == "Insufficient funds" {
					if !fiatBroke {
						log.Println(Green(Bold("bid no cash                %10s  ⨯             %8s  ≈            %7s",
							price, size, PxS(price, size))))

						// reset to minimum bidding (BidMore() may bump it up)
						product.BidCost = 0
						fiatBroke = true
					}
					continue
				} else {
					log.Println(msg)
					log.Panic(err)
				}
			}

			if bid.Status != "open" && bid.Status != "pending" {
				log.Println(Green(Bold("bid %-16s       %10s  ⨯             %8s  ≈            %7s",
					bid.Status, price, size, PxS(price, size))))
				continue
			}

			fiatBroke = false

			product.BidMutex.Lock()
			product.Bid = bid
			product.BidMutex.Unlock()

			// chatty
			log.Println(msg)

			err = product.Bid.Save()
			if err != nil {
				log.Panic(err)
			}
		}
	}()
}

func (product *Product) AdjustAskExpectations() {
	go func() {
		if product.MinProfit <= 0 {
			log.Println(YellowBackground("min profit                                                                  %7s", product.MinProfit))
		}
		if product.Rate <= 0 {
			log.Println(YellowBackground("rate                                                                        %7s", product.Rate))
		}

		// HACK to get it out of step with bidAdjust
		time.Sleep(time.Second)

		for range time.Tick(askAdjustInterval) {
			changed, err := product.InflateAll()
			if err != nil {
				log.Panic(err)
			}

			if changed {
				select {
				case product.AsksInvalidate <- true:
				default:
				}
			}
		}
	}()
}

func LogBalance() error {
	<-exchangeThrottle
	accounts, err := exchange.GetAccounts()
	if err != nil {
		return err
	}
	for _, account := range accounts {
		log.Println(Dim("balance %-3s %12.6f %12.6f %12.6f",
			account.Currency, account.Balance, account.Hold, account.Available))
	}
	return nil
}

func (product *Product) AdjustAsks() {
	go func() {
		for {
			select {
			case <-product.MarketAskMoved:
			case <-product.AsksInvalidate:
			}

			var (
				openSize    = product.Asking()
				desiredSize = product.Desires()
			)

			if len(product.Flips) == 0 {
				continue
			}

			/*
				var eitherPrices []Price
				var eitherPricesSeen = make(map[Price]bool)
				for price := range desiredSize {
					eitherPrices = append(eitherPrices, price)
					eitherPricesSeen[price] = true
				}
				for price := range openSize {
					if !eitherPricesSeen[price] {
						eitherPrices = append(eitherPrices, price)
					}
				}
				sort.Sort(ByPrice(eitherPrices))
				for _, price := range eitherPrices {
					log.Println(Dim("ask:demand   %s : %s → %s",
						price, openSize[price], desiredSize[price]))
				}
			*/

			// remove over-ask at a particlar price
			var cancellingAsks []Ask
			product.AsksMutex.Lock()
			for _, ask := range product.Asks {
				price := Price(ask.Price)
				size := Size(ask.Size)
				/*
					log.Println(Dim("open/demand                %10s  ⨯  %8s → %8s  ≈ %8s",
						price, openSize[price], desiredSize[price],
						size))
				*/
				if openSize[price] > desiredSize[price] {
					/*
						log.Println(Red(Dim("cancelling    %10s               ⨯  %8s → %8s  ≈  %7s",
							price, desiredSize[price], openSize[price],
							PxS(price, size))))
					*/
					cancellingAsks = append(cancellingAsks, ask)
					openSize[price] -= size
				}
			}
			product.AsksMutex.Unlock()

			// XXX should lazily cancel as coin is needed
			sort.Sort(ByAsksPrice(cancellingAsks))

			for _, ask := range cancellingAsks {
				price := Price(ask.Price)
				size := Size(ask.Size)
				log.Println(Red("ask cancel    %10s               ⨯  %8s             ≈  %7s",
					price, size, PxS(price, size)))

				// copy for concurrency
				askId := ask.Id

				if askId != "" {

				retry:
					<-exchangeThrottle
					err := exchange.CancelOrder(askId)
					if err != nil {
						if err.Error() == "request timestamp expired" {
							// try again later
							log.Println(Red(Bold("ask cancel   retry")))
							goto retry
						} else if err.Error() == "Order already done" ||
							err.Error() == "order not found" ||
							err.Error() == "NotFound" {
							continue
						} else {
							log.Panic(err)
						}
					}
				}
			}

			product.AsksMutex.Lock()
			for _, ask := range cancellingAsks {
				delete(product.Asks, ask.Id)
			}
			product.AsksMutex.Unlock()

			// HACK to allow time to settle; can omit if suitable coin balance exists
			time.Sleep(time.Millisecond * 200)

			var desiredPrices []Price
			for price, _ := range desiredSize {
				desiredPrices = append(desiredPrices, price)
			}
			sort.Sort(ByPrice(desiredPrices))
			for _, price := range desiredPrices {
				size := desiredSize[price] - openSize[price]
				size = Size(Round(float64(size), product.SizePlaces))
				if size <= 0 {
					continue
				}

				var msg string

				if price < product.MinPrice || size < product.MinSize {
					msg = Red(Dim("ask later                  %10s  ⨯  %8s             ≈            %7s",
						price, size, PxS(price, size)))
					continue
				}

				if openSize[price] > 0 {
					msg = Red("ask more                   %10s  ⨯  %8s → %8s  ≈            %7s",
						price, openSize[price], desiredSize[price],
						PxS(price, size))
				} else {
					msg = Red("ask                        %10s  ⨯             %8s  ≈            %7s",
						price, size, PxS(price, size))
				}

				confirm, err := CreateAsk(price, size)
				if err != nil {
					if err.Error() == "Insufficient funds" {
						_ = LogBalance()
						log.Println(Red(Bold("ask no coin                %10s  ⨯             %8s  ≈            %7s",
							price, size, PxS(price, size))))
						// try again later
						continue
					}
					log.Println(msg)
					log.Panic(err)
				}

				// XXX wish defer worked in scopes, not functions
				log.Println(msg)

				product.AsksMutex.Lock()
				product.Asks[confirm.Id] = confirm
				product.AsksMutex.Unlock()

				openSize[price] += Size(confirm.Size)
			}

			// XXX ask more when all flips have an incumbent ask?
		}
	}()
}

func (product *Product) MonitorMarket(feed *Feed) {
	log.Println(YellowBackground("MonitorMarket NYI"))
}

func (product *Product) MonitorTransactions(feed *Feed) {
	log.Println(YellowBackground("MonitorTransactions NYI"))
}

func GetServerTime() (gdax.Time, error) {
	serverTime, err := exchange.GetTime()
	if err != nil {
		return gdax.Time{}, err
	}
	now, err := time.Parse(time.RFC3339, serverTime.ISO)
	if err != nil {
		return gdax.Time{}, err
	}
	serverNow := gdax.Time(now)

	return serverNow, nil
}

func CreateBid(price Price, size Size) (Bid, error) {
	bid := gdax.Order{
		Type:      "limit",
		Side:      "buy",
		ProductId: product.Id,
		Price:     float64(price),
		Size:      float64(size),
		PostOnly:  true,
	}

retry:
	<-exchangeThrottle
	confirm, err := exchange.CreateOrder(&bid)
	if err != nil {
		if err.Error() == "request timestamp expired" {
			log.Println(Red(Bold("bid create   retry")))
			goto retry
		}
	}
	return Bid(confirm), err
}

func CreateAsk(price Price, size Size) (Ask, error) {
	ask := gdax.Order{
		Type:      "limit",
		Side:      "sell",
		ProductId: product.Id,
		Price:     float64(price),
		Size:      float64(size),
		PostOnly:  true,
	}

retry:
	<-exchangeThrottle
	confirm, err := exchange.CreateOrder(&ask)
	if err != nil && err.Error() == "request timestamp expired" {
		log.Println(Red(Bold("ask create   retry")))
		goto retry
	}
	return Ask(confirm), err
}

func LoadProduct(productId string) (Product, error) {
	var btc = Product{
		Id:          "BTC-USD",
		PriceFormat: "$%.2f/฿",
		SizeFormat:  "฿%.6f",
	}
	_ = btc
	var ltc = Product{
		Id:          "LTC-USD",
		PriceFormat: "$%.2f/Ł",
		SizeFormat:  "%.4fŁ",
	}
	_ = ltc

	var product = Product{
		Id:             "ETH-USD",
		FiatFormat:     "$%.2f",
		PriceFormat:    "%.2f$/Ξ",
		SizeFormat:     "%.4fΞ", // or "฿" or "Ł"
		Rate:           0.02,
		BidCost:        1.00,
		BidIncrement:   1.00,
		MaxBid:         10.00,
		MinProfit:      0.02, // GDAX truncates USD pennies
		Asks:           make(map[string]Ask),
		MarketAskMoved: make(chan Price, 1),
		MarketBidMoved: make(chan Price, 1),
		AsksInvalidate: make(chan bool, 1),
	}

	<-exchangeThrottle
	products, err := exchange.GetProducts()
	if err != nil {
		return product, err
	}

	for _, p := range products {
		if p.Id == productId {
			product.Id = productId
			product.PricePlaces = int(-math.Log10(p.QuoteIncrement))
			product.SizePlaces = 4
			product.MinPrice = Price(p.QuoteIncrement)
			product.MinSize = Size(p.BaseMinSize)
		}
	}

	<-exchangeThrottle
	ticker, err := exchange.GetTicker(product.Id)
	if err != nil {
		return product, err
	}

	product.MarketMutex.Lock()
	product.MarketBid = Price(ticker.Bid)
	product.MarketAsk = Price(ticker.Ask)
	product.MarketMutex.Unlock()

	return product, nil
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC)
}

func main() {
	var err error

	exchange = gdax.NewClient(secret, key, passphrase)

	{
		p, err := LoadProduct(productId)
		if err != nil {
			log.Panic(err)
		}
		product = p
	}
	product.MarketMutex.Lock()
	log.Println(Dim("market       [%10s,  %10s]",
		product.MarketBid, product.MarketAsk))
	product.MarketMutex.Unlock()

	// recover open bid
	err = product.LoadBid()
	if err != nil {
		log.Panic(err)
	}

	/*
		// XXX recover flips
		err = product.LoadFlips()
		if err != nil {
			log.Panic(err)
		}

		// XXX recover asks, subtracting filled and cascading deducting flips
		err = product.LoadAsks()
		if err != nil {
			log.Panic(err)
		}
	*/

	product.BidMore()
	product.AdjustBid()
	product.AdjustAskExpectations()
	product.AdjustAsks()

	feed := NewRealTimeFeed(&product)

	product.MonitorMarket(feed)
	product.MonitorTransactions(feed)

}

func NewRealTimeFeed(product *Product) *Feed {
	messages, connected, err := Subscribe(secret, key, passphrase, product.Id)
	if err != nil {
		log.Panic(err)
	}

	go func() {
		for up := range connected {
			if up {
				log.Println(YellowBackground(Bold("connected")))
			} else {
				log.Println(YellowBackground(Bold("disconnected")))
			}
		}
	}()

	for message := range messages {
		// XXX Round() unnecessary?
		price := Price(Round(message.Price, product.PricePlaces))
		size := Size(Round(message.Size, product.SizePlaces))

		switch message.Type {
		case "received":
			// track market prices
			switch message.OrderType {
			case "limit":
				product.MarketMutex.Lock()
				switch message.Side {
				case "buy":
					if price < product.MarketAsk {
						/*
							// HACK crazy limits
							if message.Price > product.MarketBid {
								log.Println(Dim("market       [%10s,  %10s]",
									product.MarketBid, product.MarketAsk))
							}
						*/
						product.MarketBid = Price(Round(
							math.Max(
								float64(product.MarketBid),
								message.Price),
							product.PricePlaces))

						select {
						case product.MarketBidMoved <- product.MarketBid:
						default:
						}
					}
				case "sell":
					if price > product.MarketBid {
						/*
							if message.Price < product.MarketAsk {
								log.Println(Dim("market       [%10s,  %10s]",
									product.MarketBid, product.MarketAsk))
							}
						*/
						product.MarketAsk = Price(Round(
							math.Min(
								float64(product.MarketAsk),
								message.Price),
							product.PricePlaces))

						select {
						case product.MarketAskMoved <- product.MarketAsk:
						default:
						}
					}
				}
				product.MarketMutex.Unlock()
			}

		case "match":
			// track market prices
			switch message.Side {
			case "buy":
				product.MarketMutex.Lock()
				product.MarketBid = price

				select {
				case product.MarketBidMoved <- product.MarketBid:
				default:
				}

				product.MarketMutex.Unlock()

			case "sell":
				product.MarketMutex.Lock()
				product.MarketAsk = price

				select {
				case product.MarketAskMoved <- product.MarketAsk:
				default:
				}

				product.MarketMutex.Unlock()
			}

			switch message.Side {
			case "buy":
				product.BidMutex.Lock()
				bidId := product.Bid.Id
				product.BidMutex.Unlock()

				if bidId != "" && message.MakerOrderId == bidId {
					// XXX chan to separate concerns?
					flip := Flip{
						ProductId: product.Id,
						Size:      size,
						BidPrice:  price,
						BidFee:    0.0, // XXX
						BidTime:   message.Time,
					}
					flip.AskPrice = flip.InflatingAskPrice(message.Time)
					basis := PxS(price, size)
					askCost := PxS(flip.AskPrice, flip.Size)
					log.Println(GreenBackground(White("flip          %10s → %10s  ⨯             %8s  ≈  %7s           Δ %7s",
						price, flip.AskPrice, size, basis, askCost-basis)))

					product.FlipsMutex.Lock()
					product.Flips = append(product.Flips, flip)
					product.FlipsMutex.Unlock()

					err := flip.Save()
					if err != nil {
						log.Panic(err)
					}

					// XXX partial bid satisfaction?
					product.BidCost = 0

					product.BidMutex.Lock()
					if product.Bid.Id == bidId {
						product.Bid.Id = ""
					}
					product.BidMutex.Unlock()

					select {
					case product.AsksInvalidate <- true:
					default:
					}
				}
			case "sell":
				// XXX what about combined asks that are responsible for multiple flips?

				costSold := PxS(price, size)
				var costBasis Cost

				// are we watching this ask?
				product.AsksMutex.Lock()
				_, ok := product.Asks[message.MakerOrderId]
				product.AsksMutex.Unlock()
				if !ok {
					continue
				}

				// remove some flip(s) and/or some portion of a flip
				var (
					deleteFlips []Flip
					newFlips    []Flip
				)
				product.FlipsMutex.Lock()
				remainder := size
				/*
					log.Println(Dim("flip remainder %8s, %d flips",
						remainder, len(product.Flips)))
				*/
				for i := range product.Flips {
					flip := &product.Flips[i]
					if flip.AskPrice != price {
						/*
							log.Println(Dim("flip retain   %10s → %10s  ⨯             %8s  ≈            %7s",
								flip.BidPrice, flip.AskPrice, flip.Size,
								PxS(flip.AskPrice, flip.Size)))
						*/
						newFlips = append(newFlips, *flip)
						continue
					}

					var contribution Size
					if flip.Size > Size(Round(float64(remainder), product.SizePlaces)) {
						contribution = remainder
						log.Println(Dim("flip split    %10s → %10s  ⨯  %8s → %8s  ≈            %7s",
							flip.BidPrice, flip.AskPrice, flip.Size, remainder,
							PxS(flip.AskPrice, flip.Size)))
						newFlips = append(newFlips, *flip)
					} else {
						contribution = flip.Size
						deleteFlips = append(deleteFlips, *flip)
						log.Println(Dim("flip sold     %10s → %10s  ⨯             %8s  ≈            %7s",
							flip.BidPrice, flip.AskPrice, flip.Size, PxS(flip.AskPrice, flip.Size)))
					}

					if contribution > 0 {
						flip.Size -= contribution
						remainder -= contribution
						costBasis += PxS(flip.BidPrice, contribution) + flip.BidFee
					}
				}
				product.Flips = newFlips
				product.FlipsMutex.Unlock()

				if remainder > 0 {
					log.Println(Dim("flip remainder %8s, %d flips",
						remainder, len(newFlips)))
				}

				for _, flip := range deleteFlips {
					// XXX gets here?
					/*
						log.Println(Dim("flip delete  %s", flip.Encode()))
					*/

					err := flip.Delete()
					if err != nil {
						log.Fatal(err)
					}
				}

				product.AsksMutex.Lock()
				delete(product.Asks, message.MakerOrderId)
				product.AsksMutex.Unlock()

				if costBasis > 0 {
					profit := costSold - costBasis
					log.Println(RedBackground(White("ask sold      %10s               ⨯ %8s              ≈  %7s → %7s Δ %7s",
						price, size, costBasis, costSold, profit)))
				}
			}
		}
	}

	return &Feed{}
}
