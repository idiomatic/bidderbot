package main

import (
	//redis "github.com/garyburd/redigo/redis"
	"fmt"
	gdax "github.com/preichenberger/go-coinbase-exchange"
	"log"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// ⨯ → ≈ ∨

// string bid:cost:productId → cost
// string bid:id:productId → id
// list ask:id → [ productId:price:fee:boughtAt ]

const (
	productId   = "ETH-USD"
	fiatPrefix  = "$"
	coinPrefix  = "Ξ"
	pricePrefix = fiatPrefix
	priceSuffix = "/" + coinPrefix

	bidMoreInterval   = time.Minute
	bidAdjustInterval = time.Minute / 10
	askAdjustInterval = time.Minute / 10

	rate                  = 0.02
	bidCostIncrement Cost = 1.00
	minProfitFiat    Cost = 0.01 // since GDAX truncates USD
)

var (
	secret     = os.Getenv("COINBASE_SECRET")
	key        = os.Getenv("COINBASE_KEY")
	passphrase = os.Getenv("COINBASE_PASSPHRASE")

	exchange         *gdax.Client
	exchangeThrottle = time.Tick(time.Second / 2)

	marketBid   Price
	marketAsk   Price
	marketMutex sync.Mutex

	pricePlaces int
	sizePlaces  = 4
	minPrice    Price
	minSize     Size

	hunger     = bidCostIncrement
	bid        gdax.Order
	flips      Flips
	flipsMutex sync.Mutex
)

type Price float64
type Size float64
type Cost float64

func (p Price) String() string { return fmt.Sprintf(pricePrefix+"%.2f"+priceSuffix, p) }
func (s Size) String() string  { return fmt.Sprintf(coinPrefix+"%.4f", s) }
func (c Cost) String() string  { return fmt.Sprintf(fiatPrefix+"%.2f", c) }
func PxS(price Price, size Size) Cost {
	return Cost(float64(price) * float64(size))
}

type Ask gdax.Order

type Flip struct {
	Size     Size      `json:"size"`
	BidPrice Price     `json:"bid_price"`
	BidFee   Cost      `json:"bid_fee"`
	BidTime  gdax.Time `json:"bid_time"`
	AskPrice Price     `json:"ask_price"`
	Ask      *Ask      `json:"ask"`
}

type Flips []Flip
type ByFlipAskPrice Flips

func (a ByFlipAskPrice) Len() int           { return len(a) }
func (a ByFlipAskPrice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFlipAskPrice) Less(i, j int) bool { return a[i].AskPrice < a[j].AskPrice }

func (f *Flip) InitialAskPrice() Price {
	minAskCost := PxS(f.BidPrice, f.Size) + f.BidFee + minProfitFiat
	minAskPrice := Price(Ceil(float64(minAskCost)/float64(f.Size), pricePlaces))

	/*
		log.Println(Red(Dim("ask min      %s ⨯ %s + %s + %s ≈ %s ≈ %s ⨯ %s ≈ %s",
			f.BidPrice, f.Size, f.BidFee, minProfitFiat, minAskCost,
			minAskPrice, f.Size, PxS(minAskPrice, f.Size))))
	*/
	return Price(math.Max(
		float64(marketAsk),
		float64(minAskPrice)))
}

func (f *Flip) InflatingAskPrice(when gdax.Time) Price {
	dt := when.Time().Sub(f.BidTime.Time())
	feeFrac := float64(f.BidFee) / float64(f.Size)
	inflatedPrice := Price(Floor(float64(f.BidPrice)*Inflation(dt)+feeFrac, pricePlaces))
	/*
		intervals := math.Ceil(dt.Seconds() / askAdjustInterval.Seconds())
			log.Println(Dim("intervals    %.1f → %s", intervals, Price(intervals*0.01)))
		inflatedPrice := Price(Floor(
			float64(f.BidPrice)+intervals*0.01+feeFrac, pricePlaces))
	*/
	max := Price(math.Max(
		float64(f.InitialAskPrice()),
		float64(inflatedPrice)))
	/*
		log.Println(Red(Dim("ask inflate  %s ⨯ %s + %s ≈ ((%s → %s ∨ %s → %s) + %s) ⨯ %s ≈ %s",
			f.BidPrice, f.Size, f.BidFee,
			f.BidPrice, f.InitialAskPrice(), inflatedPrice, max,
			Price(feeFrac), f.Size, PxS(max, f.Size))))
	*/
	return max
}

func (f Flips) PlaceCombinedAsk() error {
	var (
		openSize    = make(map[Price]Size)
		desiredSize = make(map[Price]Size)
		asksById    = make(map[string]*Ask)
		asksByPrice = make(map[Price][]*Ask)
	)

	if len(f) == 0 {
		return nil
	}

	flipsMutex.Lock()
	for _, flip := range f {
		desiredSize[flip.AskPrice] += flip.Size
		if flip.Ask == nil {
			// no corresponding ask (yet)
			continue
		}
		if _, ok := asksById[flip.Ask.Id]; ok {
			// combined ask
			continue
		}
		askPrice := Price(flip.Ask.Price)
		openSize[askPrice] += Size(flip.Ask.Size)
		asksById[flip.Ask.Id] = flip.Ask
		if _, ok := asksByPrice[askPrice]; !ok {
			asksByPrice[askPrice] = []*Ask{}
		}
		asksByPrice[askPrice] = append(asksByPrice[askPrice], flip.Ask)
	}
	flipsMutex.Unlock()

	/*
		for price, size := range openSize {
			fmt.Printf("open %s → %s\n", price, size)
		}
		for price, size := range desiredSize {
			fmt.Printf("desired %s → %s\n", price, size)
		}
	*/
	/*
		for id, ask := range asksById {
			log.Println(Red(Dim("ask byId     %s → %s ⨯ %s ≈ %s",
				id, Price(ask.Price), Size(ask.Size),
				PxS(Price(ask.Price), Size(ask.Size)))))
		}
		for price, asks := range asksByPrice {
			for _, ask := range asks {
				log.Println(Red(Dim("ask byPrice  %s → %s ⨯ %s ≈ %s",
					price, Price(ask.Price), Size(ask.Size),
					PxS(Price(ask.Price), Size(ask.Size)))))
			}
		}
	*/

	cancelAsk := func(ask *Ask) error {
		<-exchangeThrottle
		err := exchange.CancelOrder(ask.Id)
		if err != nil {
			if err.Error() != "Order already done" {
				return err
			}
		}

		flipsMutex.Lock() // unnecessary
		for i := range f {
			flip := &f[i]
			if flip.Ask == ask {
				flip.Ask = nil
			}
		}
		flipsMutex.Unlock()

		var aBP []*Ask
		price := Price(ask.Price)
		for _, other := range asksByPrice[price] {
			if other != ask {
				aBP = append(aBP, other)
			}
		}
		asksByPrice[price] = aBP
		openSize[price] -= Size(ask.Size)
		delete(asksById, ask.Id)

		return nil
	}

	// remove over-ask at a particlar price
	for price, asks := range asksByPrice {
		for openSize[price] > desiredSize[price] && len(asks) > 0 {
			ask := asks[0]
			asks = asks[1:]
			log.Println(Red("ask cancel   %s ⨯ %s ≈ %s",
				Price(ask.Price), Size(ask.Size),
				PxS(Price(ask.Price), Size(ask.Size))))
			err := cancelAsk(ask)
			if err != nil {
				return err
			}
		}
	}

	// HACK allow coin to settle for sufficient funding
	time.Sleep(askAdjustInterval / 2)

	for _, flip := range f {
		if flip.Ask == nil {
			price := flip.AskPrice
			size := Size(Floor(float64(desiredSize[price]-openSize[price]), sizePlaces))
			if openSize[price] > 0 {
				log.Println(Red(Dim("ask want     %s ⨯ %s → %s (%s)",
					price, openSize[price], desiredSize[price], size)))
			}
			if price < minPrice || size < minSize {
				log.Println(Red(Dim("ask later    %s ⨯ %s ≈ %s",
					price, size, PxS(price, size))))
				continue
			}

			log.Println(Red("ask          %s ⨯ %s ≈ %s",
				price, size, PxS(price, size)))
			confirm, err := CreateAsk(price, size)
			if err != nil {
				if err.Error() == "Insufficient funds" {
					log.Println(Red(Bold("ask no coin  %s ⨯ %s ≈ %s",
						price, size, PxS(price, size))))
					// try again later
					continue
				}
				return err
			}
			openSize[price] += Size(confirm.Size)
			asksById[confirm.Id] = &confirm
			asksByPrice[price] = append(asksByPrice[price], &confirm)

			// XXX may under-buy this generation if a flip was added mid-flight
			for i := range f {
				inner := &f[i]
				if inner.AskPrice == price {
					inner.Ask = &confirm
				}
			}
		}
	}

	// XXX ask more when all flips have an incumbent ask?

	return nil
}

func CreateAsk(price Price, size Size) (Ask, error) {
	ask := gdax.Order{
		Type:      "limit",
		Side:      "sell",
		ProductId: productId,
		Price:     float64(price),
		Size:      float64(size),
		PostOnly:  true,
	}

	<-exchangeThrottle
	confirm, err := exchange.CreateOrder(&ask)
	return Ask(confirm), err
}

func LoadProduct() error {
	products, err := exchange.GetProducts()
	if err != nil {
		return err
	}

	for _, product := range products {
		if product.Id == productId {
			pricePlaces = int(-math.Log10(product.QuoteIncrement))
			sizePlaces = 4
			minPrice = Price(product.QuoteIncrement)
			minSize = Size(product.BaseMinSize)
		}
	}

	ticker, err := exchange.GetTicker(productId)
	if err != nil {
		return err
	}

	marketMutex.Lock()
	marketBid = Price(ticker.Bid)
	marketAsk = Price(ticker.Ask)
	log.Println(Dim("market      [%s, %s]", marketBid, marketAsk))
	marketMutex.Unlock()

	return nil
}

func main() {
	exchange = gdax.NewClient(secret, key, passphrase)

	err := LoadProduct()
	if err != nil {
		log.Panic(err)
	}

	// XXX recover open bid
	// XXX recover flips from fills & orders

	/*
		var bidMoved = make(chan Price)
		var inflation = make(chan bool)
	*/

	go func() {
		for range time.Tick(bidMoreInterval) {
			hunger += bidCostIncrement
			/*
				log.Println(Dim("hunger       %s", hunger))
			*/
		}
	}()

	go func() {
		for range time.Tick(bidAdjustInterval) {
			marketMutex.Lock()
			price := marketBid
			size := Size(Floor(float64(hunger)/float64(marketBid), sizePlaces))
			marketMutex.Unlock()

			if price != Price(bid.Price) || size != Size(bid.Size) {
				if bid.Id != "" {
					<-exchangeThrottle
					err := exchange.CancelOrder(bid.Id)
					if err != nil {
						if err.Error() != "Order already done" &&
							err.Error() != "order not found" &&
							err.Error() != "NotFound" {
							log.Panic(err)
						}
					}
					bid.Id = ""
				}

				if price >= minPrice && size >= minSize {
					<-exchangeThrottle

					msg := Green("bid adjust   %s ⨯ %s ≈ %s → %s ⨯ %s ≈ %s",
						Price(bid.Price), Size(bid.Size),
						PxS(Price(bid.Price), Size(bid.Size)),
						price, size, PxS(price, size))

					bid, err = exchange.CreateOrder(&gdax.Order{
						Type:      "limit",
						Side:      "buy",
						ProductId: productId,
						Price:     float64(price),
						Size:      float64(size),
						PostOnly:  true,
					})
					if err != nil {
						if err.Error() == "Insufficient funds" {
							log.Println(Green(Bold("bid no cash  %s ⨯ %s ≈ %s",
								price, size, PxS(price, size))))
							hunger /= 2
							continue
						} else {
							log.Println(msg)
							log.Panic(err)
						}
					}
					log.Println(msg)
				}
			}
		}
	}()

	go func() {
		// HACK to get it out of step with bidAdjust
		time.Sleep(time.Second)

		for range time.Tick(askAdjustInterval) {
			now, err := GetServerTime(exchange)
			if err != nil {
				log.Panic(err)
			}

			flipsMutex.Lock()
			for i := range flips {
				flip := &flips[i]
				price := flip.InflatingAskPrice(now)
				if price != flip.AskPrice {
					log.Println(Red(Dim("inflating    %s ⨯ %s ≈ %s → %s ⨯ %s ≈ %s",
						flip.AskPrice, flip.Size, PxS(flip.AskPrice, flip.Size),
						price, flip.Size, PxS(price, flip.Size))))
				}
				flip.AskPrice = price
			}
			sort.Sort(ByFlipAskPrice(flips))
			flipsMutex.Unlock()

			err = flips.PlaceCombinedAsk()
			if err != nil {
				log.Panic(err)
			}
		}
	}()

	messages, err := Subscribe(secret, key, passphrase, productId)
	if err != nil {
		log.Panic(err)
	}

	for message := range messages {
		switch message.Type {
		case "received":
			switch message.OrderType {
			case "limit":
				marketMutex.Lock()
				switch message.Side {
				case "buy":
					if Price(message.Price) < marketAsk {
						/*
							// HACK crazy limits
							if message.Price > marketBid {
								log.Println(Dim("market      [%s, %s]",
									marketBid, marketAsk))
							}
						*/
						marketBid = Price(math.Max(float64(marketBid),
							float64(message.Price)))
					}
				case "sell":
					if Price(message.Price) > marketBid {
						/*
							if message.Price < marketAsk {
								log.Println(Dim("market      [%s, %s]",
									marketBid, marketAsk))
							}
						*/
						marketAsk = Price(math.Min(float64(marketAsk),
							float64(message.Price)))
					}
				}
				marketMutex.Unlock()
			}
		case "match":
			marketMutex.Lock()
			switch message.Side {
			case "buy":
				marketBid = Price(message.Price)
			case "sell":
				marketAsk = Price(message.Price)
			}
			marketMutex.Unlock()

			switch message.Side {
			case "buy":
				if message.MakerOrderId == bid.Id && bid.Id != "" {
					// XXX chan to separate concerns?
					price := Price(message.Price)
					size := Size(message.Size)
					flip := Flip{
						Size:     size,
						BidPrice: price,
						BidFee:   0.0, // XXX
						BidTime:  message.Time,
					}
					flip.AskPrice = flip.InitialAskPrice()

					log.Println(GreenBackground(White("flip         %s ⨯ %s ≈ %s → %s ⨯ %s ≈ %s",
						price, size, PxS(price, size),
						flip.AskPrice, flip.Size,
						PxS(flip.AskPrice, flip.Size))))

					/*
						confirm, err := CreateAsk(flip.AskPrice, flip.Size)
						if err != nil {
							if err.Error() == "Insufficient funds" {
								log.Println(RedBackground(White("flip no coin %s ⨯ %s ≈ %s",
									flip.AskPrice, flip.Size,
									PxS(flip.AskPrice, size))))
							} else {
								log.Panic(err)
							}
						}
						flip.Ask = &confirm
					*/

					flipsMutex.Lock()
					flips = append(flips, flip)
					flipsMutex.Unlock()

					hunger = bidCostIncrement
				}
			case "sell":
				price, size := Price(message.Price), Size(message.Size)

				// remove flip(s)
				flipsMutex.Lock()
				var newFlips Flips
				for _, flip := range flips {
					if flip.Ask != nil && flip.Ask.Id != message.MakerOrderId {
						newFlips = append(newFlips, flip)
					} else {
						costBasis := PxS(flip.BidPrice, flip.Size)
						costSold := PxS(price, size)
						profit := costSold - costBasis
						log.Println(RedBackground(White("ask sold     %s ⨯ %s ≈ %s → %s ⨯ %s ≈ %s (%s)",
							flip.BidPrice, flip.Size, costBasis,
							price, size, costSold, profit)))
					}
				}
				flips = newFlips
				flipsMutex.Unlock()

				// XXX should decrement openSize and remove from asksByPrice
			}
		}
	}
}

func GetServerTime(exchange *gdax.Client) (gdax.Time, error) {
	<-exchangeThrottle
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
