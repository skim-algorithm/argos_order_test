package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2"
	bcommon "github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"

	"arques.com/order/common"
	"arques.com/order/config"
	"arques.com/order/model"
)

type (
	Binance struct {
		aliasClients       map[string]*BinanceClient
		PropertiesBySymbol map[string]*SymbolProperties
	}

	BinanceClient struct {
		Info            *model.AliasInfo
		Client          *futures.Client
		UserDataStopC   chan struct{}
		Balance         map[string]*futures.Balance
		Rds             *redis.Client
		Logger          *log.Entry
		openOrders      map[int64]*model.Order
		positions       map[string]*Position
		Author          string //전략을 작성한 Author의 이름. 해당 슬랙 채널로 order 메시지를 전송한다.
		timeOffset      int64
		wg              sync.WaitGroup
		ctx             context.Context
		binanceTicker   *time.Ticker
		pingTicker      *time.Ticker
		keepAliveTicker *time.Ticker
	}

	EventTypeChecker struct {
		EventType       string `json:"e"`
		EventTime       int64  `json:"E"`
		TransactionTime int64  `json:"T"`
	}

	OrderUpdate struct {
		EventType       string       `json:"e"`
		EventTime       int64        `json:"E"`
		TransactionTime int64        `json:"T"`
		Result          *OrderResult `json:"o"`
	}

	OrderResult struct {
		Symbol                   string  `json:"s"`
		ClientOrderID            string  `json:"c"`
		Side                     string  `json:"S"`
		Type                     string  `json:"o"`
		TimeInForce              string  `json:"f"`
		OriginalQuantity         float64 `json:"q,string"`
		OriginalPrice            float64 `json:"p,string"`
		AveragePrice             float64 `json:"ap,string"`
		StopPrice                float64 `json:"sp,string"` // Stop Price. Please ignore with TRAILING_STOP_MARKET order
		ExecutionType            string  `json:"x"`
		OrderStatus              string  `json:"X"`
		OrderID                  int64   `json:"i"`
		OrderLastFilledQuantity  float64 `json:"l,string"`
		OrderFilledAccumQuantity float64 `json:"z,string"` // Order Filled Accumulated Quantity
		LastFilledPrice          float64 `json:"L,string"`
		CommissionAsset          string  `json:"N"`        // Commission Asset, will not push if no commission
		Commission               float64 `json:"n,string"` // Commission, will not push if no commission
		OrderTradeTime           int64   `json:"T"`
		TraceID                  int64   `json:"t"`
		BidsNotional             float64 `json:"b,string"`
		AskNotional              float64 `json:"a,string"`
		IsMakerSide              bool    `json:"m"` // Is this trade the maker side?
		IsReduceOnly             bool    `json:"R"` // Is this reduce only
		StopPriceWorkingType     string  `json:"wt"`
		OriginalOrderType        string  `json:"ot"`
		PositionSide             string  `json:"ps"`
		IsCloseConditional       bool    `json:"cp"`        // If Close-All, pushed with conditional order
		ActivationPrice          float64 `json:"AP,string"` // Activation Price, only puhed with TRAILING_STOP_MARKET order
		CallbackRate             float64 `json:"cr,string"` // Callback Rate, only puhed with TRAILING_STOP_MARKET order
		RealizedProfit           float64 `json:"rp,string"` // Realized Profit of the trade
	}

	Balance struct {
		Asset              string `json:"a"`
		WalletBalance      string `json:"wb"`
		CrossWalletBalance string `json:"cw"`
	}

	Position struct {
		Symbol         string  `json:"s"`
		PositionAmount float64 `json:"pa,string"`
		EntryPrice     float64 `json:"ep,string"`
		AccumRealized  float64 `json:"cr,string"` // pre-fee
		UnrealizedPnL  float64 `json:"up,string"`
		MarginType     string  `json:"mt"`
		IsolatedWallet float64 `json:"iw,string"` // if isolated position
		PositionSide   string  `json:"ps"`
	}

	Account struct {
		EventResponseType string      `json:"m"`
		Balances          []*Balance  `json:"B"`
		Positions         []*Position `json:"P"`
	}

	AccountUpdate struct {
		EventType       string   `json:"e"`
		EventTime       int64    `json:"E"`
		TransactionTime int64    `json:"T"`
		Account         *Account `json:"a"`
	}

	SymbolProperties struct {
		MaxQty         float64
		MinQty         float64
		StepSize       float64
		MarketMaxQty   float64
		MarketMinQty   float64
		MarketStepSize float64
		MinNotional    float64 // 최소 주문 크기 (USD)
		PriceTickSize  float64
	}
)

func init() {
	log.SetReportCaller(true)
	log.SetFormatter(&log.JSONFormatter{})
}

// New 는 새로운 거래소와 하위 클라이언트를 생성한다.
func New() (e *Binance) {
	e = &Binance{
		aliasClients:       make(map[string]*BinanceClient),
		PropertiesBySymbol: make(map[string]*SymbolProperties),
	}

	infos := model.LoadAliasInfos()
	for key, info := range infos {
		// 거래소 생성 시 모든 클라이언트 생성
		c := newClient(info)
		if c != nil {
			e.aliasClients[key] = c
		}
	}

	e.LoadSymbolProperties()
	return
}

func newClient(info *model.AliasInfo) *BinanceClient {
	c := &BinanceClient{
		Info: info,
		ctx:  context.Background(),
		// 클라이언트 로거 설정
		Logger: log.WithFields(log.Fields{
			"alias": info.ExchangeAlias,
		}),
	}

	if author, err := model.GetAuthor(info.ExchangeAlias); err == nil {
		c.Author = author
	} else {
		c.Logger.WithError(err).WithField("Alias", info.ExchangeAlias).Info("could not find strategy author")
	}
	client := binance.NewFuturesClient("", "")
	openOrders, err := client.NewListOpenOrdersService().Symbol("BNBUSDT").
		Do(context.Background())
	if err == nil {
		for _, o := range openOrders {
			fmt.Println(o)
		}
	}

	// 포지션 리스크 조회 서비스 생성
	positionRiskService := client.NewGetPositionRiskService()
	// 심볼을 지정하지 않으면 전체 포지션을 가져옴
	positionRisks, err := positionRiskService.Do(context.Background())
	if err != nil {
		log.Fatalf("포지션 리스크 조회 실패: %v", err)
	}
	// 포지션 리스크 출력
	for _, risk := range positionRisks {
		fmt.Printf("심볼: %s, 포지션 양: %s\n", risk.Symbol, risk.PositionAmt)
	}

	// 바이낸스 클라이언트 생성, 서버 시간 동기화 수행
	c.Client = binance.NewFuturesClient("", "")
	c.SyncTimeOffset(false)

	// 레디스 클라이언트 생성
	c.Rds = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Get().Redis.Url, config.Get().Redis.Port),
		Password: "",
		DB:       0,
	})

	// Balance 생성
	c.Balance = make(map[string]*futures.Balance)

	// 오픈 오더 정보 로드, 갱신
	if err := c.loadOpenOrders(); err != nil {
		c.Logger.WithError(err).Error("Failed to load open orders.")
		return nil
	}

	// 포지션 정보 로드
	if err := c.loadPositions(); err != nil {
		c.Logger.WithError(err).Error("Failed to load positions.")
		return nil
	}

	// 유저 스트림 시작
	if err := c.StartUserStream(); err != nil {
		c.Logger.WithError(err).Error("Failed to start userstream.")
		return nil
	}

	// Binance API 서버와 timeOffset을 맞춤
	c.binanceTicker = time.NewTicker(time.Second * 60)
	go func() {
		for range c.binanceTicker.C {
			withLogging := time.Now().Minute() == 0
			c.SyncTimeOffset(withLogging)
		}
	}()

	// 30초마다 레디스에 Ping 전송
	c.pingTicker = time.NewTicker(time.Second * 30)
	go func() {
		for range c.pingTicker.C {
			c.Publish(1)
		}
	}()

	c.Logger.Info("Binance client initialized.")

	return c
}

// Client 는 거래소의 해당 alias 클라이언트를 반환한다.
func (e *Binance) Client(exAlias string) *BinanceClient {
	for alias := range e.aliasClients {
		fmt.Println("-", alias)
	}

	if c, exist := e.aliasClients[exAlias]; exist {
		return c
	}

	// 해당 클라이언트 생성에 실패했었다면 DB에서 새로 정보를 로드해 생성 시도한다.
	infos := model.LoadAliasInfos()
	if info, exist := infos[exAlias]; exist {
		c := newClient(info)
		if c != nil {
			e.aliasClients[exAlias] = c
			return c
		}
	}

	log.WithField("alias", exAlias).Error("Cannot find alias info.")
	return nil
}

// UpdateClient 는 해당 alias의 client를 추가/제거/갱신 한다.
func (e *Binance) UpdateClient(exAlias string) error {
	infos := model.LoadAliasInfos()

	if info, exist := infos[exAlias]; exist {
		if c, exist := e.aliasClients[exAlias]; exist {
			c.Close()
		}
		newC := newClient(info)
		e.aliasClients[exAlias] = newC
	} else {
		if c, exist := e.aliasClients[exAlias]; exist {
			c.Close()
			delete(e.aliasClients, exAlias)
			log.WithField("alias", exAlias).Info("Alias delete.")
		} else {
			return errors.New("no alias to delete")
		}
	}

	return nil
}

func (e *Binance) UpdateAuthor(exAlias string) error {
	if c, exist := e.aliasClients[exAlias]; exist {
		if author, err := model.GetAuthor(exAlias); err == nil && author != "" {
			c.Author = author
			log.WithField("alias", exAlias).Info("Set author as " + author)
		} else {
			log.WithField("alias", exAlias).Info("Unset author")
		}
	} else {
		return errors.New("alias doesn't exist: " + exAlias)
	}

	return nil
}

func (e *Binance) GetTimeOffsets() (ret map[string]int64) {
	ret = make(map[string]int64)
	for alias, client := range e.aliasClients {
		ret[alias] = client.timeOffset
	}
	return
}

func (e *Binance) LoadSymbolProperties() (err error) {
	url := config.Get().API.Url
	url += "/api/symbol/properties"
	client := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}

	req.Header.Set("Authorization", "Bearer "+config.Get().API.Token)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	bytes, err := ioutil.ReadAll(resp.Body)
	var objMap map[string]([](map[string]interface{}))
	if err = json.Unmarshal(bytes, &objMap); err != nil {
		return
	}

	for _, v := range objMap["data"] {
		sym := v["symbol"].(string)
		props := v["properties"].([]interface{})
		_, exists := e.PropertiesBySymbol[sym]
		if !exists {
			e.PropertiesBySymbol[sym] = &SymbolProperties{}
		}

		for _, w := range props {
			m := w.(map[string]interface{})
			filterType := m["filterType"].(string)
			filterName := m["filterName"].(string)
			value := m["value"].(string)

			switch {
			case filterType == "LOT_SIZE" && filterName == "maxQty":
				e.PropertiesBySymbol[sym].MaxQty, _ = strconv.ParseFloat(value, 64)
			case filterType == "LOT_SIZE" && filterName == "minQty":
				e.PropertiesBySymbol[sym].MinQty, _ = strconv.ParseFloat(value, 64)
			case filterType == "LOT_SIZE" && filterName == "stepSize":
				e.PropertiesBySymbol[sym].StepSize, _ = strconv.ParseFloat(value, 64)
			case filterType == "MARKET_LOT_SIZE" && filterName == "maxQty":
				e.PropertiesBySymbol[sym].MarketMaxQty, _ = strconv.ParseFloat(value, 64)
			case filterType == "MARKET_LOT_SIZE" && filterName == "minQty":
				e.PropertiesBySymbol[sym].MarketMinQty, _ = strconv.ParseFloat(value, 64)
			case filterType == "MARKET_LOT_SIZE" && filterName == "stepSize":
				e.PropertiesBySymbol[sym].MarketStepSize, _ = strconv.ParseFloat(value, 64)
			case filterType == "MIN_NOTIONAL" && filterName == "notional":
				e.PropertiesBySymbol[sym].MinNotional, _ = strconv.ParseFloat(value, 64)
			case filterType == "PRICE_FILTER" && filterName == "tickSize":
				e.PropertiesBySymbol[sym].PriceTickSize, _ = strconv.ParseFloat(value, 64)
			}
		}
	}

	return
}

func (c *BinanceClient) Close() {
	if c.binanceTicker != nil {
		c.binanceTicker.Stop()
	}
	if c.pingTicker != nil {
		c.pingTicker.Stop()
	}
	if c.keepAliveTicker != nil {
		c.keepAliveTicker.Stop()
	}
	c.StopUserStream()
}

func (c *BinanceClient) SyncTimeOffset(withLogging bool) {
	offset, err := c.Client.NewSetServerTimeService().Do(c.ctx)
	if err != nil {
		c.Logger.WithError(err).Info("Could not sync to server time.")
		return
	}

	c.timeOffset = offset

	if withLogging {
		c.Logger.WithFields(log.Fields{"timeOffset": c.timeOffset}).Info("timeOffset set.")
	}
}

// NewOrder 는 새로운 주문을 요청하고 결과를 반환한다.
func (c *BinanceClient) NewOrder(o *model.Order, propertiesBySymbol map[string]*SymbolProperties) ([]*futures.CreateOrderResponse, error) {
	symbolProps := *propertiesBySymbol[o.Symbol]

	var price float64
	{
		// MinNotional 등을 적용하기 위해 Market 주문을 포함한 모든 주문에서 가격 정보가 필요하다.
		ticker, err := c.GetBookTicker(o.Symbol)
		if err != nil {
			return nil, err
		}
		if o.GetSide() == futures.SideTypeBuy {
			// 구매 주문인 경우 가격은 BestAskPrice
			price, err = strconv.ParseFloat(ticker.AskPrice, 64)
			if err != nil {
				return nil, err
			}
		} else if o.GetSide() == futures.SideTypeSell {
			// 판매 주문인 경우 가격은 BestBidPrice
			price, err = strconv.ParseFloat(ticker.BidPrice, 64)
			if err != nil {
				return nil, err
			}
		}
	}

	if o.Rate > 0.0 && o.Quantity == 0.0 {
		// Rate가 입력된 경우 RateBase에서 사용할 balance를 가져온다.
		balance, err := c.GetRateBaseBalance(o.Symbol, o.RateBase)
		if err != nil {
			return nil, err
		}
		c.Logger.WithField(o.RateBase, balance).Info("Using balance")

		switch o.GetType() {
		case futures.OrderTypeMarket:
		case futures.OrderTypeStop:
			fallthrough
		case futures.OrderTypeStopMarket:
			price = o.StopPrice
		default:
			price = o.Price
		}

		if price <= 0.0 {
			return nil, errors.New("invalid price")
		}

		o.Quantity = (balance * math.Min(o.Rate, 0.95)) / price
	}

	s := c.Client.NewCreateOrderService().Symbol(o.Symbol).Side(o.GetSide()).Type(o.GetType())
	s = s.ReduceOnly(o.ReduceOnly)

	if o.WorkingType != "" {
		s = s.WorkingType(futures.WorkingType(o.WorkingType))
	}

	if o.GetType() == futures.OrderTypeLimit {
		// Limit 주문은 TimeInForce가 필수로 필요하다. 현재는 항상 GTC로 고정.
		s = s.TimeInForce(futures.TimeInForceTypeGTC)
	}

	orderType := o.GetType()
	isMarketOrder := (orderType == futures.OrderTypeMarket ||
		orderType == futures.OrderTypeStopMarket ||
		orderType == futures.OrderTypeTakeProfitMarket ||
		orderType == futures.OrderTypeTrailingStopMarket)

	stepSize := symbolProps.StepSize
	priceTickSize := symbolProps.PriceTickSize
	qtyPrecision := int(math.Round(math.Abs(math.Log10(stepSize))))
	pricePrecision := int(math.Round(math.Abs(math.Log10(priceTickSize))))

	var minQty, maxQty float64
	if isMarketOrder {
		minQty = symbolProps.MarketMinQty
		maxQty = symbolProps.MarketMaxQty
	} else {
		minQty = symbolProps.MinQty
		maxQty = symbolProps.MaxQty
	}

	if !o.ReduceOnly {
		notionalMinQty := symbolProps.MinNotional / price
		for minQty < notionalMinQty {
			minQty += stepSize
		}
	}

	if o.Quantity != 0.0 {
		o.Quantity = math.Max(minQty, o.Quantity)
		s = s.Quantity(common.ToString(o.Quantity, qtyPrecision))
	}
	if o.Price != 0.0 && o.GetType() != futures.OrderTypeMarket {
		s = s.Price(common.ToString(o.Price, pricePrecision))
	}
	if o.StopPrice != 0.0 {
		s = s.StopPrice(common.ToString(o.StopPrice, pricePrecision))
	}
	if o.ActivationPrice != 0.0 {
		s = s.ActivationPrice(common.ToString(o.ActivationPrice, pricePrecision))
	}
	if o.CallbackRate != 0.0 {
		s = s.CallbackRate(common.ToString(o.CallbackRate, 2))
	}

	// res보다 웹소켓 결과가 먼저 도달하는 경우가 있어 WaitGroup을 사용해 순차적으로 실행한다.
	c.wg.Add(1)
	resChannel := make(chan []*futures.CreateOrderResponse, 1)
	errChannel := make(chan error, 1)
	go func() {
		defer c.wg.Done()

		var responses []*futures.CreateOrderResponse
		remainingQty := o.Quantity

		for 0 < remainingQty {
			if maxQty <= remainingQty {
				o.Quantity = maxQty
				remainingQty -= maxQty
			} else {
				o.Quantity = math.Max(minQty, remainingQty)
				remainingQty = 0
			}

			s = s.Quantity(common.ToString(o.Quantity, qtyPrecision))
			res, err := s.Do(c.ctx)

			// 에러 처리
			if err != nil {
				if errVal, ok := err.(*bcommon.APIError); ok {
					if errVal.Code == -2021 { // Already trigger
						if o.GetType() == futures.OrderTypeStopMarket {
							// StopMarket 주문에서 해당 에러가 발생했을 경우 Market 주문으로 다시 보낸다.
							c.Logger.WithField("order", o).WithField("timeOffset", c.timeOffset).Info(errVal)
							bakO := *o

							o.Type = string(futures.OrderTypeMarket)
							o.StopPrice = 0.0
							// NOTE: Quantity로 주문을 했을 경우 가격이 바뀌면서 잔고가 부족할 수도 있다.
							mRes, mErr := c.NewOrder(o, propertiesBySymbol)

							// 다시 보낸 Market 주문에서도 에러가 발생했을 경우 종료한다.
							if mErr != nil {
								c.SendSlackAlert(fmt.Sprintf("[%s][%s] %s", o.ExchangeAlias, o.StrategyName, mErr.Error()))
								resChannel <- mRes
								errChannel <- mErr
								return
							}

							for _, res := range mRes {
								responses = append(responses, res)
							}

							o = &bakO

							continue
						}
					} else if errVal.Code == -2022 { // ReduceOnly Order is rejected
						// 이전 주문에 의해 reduce only 주문 가능한 수량이 모두 찬 경우
						c.Logger.WithField("order", o).WithField("timeOffset", c.timeOffset).Info(errVal)
						c.SendSlackAlert(fmt.Sprintf("[%s][%s] %s", o.ExchangeAlias, o.StrategyName, err.Error()))

						resChannel <- nil
						errChannel <- err
						return
					}
				}

				c.Logger.WithError(err).WithField("order", o).WithField("timeOffset", c.timeOffset).Error()
				c.SendSlackAlert(fmt.Sprintf("[%s][%s] %s", o.ExchangeAlias, o.StrategyName, err.Error()))

				resChannel <- nil
				errChannel <- err
				return
			}

			// DB 기록 저장을 위한 값 채우기
			o.TimeInForce = string(futures.TimeInForceTypeGTC)
			o.Timestamp = res.UpdateTime

			// 결과 확인을 위해 Order 정보를 캐싱
			c.openOrders[res.OrderID] = o

			if err := o.Insert(); err != nil {
				c.Logger.WithError(err).WithField("order", o).Error("Failed to insert order.")
			}

			c.Logger.WithFields(log.Fields{"order": o, "res": responses, "timeOffset": c.timeOffset}).Info()
			responses = append(responses, res)
			newO := *o
			o = &newO
		}

		// OrderResult 로그 저장은 messageHandler에서 수행
		resChannel <- responses
		errChannel <- nil
	}()

	// NOTE: 위에서 둘 중 하나만 전송하는 경우 데드락이 발생한다.
	responses := <-resChannel
	err := <-errChannel

	return responses, err
}

func (c *BinanceClient) CancelOrder(o *model.Cancel) ([]*futures.CancelOrderResponse, error) {
	// res보다 웹소켓 결과가 먼저 도달하는 경우가 있어 WaitGroup을 사용해 순차적으로 실행한다.
	c.wg.Add(1)
	resChannel := make(chan []*futures.CancelOrderResponse, 1)
	errChannel := make(chan error, 1)
	go func() {
		defer c.wg.Done()

		var responses []*futures.CancelOrderResponse

		if o.OrderID > 0 {
			// OrderID가 있으면 해당 주문을 취소한다.
			res, err := c.Client.NewCancelOrderService().Symbol(o.Symbol).OrderID(o.OrderID).Do(c.ctx)
			if err != nil {
				c.Logger.WithError(err).WithFields(log.Fields{"req": o, "res": res, "timeOffset": c.timeOffset}).Error()

				resChannel <- nil
				errChannel <- err
				return
			}
			responses = append(responses, res)
			c.Logger.WithFields(log.Fields{"req": o, "res": res, "timeOffset": c.timeOffset}).Info()

			c.Logger.WithField("res", res).Info()
		} else {
			// OrderID가 없으면 전체 주문을 취소한다.
			res, err := c.CancelAllOrders(o.Symbol)
			if err != nil {
				c.Logger.WithError(err).Error()

				resChannel <- nil
				errChannel <- err
				return
			}
			for _, response := range res {
				responses = append(responses, response)
			}
		}

		resChannel <- responses
		errChannel <- nil
	}()

	// NOTE: 위에서 둘 중 하나만 전송하는 경우 데드락이 발생한다.
	responses := <-resChannel
	err := <-errChannel

	return responses, err
}

// GetPosition 은 특정 symbol의 포지션을 반환한다.
func (c *BinanceClient) GetPosition(symbol string) *Position {
	return c.positions[symbol]
}

// GetPositions 는 현재 계정의 포지션을 배열로 모두 반환한다.
func (c *BinanceClient) GetPositions() []*Position {
	var positions []*Position

	for _, val := range c.positions {
		positions = append(positions, val)
	}

	return positions
}

func (c *BinanceClient) ListenKey() (string, error) {
	res, err := c.Client.NewStartUserStreamService().Do(c.ctx)
	if err != nil {
		c.Logger.WithError(err).Error("ListenKey failed.")
		return "", err
	}
	return res, nil
}

func (c *BinanceClient) KeepAlive() error {
	err := c.Client.NewKeepaliveUserStreamService().Do(c.ctx)
	if err != nil {
		c.Logger.WithError(err).Error("KeepAlive failed.")
		restartErr := c.RestartUserStream()
		if restartErr != nil {
			c.SendSlackAlert(restartErr.Error())
		}
	}
	return nil
}

// TODO sungmkim - update codes for WsUserDataServe function
func (c *BinanceClient) StartUserStream() error {
	//listenKey, err := c.ListenKey()
	//if err != nil {
	//	return err
	//}

	// ListenKey가 expire하는 것을 막기 위해 1분 마다 keepAlive를 전송한다.
	c.keepAliveTicker = time.NewTicker(time.Minute * 1)
	go func() {
		for range c.keepAliveTicker.C {
			c.KeepAlive()
		}
	}()

	//errHandler := func(err error) {
	//	c.Logger.WithError(err).Error("Stream error.")
	//	// 계속 실패하면 무한 루프처럼 돌 수도..
	//	c.RestartUserStream()
	//}

	go func() {
		// NOTE: KeepAlive 옵션을 켜도 웹소켓이 1시간 후 만료된다. 핑퐁밖에는 안 해주는 것 같아보임.
		binance.WebsocketKeepalive = true
		futures.WebsocketKeepalive = true
		//doneC, stopC, err := binance.WsUserDataServe(listenKey, c.ctx, c.messageHandler, errHandler)(listenKey, c.messageHandler, errHandler)
		//if err != nil {
		//	c.Logger.WithError(err).Error("Failed to open user data ws.")
		//	return
		//}
		//c.UserDataStopC = stopC
		//<-doneC
		//c.Logger.Info("User data stream closed.")
	}()

	return nil
}

func (c *BinanceClient) RestartUserStream() error {
	c.keepAliveTicker.Stop()
	c.StopUserStream()
	return c.StartUserStream()
}

func (c *BinanceClient) StopUserStream() {
	c.UserDataStopC <- struct{}{}
}

func (c *BinanceClient) GetBookTicker(symbol string) (ticker *futures.BookTicker, err error) {
	list, err := c.Client.NewListBookTickersService().Symbol(symbol).Do(c.ctx)
	if err != nil {
		return
	}
	if len(list) < 1 {
		err = errors.New("no book ticker returned")
		return
	}

	ticker = list[0]
	c.Logger.WithField("ticker", ticker).Info()
	return
}

// GetRateBaseBalance 는 base 타입에 맞게 rate 계산에 사용할 밸런스를 반환한다.
func (c *BinanceClient) GetRateBaseBalance(symbol string, base string) (float64, error) {
	switch base {
	case "balance":
		balance, err := c.GetBalance(symbol)
		if err != nil {
			return 0.0, err
		}
		return strconv.ParseFloat(balance.Balance, 64)
	case "available_balance":
		balance, err := c.GetBalance(symbol)
		if err != nil {
			return 0.0, err
		}
		return strconv.ParseFloat(balance.AvailableBalance, 64)
	case "margin_balance":
		// NOTE: Account에서 totalMarginBalance를 가져와서 사용함. weight가 5로 약간 무겁다.
		account, err := c.GetAccount()
		if err != nil {
			return 0.0, err
		}
		return strconv.ParseFloat(account.TotalMarginBalance, 64)
	}
	return 0.0, errors.New("invalid base type")
}

// GetBalance 는 해당 symbol에 맞는 balance 정보를 반환한다.
func (c *BinanceClient) GetBalance(symbol string) (*futures.Balance, error) {
	// NOTE: 현재는 항상 API를 통해 계정의 밸런스를 가져온다.
	balance, err := c.AccountBalance()
	if err != nil {
		return nil, err
	}

	for _, b := range balance {
		assetLen := len(b.Asset)
		if symbol[len(symbol)-assetLen:] == b.Asset {
			return b, nil
		}
	}

	return nil, errors.New("Balance not found")
}

func (c *BinanceClient) AccountBalance() ([]*futures.Balance, error) {
	res, err := c.Client.NewGetBalanceService().Do(c.ctx)
	if err != nil {
		return nil, err
	}

	c.Balance = map[string]*futures.Balance{}
	for _, b := range res {
		c.Balance[b.Asset] = b
	}

	return res, nil
}

func (c *BinanceClient) GetAccount() (res *futures.Account, err error) {
	const MAX_RETRY = 3

	for i := 0; i < MAX_RETRY; i++ {
		res, err = c.Client.NewGetAccountService().Do(c.ctx)
		if err == nil {
			return // success
		}

		apiError, ok := err.(*bcommon.APIError)
		if !ok {
			return // unhandled error
		}

		switch apiError.Code {
		case -1003: // Too many request
			// API limit을 초과하면 재시도
			// 단 limit이 분당 2400, Account API의 weight이 5이므로 실제로 limit을 넘기는 어렵다
			wait := time.Duration((i + 1) * 10)
			c.Logger.WithError(apiError).Infof("Too may request. Sleep for %d sec", wait)
			time.Sleep(wait * time.Second)
			continue

		default:
			return // Unhandled errors
		}
	}

	return // exceeded max retry
}

func (c *BinanceClient) Publish(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		c.Logger.WithError(err).Error("Failed to marshal.")
		return err
	}
	err = c.Rds.Publish(c.ctx, c.Info.ExchangeAlias, b).Err()
	if err != nil {
		c.Logger.WithError(err).Error("Failed to publish.")
		return err
	}
	return nil
}

func (c *BinanceClient) OpenOrders(symbol string) ([]*futures.Order, error) {
	openOrders, err := c.Client.NewListOpenOrdersService().Symbol("BTCUSDT").Do(c.ctx)
	if err != nil {
		c.Logger.WithError(err).WithField("timeOffset", c.timeOffset).Error()
		return nil, err
	}

	c.Logger.WithFields(log.Fields{"symbol": symbol, "timeOffset": c.timeOffset}).Info()
	for _, order := range openOrders {
		c.Logger.WithField("order", order).Info()
	}

	return openOrders, err
}

func (c *BinanceClient) CancelAllOrders(symbol string) ([]*futures.CancelOrderResponse, error) {
	openOrders, err := c.OpenOrders(symbol)
	if err != nil {
		return nil, err
	}

	var responses []*futures.CancelOrderResponse

	for _, order := range openOrders {
		req := new(model.Cancel)
		req.Symbol = order.Symbol
		req.OrderID = order.OrderID

		res, err := c.CancelOrder(req)
		if err != nil {
			c.Logger.WithError(err).Error()
		}
		for _, response := range res {
			responses = append(responses, response)
		}
	}

	return responses, nil
}

func (c *BinanceClient) SetLeverage(symbol string, leverage int) (*futures.SymbolLeverage, error) {
	s := c.Client.NewChangeLeverageService()
	res, err := s.Symbol(symbol).Leverage(leverage).Do(c.ctx)

	if err != nil {
		c.Logger.WithError(err).WithField("timeOffset", c.timeOffset).Error()
		return nil, err
	}

	c.Logger.WithFields(log.Fields{"timeOffset": c.timeOffset}).Info()
	return res, nil
}

func (c *BinanceClient) messageHandler(message []byte) {
	// 주문 전송이 진행중인 경우 끝날때까지 기다린다.
	c.wg.Wait()

	eventTypeChecker := new(EventTypeChecker)
	if err := json.Unmarshal(message, eventTypeChecker); err != nil {
		c.Logger.WithError(err).WithField("msg", string(message)).Error("Failed to check event type.")
		return
	}

	switch eventTypeChecker.EventType {
	case "ORDER_TRADE_UPDATE":
		orderUpdate := new(OrderUpdate)
		if err := json.Unmarshal(message, orderUpdate); err != nil {
			c.Logger.WithError(err).WithField("msg", string(message)).Error("Failed to unmarshal order update.")
			return
		}
		c.Logger.WithField("result", orderUpdate.Result).Info()
		if err := c.handleOrderTradeUpdate(orderUpdate); err != nil {
			c.Logger.WithField("result", orderUpdate.Result).WithError(err).Error("Failed to handle trade update")
			return
		}
	case "ACCOUNT_UPDATE":
		accountUpdate := new(AccountUpdate)
		if err := json.Unmarshal(message, accountUpdate); err != nil {
			c.Logger.WithError(err).WithField("msg", string(message)).Error("Failed to unmarshal account update.")
			return
		}
		c.Logger.WithField("account", accountUpdate.Account).Info()
		if err := c.handleAccountUpdate(accountUpdate); err != nil {
			c.Logger.WithField("accountUpdate", accountUpdate).WithError(err).Error("Failed to handle account upate")
			return
		}
	case "listenKeyExpired":
		if err := c.RestartUserStream(); err != nil {
			c.Logger.WithError(err).Error("Failed to restart userstream")
			return
		}
	default:
		c.Logger.WithField("msg", string(message)).Error("Message not handled.")
	}
}

func (c *BinanceClient) handleOrderTradeUpdate(o *OrderUpdate) error {
	orderID := o.Result.OrderID
	openOrder, exist := c.openOrders[orderID]
	if !exist {
		return errors.New("invalid OrderID")
	}

	// Slack 노티용 정보를 기록한다.
	m := o.CreateModel(openOrder.ID)
	m.StrategyName = openOrder.StrategyName
	m.ExchangeAlias = openOrder.ExchangeAlias
	m.Author = c.Author
	m.Type = openOrder.Type

	var err error
	switch o.Result.OrderStatus {
	case "NEW":
		// 신규 주문 결과 생성
		err = m.Insert()
		if err == nil {
			m.SendSlack(c.Logger)
		} else if strings.Contains(err.Error(), "duplicate key") {
			// STOP_MARKET 주문의 경우 동일한 OrderID로 MARKET 주문이 새로 생성되기 때문에 Insert 할 수 없다.
			err = nil
		}
	case "PARTIALLY_FILLED":
		// 주문 결과 업데이트
		// 주문 서버 재시작으로 openOrder를 새로 받아오는 경우엔 총합이 정상적으로 누적되지 않는다.
		openOrder.RealizedProfit += m.RealizedProfit
		m.RealizedProfit = openOrder.RealizedProfit
		err = m.Update()
	case "FILLED":
		openOrder.RealizedProfit += m.RealizedProfit
		m.RealizedProfit = openOrder.RealizedProfit
		m.SendSlack(c.Logger)
		if err := openOrder.WriteSlippage(m); err != nil {
			c.Logger.WithField("openOrder", openOrder).WithError(err).Error("Failed to write slippage")
		}
		fallthrough
	case "CANCELED":
		// 주문 결과 업데이트 및 완료 처리
		err = m.Update()
		if err != nil {
			break
		}
		delete(c.openOrders, orderID)
		err = c.Publish(m)
		if err != nil {
			break
		}
	case "EXPIRED":
		// STOP_MARKET 주문이 체결될 때 기존 주문이 EXPIRED 되고 MARKET 주문이 같은 OrderID로 새로 생성된다.
		err = nil
	case "CALCULATED":
		fallthrough
	case "TRADE":
		fallthrough
	default:
		err = errors.New("unexpected OrderStatus")
	}

	if err != nil {
		return err
	}

	return nil
}

func (c *BinanceClient) handleAccountUpdate(a *AccountUpdate) error {
	// Balance 업데이트
	// NOTE(vince): 실제로는 WalletBalance 변동 시에만 업데이트 되는 것으로 보인다. (주문 / 체결 / 펀딩피 발생 등)
	//   그러므로 이 data에 의존하지 말고 요청 시 REST API 호출하여 현재 값을 조회해 리턴하자
	for _, b := range a.Account.Balances {
		if _, ok := c.Balance[b.Asset]; ok {
			c.Balance[b.Asset].Balance = b.WalletBalance
			c.Balance[b.Asset].CrossWalletBalance = b.CrossWalletBalance
		} else {
			c.Balance[b.Asset] = &futures.Balance{
				Asset:              b.Asset,
				Balance:            b.WalletBalance,
				CrossWalletBalance: b.CrossWalletBalance,
			}
		}
	}

	// 포지션 업데이트
	for _, p := range a.Account.Positions {
		if p.PositionSide != "BOTH" {
			c.Logger.WithField("position", p).Error("Unexpected position update. Only BOTH is considered.")
			continue
		}
		c.positions[p.Symbol] = p
		c.Logger.WithField("position", p).Info("Position updated.")
	}

	return nil
}

// TODO sungmkim - update codes for NewPremiumIndexService function
func (c *BinanceClient) GetFundingRate(symbol string) (*futures.PremiumIndex, error) {
	//res, err := c.Client.NewPremiumIndexService().Symbol(symbol).Do(context.Background())
	//if err != nil {
	//	return nil, err
	//}
	//
	return nil, nil
}

// CloseAll은 해당 클라이언트의 모든 주문을 취소하고 포지션을 종료한다.
func (c *BinanceClient) CloseAll(propertiesBySymbol map[string]*SymbolProperties) error {
	c.CancelAllOrders("")

	for symbol, pos := range c.positions {
		oppositeSide := ""
		if pos.PositionAmount > 0 {
			oppositeSide = string(futures.SideTypeSell)
		} else {
			oppositeSide = string(futures.SideTypeBuy)
		}

		c.NewOrder(&model.Order{
			Symbol:     symbol,
			Quantity:   math.Abs(pos.PositionAmount),
			Type:       string(futures.OrderTypeMarket),
			Side:       oppositeSide,
			ReduceOnly: true,
		}, propertiesBySymbol)
	}

	// 해당하는 클라이언트가 바로 종료되도록 메시지를 보낸다.
	c.Publish(2)
	return nil
}

func (c *BinanceClient) loadOpenOrders() error {
	c.openOrders = make(map[int64]*model.Order)

	apiOpenOrders, err := c.OpenOrders("")
	if err != nil {
		return err

	}
	mapOpenOrders := make(map[int64]*futures.Order)
	for _, order := range apiOpenOrders {
		mapOpenOrders[order.OrderID] = order
	}

	dbOpenOrders, err := model.GetOpenOrderResults(c.Info.ExchangeAlias)
	if err != nil {
		return err
	}

	for _, order := range dbOpenOrders {
		if _, exist := mapOpenOrders[order.OrderID]; !exist {
			// DB에 존재하나 API의 OpenOrder에는 없는 상황.
			// 바이낸스에 GetOrder 호출을 날려 없으면 캔슬, 있으면 체결으로 판단한다.
			// TODO: OrderResult의 내용 업데이트에 대한 고민 필요.
			res, err := c.queryOrder(order.Symbol, order.OrderID)
			if err != nil {
				// 캔슬된 오더
				order.OrderStatus = "CANCELED"
			} else {
				// 체결된 오더
				order.OrderStatus = string(res.Status)
			}
			if err := order.Update(); err != nil {
				c.Logger.WithError(err).WithField("order", order).Error("Failed to update order.")
			}
		}

	}

	// 갱신된 결과로 최종 열려있는 오픈 오더 정보 저장
	openOrders, err := model.GetOpenOrders(c.Info.ExchangeAlias)
	if err != nil {
		c.Logger.WithError(err).Error("Failed to get open orders.")
		return err
	}
	for _, o := range openOrders {
		c.openOrders[o.OrderID] = o
	}

	return nil
}

func (c *BinanceClient) queryOrder(symbol string, orderID int64) (res *futures.Order, err error) {
	res, err = c.Client.NewGetOrderService().Symbol(symbol).OrderID(orderID).Do(c.ctx)
	if err != nil {
		c.Logger.WithError(err).WithFields(log.Fields{"symbol": symbol, "orderID": orderID, "timeOffset": c.timeOffset}).Error()
		return
	}

	c.Logger.WithFields(log.Fields{"symbol": symbol, "orderID": orderID, "timeOffset": c.timeOffset}).Info()
	return
}

func (c *BinanceClient) loadPositions() error {
	c.positions = make(map[string]*Position)

	res, err := c.Client.NewGetPositionRiskService().Do(c.ctx)
	if err != nil {
		return err
	}

	for _, p := range res {
		if p.PositionSide != "BOTH" {
			c.Logger.WithField("positionRisk", p).Error("Unexpected position update. Only BOTH is considered.")
			continue
		}

		pos := new(Position)
		pos.Symbol = p.Symbol
		if val, err := strconv.ParseFloat(p.PositionAmt, 64); err == nil {
			pos.PositionAmount = val
		}
		if val, err := strconv.ParseFloat(p.EntryPrice, 64); err == nil {
			pos.EntryPrice = val
		}
		pos.AccumRealized = 0.0 // 정보 없음
		if val, err := strconv.ParseFloat(p.UnRealizedProfit, 64); err == nil {
			pos.UnrealizedPnL = val
		}
		pos.MarginType = p.MarginType
		// TODO: 같은 정보가 맞는지 확인 필요.
		if val, err := strconv.ParseFloat(p.IsolatedMargin, 64); err == nil {
			pos.IsolatedWallet = val
		}
		pos.PositionSide = p.PositionSide

		if pos.PositionAmount == 0.0 {
			continue
		}

		c.positions[pos.Symbol] = pos
		c.Logger.WithField("position", pos).Info("Position loaded.")
	}

	return nil
}

func (c *BinanceClient) SendSlackAlert(message string) error {
	isLive := config.Get().Server.Env == "live"
	author := strings.TrimSpace(c.Author)

	// 공용 채널에 메시지 전송
	{
		channel := config.GetSlackChannel()
		req := model.SlackRequest{
			Channel: channel,
			Text:    message,
		}

		pb, err := json.Marshal(req)
		if err != nil {
			c.Logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to marshal.")
			return err
		}
		buff := bytes.NewBuffer(pb)

		res, err := http.Post(config.Get().Slack.Url, "application/json", buff)
		if err != nil {
			c.Logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to send.")
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			c.Logger.WithFields(log.Fields{"channel": channel, "req": req}).Error("slack api error.")
			return err
		}
	}

	// author별 채널에 메시지 전송
	if isLive && author != "" {
		channel := config.GetSlackChannel() + "-" + author
		req := model.SlackRequest{
			Channel: channel,
			Text:    message,
		}

		pb, err := json.Marshal(req)
		if err != nil {
			c.Logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to marshal.")
			return err
		}
		buff := bytes.NewBuffer(pb)

		res, err := http.Post(config.Get().Slack.Url, "application/json", buff)
		if err != nil {
			c.Logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to send.")
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			c.Logger.WithFields(log.Fields{"channel": channel, "req": req}).Error("slack api error.")
			return err
		}
	}

	return nil
}

func (o *OrderUpdate) CreateModel(ID int64) *model.OrderResult {
	m := new(model.OrderResult)

	r := o.Result
	m.ExchangeOrderID = ID
	m.ClientOrderID = r.ClientOrderID
	m.OrderID = r.OrderID
	m.Symbol = r.Symbol
	m.Side = r.Side
	m.Type = r.Type
	m.OrderStatus = r.OrderStatus
	m.TimeInForce = r.TimeInForce
	m.OriginalQuantity = r.OriginalQuantity
	m.OriginalPrice = r.OriginalPrice
	m.AveragePrice = r.AveragePrice
	m.StopPrice = r.StopPrice
	m.ExecutionType = r.ExecutionType
	m.OrderLastFilledQuantity = r.OrderLastFilledQuantity
	m.OrderFilledAccumQuantity = r.OrderFilledAccumQuantity
	m.LastFilledPrice = r.LastFilledPrice
	m.CommissionAsset = r.CommissionAsset
	m.Commission = r.Commission
	m.OrderTradeTime = r.OrderTradeTime
	m.TraceID = r.TraceID
	m.BidsNotional = r.BidsNotional
	m.AskNotional = r.AskNotional
	m.IsMakerSide = r.IsMakerSide
	m.IsReduceOnly = r.IsReduceOnly
	m.StopPriceWorkingType = r.StopPriceWorkingType
	m.OriginalOrderType = r.OriginalOrderType
	m.PositionSide = r.PositionSide
	m.IsCloseConditional = r.IsCloseConditional
	m.ActivationPrice = r.ActivationPrice
	m.CallbackRate = r.CallbackRate
	m.RealizedProfit = r.RealizedProfit

	return m
}
