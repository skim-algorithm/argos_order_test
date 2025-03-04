package exchange_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"arques.com/order/config"
	"arques.com/order/exchange"
	"arques.com/order/model"
)

func TestNewOrder(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	req := new(model.Order)
	req.StrategyName = "TEST"
	req.ExchangeAlias = c.Info.ExchangeAlias
	req.Symbol = "BTCUSDT"
	req.Side = "BUY"
	req.Type = "STOP_MARKET"
	req.Quantity = 0.5
	req.StopPrice = 1000.0

	_, err := c.NewOrder(req, ex.PropertiesBySymbol)
	assert.Error(t, err)

	if errVal, ok := err.(*common.APIError); ok {
		if errVal.Code != -2021 {
			// 에러 코드 -2021은 already trigger라는 뜻
			assert.NoError(t, err)
		}
	}

	req.Quantity = 0.0
	req.Rate = 1.0

	_, err = c.NewOrder(req, ex.PropertiesBySymbol)
	assert.Error(t, err)

	if errVal, ok := err.(*common.APIError); ok {
		if errVal.Code != -2021 {
			// 에러 코드 -2021은 already trigger라는 뜻
			assert.NoError(t, err)
		}
	}
}

func TestListenKey(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)
	key, err := c.ListenKey()
	assert.NoError(t, err)
	assert.NotEqual(t, "", key)
}

func TestUserStream(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	time.Sleep(time.Second)

	req := new(model.Order)
	req.StrategyName = "test_strategy"
	req.ExchangeAlias = "ArquesDev01_API"
	req.Symbol = "BTCUSDT"
	req.Side = "BUY"
	req.Type = "STOP_MARKET"
	req.Quantity = 0.001
	req.StopPrice = 100000.0

	_, err := c.NewOrder(req, ex.PropertiesBySymbol)
	assert.NoError(t, err)

	_, err = c.CancelAllOrders("BTCUSDT")
	assert.NoError(t, err)

	done := make(chan struct{}, 0)

	go func() {
		defer close(done)
		time.Sleep(4 * time.Second)
		err := c.RestartUserStream()
		assert.NoError(t, err)
		time.Sleep(1 * time.Second)
		c.StopUserStream()
		time.Sleep(1 * time.Second)
	}()

	<-done
}

func TestGetBookTicker(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	ticker, err := c.GetBookTicker("BTCUSDT")
	assert.NoError(t, err)

	fmt.Printf("%+v\n", ticker)
}

func TestGetRateBaseBalance(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	b, err := c.GetRateBaseBalance("BTCUSDT", "")
	assert.Error(t, err)

	b, err = c.GetRateBaseBalance("BTCUSDT", "balance")
	assert.NoError(t, err)
	fmt.Println("balance:", b)

	b, err = c.GetRateBaseBalance("BTCUSDT", "available_balance")
	assert.NoError(t, err)
	fmt.Println("available_balance:", b)

	b, err = c.GetRateBaseBalance("BTCUSDT", "margin_balance")
	assert.NoError(t, err)
	fmt.Println("margin_balance:", b)
}

func TestAccountBalance(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	_, err := c.AccountBalance()
	assert.NoError(t, err)
}

func TestAccount(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	_, err := c.GetAccount()
	assert.NoError(t, err)
}

func TestCancelOrder(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	req := new(model.Cancel)
	req.Symbol = "BTCUSDT"
	req.OrderID = 1234

	_, err := c.CancelOrder(req)
	assert.Error(t, err)

	if errVal, ok := err.(*common.APIError); ok {
		if errVal.Code == -2011 {
			// 에러 코드 -2011은 해당 주문이 없다는 에러로 정상이다.
			return
		}
	}
	assert.NoError(t, err)
}

func TestPublish(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	rds := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Get().Redis.Url, config.Get().Redis.Port),
		Password: "",
		DB:       0,
	})

	done := make(chan struct{}, 0)

	go func() {
		defer close(done)
		pubsub := rds.Subscribe(context.Background(), c.Info.ExchangeAlias)
		ch := pubsub.Channel()

		for msg := range ch {
			fmt.Println(msg.Channel, msg.Payload)
			pubsub.Close()
		}
	}()

	time.AfterFunc(time.Second, func() {
		err := c.Publish(new(model.OrderResult))
		assert.NoError(t, err)
	})

	<-done
}

func TestOpenOrders(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	_, err := c.OpenOrders("")
	assert.NoError(t, err)
}

func TestCancelAllOrders(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	_, err := c.CancelAllOrders("BTCUSDT")
	assert.NoError(t, err)
}

func TestCloseAllOrders(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	err := c.CloseAll(ex.PropertiesBySymbol)
	assert.NoError(t, err)
}

func TestLoadSymbolProperties(t *testing.T) {
	ex := exchange.New()
	err := ex.LoadSymbolProperties()
	assert.Error(t, err)
}

func TestSendSlack(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	result := model.OrderResult{
		StrategyName:             "test_strategy_001",
		ExchangeAlias:            "ArquesDev01_API",
		OrderID:                  1234567890,
		Symbol:                   "BTCUSDT",
		Side:                     "SELL",
		Type:                     "STOP_MARKET",
		OrderStatus:              "FILLED",
		OriginalQuantity:         1234.5,
		AveragePrice:             11000.0,
		StopPrice:                10000.0,
		OrderFilledAccumQuantity: 1234.5,
		IsReduceOnly:             true,
		StopPriceWorkingType:     "MARK_PRICE",
		RealizedProfit:           1400.23,
	}

	err := result.SendSlack(c.Logger)
	assert.NoError(t, err)
}

func TestSlackAlert(t *testing.T) {
	ex := exchange.New()
	c := ex.Client("ArquesDev01_API")
	assert.NotNil(t, c)

	err := errors.New("testing slack alert")
	resErr := c.SendSlackAlert(err.Error())
	assert.NoError(t, resErr)
}
