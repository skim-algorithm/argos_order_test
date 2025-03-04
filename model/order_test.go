package model_test

import (
	"fmt"
	"testing"

	"arques.com/order/common"
	"arques.com/order/model"
	"github.com/stretchr/testify/assert"
)

func TestOrderInsert(t *testing.T) {
	order := new(model.Order)
	order.StrategyName = "TEST"
	order.ExchangeAlias = "Test_alias"
	order.Symbol = "BTCUSDT"
	order.Side = "BUY"
	order.Type = "STOP_MARKET"
	order.Quantity = 0.5
	order.StopPrice = 1000.0
	order.IptTime = common.Now()
	order.TimeInForce = "GTC"

	err := order.Insert()
	assert.NoError(t, err)

	res := new(model.Order)
	res.ID = order.ID
	res.Load()

	assert.Equal(t, order, res)
}

func TestResultInsert(t *testing.T) {
	order := new(model.Order)
	order.StrategyName = "TEST"
	order.ExchangeAlias = "Test_alias"
	order.Symbol = "BTCUSDT"
	order.Side = "BUY"
	order.Type = "STOP_MARKET"
	order.Quantity = 0.5
	order.StopPrice = 1000.0
	order.IptTime = common.Now()
	order.TimeInForce = "GTC"

	err := order.Insert()
	assert.NoError(t, err)

	tmpOrderID := common.Now()

	result := new(model.OrderResult)
	result.ExchangeOrderID = order.ID
	result.ClientOrderID = "test"
	result.OrderID = tmpOrderID
	result.Symbol = "BTCUSDT"
	result.Side = "BUY"
	result.Type = "STOP_MARKET"
	result.OrderStatus = "NEW"
	result.TimeInForce = "GCT"
	result.IptTime = common.Now()

	err = result.Insert()
	assert.NoError(t, err)

	res := new(model.OrderResult)
	res.OrderID = tmpOrderID
	err = res.Load()
	assert.NoError(t, err)

	assert.Equal(t, result, res)

	resLog := new(model.OrderResult)
	resLog.ID = result.ID
	resLog.LoadLog()

	assert.Equal(t, result, resLog)

	result.OrderStatus = "CANCELED"
	err = result.Update()
	assert.NoError(t, err)

	res.Load()
	assert.Equal(t, "CANCELED", res.OrderStatus)
	assert.Equal(t, result, res)
}

func TestOpenOrders(t *testing.T) {
	orders, err := model.GetOpenOrders("Arquesbinance01_test01")
	assert.NoError(t, err)
	for _, o := range orders {
		fmt.Printf("%+v\n", o)
	}
}

func TestWriteSlippage(t *testing.T) {
	now := common.Now()

	tmpOrder := model.Order{
		StrategyName:  "test_strategy",
		ExchangeAlias: "test_alias",
		Symbol:        "BTCUSDT",
		Type:          "MARKET",
		Timestamp:     now - 100,
		Price:         7000.10,
	}
	tmpResult := model.OrderResult{
		OrderID:                  1111,
		LastFilledPrice:          7000.90,
		OrderFilledAccumQuantity: 0.43,
		OrderTradeTime:           now + 100,
	}

	err := tmpOrder.WriteSlippage(&tmpResult)
	assert.NoError(t, err)
}
