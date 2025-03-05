package api

import (
	"net/http"
	"strings"

	"arques.com/order/exchange"
	"arques.com/order/model"
	"github.com/labstack/echo"
)

type (
	ReqCommon struct {
		StrategyName  string `json:"strategy_name"`
		ExchangeAlias string `json:"exchange_alias"`
		Symbol        string `json:"symbol"`
	}

	ReqOrder struct {
		ReqCommon
		Side            string  `json:"side"`
		Type            string  `json:"type"`
		Rate            float64 `json:"rate"`
		RateBase        string  `json:"rate_base"`
		Quantity        float64 `json:"quantity"`
		Price           float64 `json:"price"`
		StopPrice       float64 `json:"stop_price"`
		ReduceOnly      bool    `json:"reduce_only"`
		WorkingType     string  `json:"working_type"`
		ActivationPrice float64 `json:"activation_price"`
		CallbackRate    float64 `json:"callback_rate"`
	}

	ReqCancel struct {
		ReqCommon
		OrderID int64 `json:"order_id"`
	}

	ReqLeverage struct {
		ReqCommon
		Leverage int `json:"leverage"`
	}

	ReqExchangeAlias struct {
		ExchangeAlias string `json:"exchange_alias"`
	}

	ReqAuthor struct {
		ExchangeAlias string `json:"exchange_alias"`
	}

	ResErr struct {
		Error string `json:"error"`
	}
)

var (
	ex *exchange.Binance
)

func init() {
	ex = exchange.New()
}

func (o *ReqOrder) CreateModel() *model.Order {
	m := new(model.Order)

	m.StrategyName = o.StrategyName
	m.ExchangeAlias = o.ExchangeAlias
	m.Symbol = strings.ToUpper(o.Symbol)
	m.Side = o.Side
	m.Type = o.Type
	m.Rate = o.Rate
	m.RateBase = o.RateBase
	m.Quantity = o.Quantity
	m.Price = o.Price
	m.StopPrice = o.StopPrice
	m.ReduceOnly = o.ReduceOnly
	m.WorkingType = o.WorkingType
	m.ActivationPrice = o.ActivationPrice
	m.CallbackRate = o.CallbackRate

	return m
}

func (o *ReqCancel) CreateModel() *model.Cancel {
	m := new(model.Cancel)

	m.Symbol = strings.ToUpper(o.Symbol)
	m.OrderID = o.OrderID

	return m
}

func NewOrder(c echo.Context) error {
	reqOrder := new(ReqOrder)
	if err := c.Bind(reqOrder); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	order := reqOrder.CreateModel()

	if err := order.CheckValid(); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(order.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	res, err := client.NewOrder(order)
	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}
	return c.JSON(http.StatusOK, res)
}

func CancelOrder(c echo.Context) error {
	reqCancel := new(ReqCancel)
	if err := c.Bind(reqCancel); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	cancel := reqCancel.CreateModel()

	if err := cancel.CheckValid(); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(reqCancel.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	res, err := client.CancelOrder(cancel)

	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	if len(res) == 1 {
		return c.JSON(http.StatusOK, res[0])
	} else {
		return c.JSON(http.StatusOK, res)
	}
}

func GetPositions(c echo.Context) error {
	reqPosition := new(ReqCommon)
	if err := c.Bind(reqPosition); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(reqPosition.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	positions := client.GetPositions()

	return c.JSON(http.StatusOK, positions)
}

func GetOpenOrders(c echo.Context) error {
	req := new(ReqCommon)
	if err := c.Bind(req); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(req.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	openOrders, err := client.OpenOrders("")
	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	return c.JSON(http.StatusOK, openOrders)
}

func PostLeverage(c echo.Context) error {
	req := new(ReqLeverage)
	if err := c.Bind(req); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(req.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	res, err := client.SetLeverage(req.Symbol, req.Leverage)
	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	return c.JSON(http.StatusOK, res)
}

func PostRequestLoadSymbolProperties(c echo.Context) error {
	ex.LoadSymbolProperties()
	return c.JSON(http.StatusOK, "")
}

func GetFundingRate(c echo.Context) error {
	req := new(ReqCommon)
	if err := c.Bind(req); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(req.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	res, err := client.GetFundingRate(req.Symbol)
	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	return c.JSON(http.StatusOK, res)
}

func GetAccount(c echo.Context) error {
	req := new(ReqCommon)
	if err := c.Bind(req); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(req.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	res, err := client.GetAccount()
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, ResErr{Error: err.Error()}) // unhandled error
	}

	// NOTE(vince): 빈 asset, position을 전송하지 않으려면 아래 코드를 추가로 실행할 수 있다.
	/*
		{
			temp := []*futures.AccountAsset{}
			for _, a := range res.Assets {
				w, _ := strconv.ParseFloat(a.WalletBalance, 64)
				m, _ := strconv.ParseFloat(a.MarginBalance, 64)
				if w != 0 || m != 0 {
					temp = append(temp, a)
				}
			}
			res.Assets = temp
		}

		{
			temp := []*futures.AccountPosition{}
			for _, p := range res.Positions {
				a, _ := strconv.ParseFloat(p.PositionAmt, 64)
				if a != 0 {
					temp = append(temp, p)
				}
			}
			res.Positions = temp
		}
	*/

	return c.JSON(http.StatusOK, res)
}

// ExchangeAlias 는 해당 alias의 key를 새로 읽어 갱신한다.
func ExchangeAlias(c echo.Context) error {
	reqEx := new(ReqExchangeAlias)
	if err := c.Bind(reqEx); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	if err := ex.UpdateClient(reqEx.ExchangeAlias); err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	return c.JSON(http.StatusOK, "")
}

func UpdateAuthor(c echo.Context) error {
	reqAuthor := new(ReqAuthor)
	if err := c.Bind(reqAuthor); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	if err := ex.UpdateAuthor(reqAuthor.ExchangeAlias); err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	return c.JSON(http.StatusOK, "")
}

func GetTimeOffsets(c echo.Context) error {
	offsets := ex.GetTimeOffsets()
	return c.JSON(http.StatusOK, offsets)
}

func CloseAll(c echo.Context) error {
	reqEx := new(ReqExchangeAlias)
	if err := c.Bind(reqEx); err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	client := ex.Client(reqEx.ExchangeAlias)
	if client == nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: "invalid exchange alias"})
	}

	err := client.CloseAll()
	if err != nil {
		return c.JSON(http.StatusBadRequest, ResErr{Error: err.Error()})
	}

	return c.JSON(http.StatusOK, "")
}
