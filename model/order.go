package model

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"

	"arques.com/order/common"
	"arques.com/order/config"
)

type (
	Order struct {
		ID               int64
		StrategyName     string
		ExchangeAlias    string
		Symbol           string
		Side             string
		Type             string
		Rate             float64
		Quantity         float64
		Price            float64
		StopPrice        float64
		PositionSide     string
		ReduceOnly       bool
		TimeInForce      string
		ClosePosition    bool
		ActivationPrice  float64
		CallbackRate     float64
		WorkingType      string
		PriceProtect     bool
		NewOrderRespType string
		RecvWindow       int32
		Timestamp        int64
		MultiOrder       int16
		IptTime          int64
		OrderID          int64 // DB에는 저장되지 않으나 편의를 위해 들고있는다.
		RateBase         string
		RealizedProfit   float64 // DB에는 저장되지 않으나 누적 계산을 위해 들고있는다.
	}

	OrderResult struct {
		StrategyName             string
		ExchangeAlias            string
		Author                   string
		ID                       int64  `json:"-"`
		ExchangeOrderID          int64  `json:"-"`
		ClientOrderID            string `json:"-"`
		OrderID                  int64
		Symbol                   string
		Side                     string
		Type                     string
		OrderStatus              string
		TimeInForce              string `json:"-"`
		OriginalQuantity         float64
		OriginalPrice            float64
		AveragePrice             float64
		StopPrice                float64
		ExecutionType            string  `json:"-"`
		OrderLastFilledQuantity  float64 `json:"-"`
		OrderFilledAccumQuantity float64
		LastFilledPrice          float64 `json:"-"`
		CommissionAsset          string  `json:"-"`
		Commission               float64 `json:"-"`
		OrderTradeTime           int64   `json:"-"`
		TraceID                  int64   `json:"-"`
		BidsNotional             float64 `json:"-"`
		AskNotional              float64 `json:"-"`
		IsMakerSide              bool    `json:"-"`
		IsReduceOnly             bool
		StopPriceWorkingType     string
		OriginalOrderType        string `json:"-"`
		PositionSide             string `json:"-"`
		IsCloseConditional       bool   `json:"-"`
		ActivationPrice          float64
		CallbackRate             float64
		RealizedProfit           float64
		IptTime                  int64 `json:"-"`
		UptTime                  int64 `json:"-"`
	}

	Cancel struct {
		Symbol  string
		OrderID int64
	}

	SlackRequest struct {
		Channel string `json:"channel"`
		Text    string `json:"text"`
		Reply   string `json:"reply"`
	}
)

var (
	db      *sql.DB
	dbAdmin *sql.DB

	stringToType = map[string]futures.OrderType{
		"LIMIT":                futures.OrderTypeLimit,
		"MARKET":               futures.OrderTypeMarket,
		"STOP":                 futures.OrderTypeStop,
		"STOP_MARKET":          futures.OrderTypeStopMarket,
		"TRAILING_STOP_MARKET": futures.OrderTypeTrailingStopMarket,
		"TAKE_PROFIT":          futures.OrderTypeTakeProfit,
		"TAKE_PROFIT_MARKET":   futures.OrderTypeTakeProfitMarket,
	}
	stringToSide = map[string]futures.SideType{
		"BUY":  futures.SideTypeBuy,
		"SELL": futures.SideTypeSell,
	}
)

func init() {
	log.SetReportCaller(true)

	c := config.Get().Sql

	{
		connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			c.Url, c.Port, c.Id, c.Pw, c.Db)

		if newDb, err := sql.Open("postgres", connString); err == nil {
			db = newDb
		} else {
			panic(err)
		}
	}

	{
		connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			c.Url, c.Port, c.Id, c.Pw, c.DbAdmin)

		if newDb, err := sql.Open("postgres", connString); err == nil {
			dbAdmin = newDb
		} else {
			panic(err)
		}
	}
}

func GetOpenOrders(alias string) (results []*Order, err error) {
	stmt := `SELECT  r.order_id, o.*
	FROM    public.t_exchange_order as o
	INNER
	JOIN    public.t_exchange_order_result as r
		ON  o.id = r.exchange_order_id
		AND o.exchange_alias = $1
		AND r.status IN ('NEW','PARTIAL_FILL')`

	rows := new(sql.Rows)
	rows, err = db.Query(stmt, alias)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		r := new(Order)
		err = rows.Scan(
			&r.OrderID,
			&r.ID, &r.StrategyName, &r.ExchangeAlias, &r.Symbol, &r.Side, &r.Type, &r.Rate,
			&r.Quantity, &r.Price, &r.StopPrice, &r.CallbackRate, &r.PositionSide,
			&r.ReduceOnly, &r.TimeInForce, &r.ClosePosition, &r.WorkingType, &r.PriceProtect,
			&r.NewOrderRespType, &r.RecvWindow, &r.Timestamp, &r.MultiOrder, &r.IptTime,
			&r.RateBase, &r.ActivationPrice,
		)
		if err != nil {
			return
		}
		results = append(results, r)
	}
	return
}

func GetOpenOrderResults(alias string) (results []*OrderResult, err error) {
	stmt := `SELECT r.*
	FROM	public.t_exchange_order_result as r
	INNER
	JOIN	public.t_exchange_order as o
		ON o.id = r.exchange_order_id
		AND o.exchange_alias = $1
		AND r.status IN ('NEW', 'PARTIAL_FILL')`
	rows := new(sql.Rows)
	rows, err = db.Query(stmt, alias)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		r := new(OrderResult)
		err = rows.Scan(
			&r.ID, &r.ExchangeOrderID, &r.ClientOrderID, &r.OrderID, &r.Symbol, &r.Side, &r.Type,
			&r.OrderStatus, &r.TimeInForce, &r.OriginalQuantity, &r.OriginalPrice, &r.AveragePrice,
			&r.StopPrice, &r.ExecutionType, &r.OrderLastFilledQuantity, &r.OrderFilledAccumQuantity,
			&r.LastFilledPrice, &r.CommissionAsset, &r.Commission, &r.OrderTradeTime, &r.TraceID,
			&r.BidsNotional, &r.AskNotional, &r.IsMakerSide, &r.IsReduceOnly, &r.StopPriceWorkingType,
			&r.OriginalOrderType, &r.PositionSide, &r.IsCloseConditional, &r.ActivationPrice,
			&r.CallbackRate, &r.RealizedProfit, &r.IptTime, &r.UptTime,
		)
		if err != nil {
			return
		}
		results = append(results, r)
	}
	return
}

func GetAuthor(alias string) (author string, err error) {
	stmt := `
	SELECT  author
    FROM    public.t_strategy_info
    WHERE   exchange_alias = $1
    LIMIT   1
    `
	row := dbAdmin.QueryRow(stmt, alias)
	err = row.Scan(&author)
	if err == sql.ErrNoRows {
		return "", nil
	}

	return
}

func (o *Order) GetType() futures.OrderType {
	return stringToType[o.Type]
}

func (o *Order) GetSide() futures.SideType {
	return stringToSide[o.Side]
}

func (o *Order) CheckValid() error {
	// 공통 필수 필드 검사
	if o.Symbol == "" {
		return errors.New("Symbol is mandatory")
	}
	if o.Side == "" {
		return errors.New("Side is mandatory")
	}
	if o.Type == "" {
		return errors.New("OrderType is mandatory")
	}
	if o.Rate != 0.0 && o.Quantity == 0.0 && o.RateBase == "" {
		return errors.New("RateBase is mandatory in rate order")
	}

	// Type별 필수 필드 검사
	switch o.GetType() {
	case futures.OrderTypeLimit:
		// Limit은 Quantity, Price가 필수
		if (o.Rate == 0.0 && o.Quantity == 0.0) || o.Price == 0.0 {
			return errors.New("Limit order requires quantity and price")
		}
	case futures.OrderTypeMarket:
		// Market은 Quantity가 필수
		if o.Rate == 0.0 && o.Quantity == 0.0 {
			return errors.New("Market order requires quantity")
		}
	case futures.OrderTypeStop:
		fallthrough
	case futures.OrderTypeTakeProfit:
		// Stop은 Quantity, Price, StopPrice가 필수
		if (o.Rate == 0.0 && o.Quantity == 0.0) || o.Price == 0.0 || o.StopPrice == 0.0 {
			return errors.New("Stop order requires quantity, price and stopPrice")
		}
	case futures.OrderTypeTakeProfitMarket:
		fallthrough
	case futures.OrderTypeStopMarket:
		// StopMarket은 StopPrice가 필수
		if o.StopPrice == 0.0 {
			return errors.New("Stop Market order requires stopPrice")
		}
	case futures.OrderTypeTrailingStopMarket:
		// TrailingStopMarket은 CallbackRate가 필수
		if o.CallbackRate < 0.1 || o.CallbackRate > 5.0 {
			return errors.New("CallbackRate must be within the range 0.1, 5.0")
		}
	default:
		return errors.New("Invalid order type")
	}

	return nil
}

func (o *Order) Insert() error {
	o.IptTime = common.Now()

	stmt, err := db.Prepare(`INSERT INTO public.t_exchange_order(strategy_name, exchange_alias, symbol,
		side, type, rate, quantity, price, stop_price, call_back_rate, position_side, reduce_only,
		time_in_force, close_position, working_type, price_protect, new_order_resp_type, recv_window,
		timestamp, multi_order, ipt_time, rate_base, activation_price)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
		RETURNING id`)
	if err != nil {
		return err
	}

	var id int64
	err = stmt.QueryRow(
		o.StrategyName,
		o.ExchangeAlias,
		o.Symbol,
		o.Side,
		o.Type,
		o.Rate,
		o.Quantity,
		o.Price,
		o.StopPrice,
		o.CallbackRate,
		o.PositionSide,
		o.ReduceOnly,
		o.TimeInForce,
		o.ClosePosition,
		o.WorkingType,
		o.PriceProtect,
		o.NewOrderRespType,
		o.RecvWindow,
		o.Timestamp,
		o.MultiOrder,
		o.IptTime,
		o.RateBase,
		o.ActivationPrice,
	).Scan(&id)
	if err != nil {
		return err
	}

	o.ID = id
	return nil
}

func (o *Order) WriteSlippage(r *OrderResult) error {
	orderPrice := 0.0
	switch o.GetType() {
	case futures.OrderTypeMarket:
		orderPrice = o.Price
	case futures.OrderTypeStopMarket:
		fallthrough
	case futures.OrderTypeTakeProfitMarket:
		orderPrice = o.StopPrice
	default:
		// Makret, StopMarket, TakeProfitMarket 주문을 제외한 Type은 슬리피지를 기록하지 않는다.
		return nil
	}

	logTime := common.Now()
	duration := r.OrderTradeTime - o.Timestamp

	stmt, err := db.Prepare(`INSERT INTO public.t_exchange_slippage(log_time, strategy_name, exchange_alias,
		symbol, order_id, order_price, contract_price, order_size, order_type, duration_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`)
	if err != nil {
		return err
	}

	result, err := stmt.Exec(
		logTime,
		o.StrategyName,
		o.ExchangeAlias,
		o.Symbol,
		r.OrderID,
		orderPrice,
		r.LastFilledPrice,
		r.OrderFilledAccumQuantity,
		o.Type,
		duration,
	)
	if err != nil {
		return err
	}

	if cnt, _ := result.RowsAffected(); cnt <= 0 {
		return errors.New("No rows affected.")
	}

	return nil
}

func (o *Order) Load() error {
	if o.ID <= 0 {
		return errors.New("ID is mandatory")
	}

	stmt := "SELECT * FROM public.t_exchange_order WHERE id = $1"
	if err := db.QueryRow(stmt, o.ID).Scan(
		&o.ID, &o.StrategyName, &o.ExchangeAlias, &o.Symbol, &o.Side, &o.Type, &o.Rate,
		&o.Quantity, &o.Price, &o.StopPrice, &o.CallbackRate, &o.PositionSide, &o.ReduceOnly,
		&o.TimeInForce, &o.ClosePosition, &o.WorkingType, &o.PriceProtect, &o.NewOrderRespType,
		&o.RecvWindow, &o.Timestamp, &o.MultiOrder, &o.IptTime, &o.RateBase, &o.ActivationPrice,
	); err != nil {
		return err
	}

	return nil
}

func (r *OrderResult) Insert() error {
	if r.ExchangeOrderID <= 0 {
		return errors.New("ExchangeOrderID is mandatory.")
	}

	r.IptTime = common.Now()

	stmt, err := db.Prepare(`INSERT INTO public.t_exchange_order_result(exchange_order_id,
		client_order_id, order_id, symbol, side, type, status, time_in_force, original_quantity,
		original_price, average_price, stop_price, execute_type, order_last_filled_quantity,
		order_filled_accum_quantity, last_filled_price, commission_asset, commission, order_trade_time, trace_id,
		bids_notional, ask_notional, is_maker_side, is_reduce_only, stop_price_working_type,
		original_order_type, position_side, is_close_conditional, activation_price, call_back_rate,
		realized_profit, ipt_time, upt_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33)
		RETURNING id`)
	if err != nil {
		return err
	}

	var resultID int64
	err = stmt.QueryRow(
		r.ExchangeOrderID,
		r.ClientOrderID,
		r.OrderID,
		r.Symbol,
		r.Side,
		r.Type,
		r.OrderStatus,
		r.TimeInForce,
		r.OriginalQuantity,
		r.OriginalPrice,
		r.AveragePrice,
		r.StopPrice,
		r.ExecutionType,
		r.OrderLastFilledQuantity,
		r.OrderFilledAccumQuantity,
		r.LastFilledPrice,
		r.CommissionAsset,
		r.Commission,
		r.OrderTradeTime,
		r.TraceID,
		r.BidsNotional,
		r.AskNotional,
		r.IsMakerSide,
		r.IsReduceOnly,
		r.StopPriceWorkingType,
		r.OriginalOrderType,
		r.PositionSide,
		r.IsCloseConditional,
		r.ActivationPrice,
		r.CallbackRate,
		r.RealizedProfit,
		r.IptTime,
		r.UptTime,
	).Scan(&resultID)

	if err != nil {
		return err
	}
	r.ID = resultID

	// log 적재
	if err := r.Log(); err != nil {
		return err
	}

	return nil
}

func (r *OrderResult) Load() error {
	if r.OrderID <= 0 {
		return errors.New("OrderID is mandatory")
	}

	stmt := "SELECT * FROM public.t_exchange_order_result WHERE order_id = $1"
	if err := db.QueryRow(stmt, r.OrderID).Scan(
		&r.ID, &r.ExchangeOrderID, &r.ClientOrderID, &r.OrderID, &r.Symbol, &r.Side, &r.Type,
		&r.OrderStatus, &r.TimeInForce, &r.OriginalQuantity, &r.OriginalPrice, &r.AveragePrice,
		&r.StopPrice, &r.ExecutionType, &r.OrderLastFilledQuantity, &r.OrderFilledAccumQuantity,
		&r.LastFilledPrice, &r.CommissionAsset, &r.Commission, &r.OrderTradeTime, &r.TraceID,
		&r.BidsNotional, &r.AskNotional, &r.IsMakerSide, &r.IsReduceOnly, &r.StopPriceWorkingType,
		&r.OriginalOrderType, &r.PositionSide, &r.IsCloseConditional, &r.ActivationPrice,
		&r.CallbackRate, &r.RealizedProfit, &r.IptTime, &r.UptTime,
	); err != nil {
		return err
	}

	return nil
}

func (r *OrderResult) Update() error {
	r.UptTime = common.Now()

	stmt, err := db.Prepare(`UPDATE public.t_exchange_order_result SET
	status=$1, time_in_force=$2, original_quantity=$3, original_price=$4, average_price=$5,
	stop_price=$6, execute_type=$7, order_last_filled_quantity=$8, order_filled_accum_quantity=$9,
	last_filled_price=$10, commission_asset=$11, commission=$12, order_trade_time=$13, trace_id=$14,
	bids_notional=$15, ask_notional=$16, is_maker_side=$17, is_reduce_only=$18, stop_price_working_type=$19,
	original_order_type=$20, position_side=$21, is_close_conditional=$22, activation_price=$23,
	call_back_rate=$24, realized_profit=$25, upt_time=$26 WHERE order_id = $27
	RETURNING id`)

	if err != nil {
		return err
	}

	var resultID int64
	err = stmt.QueryRow(
		r.OrderStatus,
		r.TimeInForce,
		r.OriginalQuantity,
		r.OriginalPrice,
		r.AveragePrice,
		r.StopPrice,
		r.ExecutionType,
		r.OrderLastFilledQuantity,
		r.OrderFilledAccumQuantity,
		r.LastFilledPrice,
		r.CommissionAsset,
		r.Commission,
		r.OrderTradeTime,
		r.TraceID,
		r.BidsNotional,
		r.AskNotional,
		r.IsMakerSide,
		r.IsReduceOnly,
		r.StopPriceWorkingType,
		r.OriginalOrderType,
		r.PositionSide,
		r.IsCloseConditional,
		r.ActivationPrice,
		r.CallbackRate,
		r.RealizedProfit,
		r.UptTime,
		r.OrderID,
	).Scan(&resultID)
	if err != nil {
		return err
	}

	r.ID = resultID
	if err := r.Log(); err != nil {
		return err
	}

	return nil
}

func (r *OrderResult) Log() error {
	// log 적재
	stmt, err := db.Prepare(`INSERT INTO public.t_exchange_order_log(log_time, exchange_order_id,
		exchange_result_id, client_order_id, order_id, symbol, side, type, status, time_in_force, original_quantity,
		original_price, average_price, stop_price, execute_type, order_last_filled_quantity,
		order_filled_accum_quantity, last_filled_price, commission_asset, commission, order_trade_time, trace_id,
		bids_notional, ask_notional, is_maker_side, is_reduce_only, stop_price_working_type,
		original_order_type, position_side, is_close_conditional, activation_price, call_back_rate,
		realized_profit, ipt_time, upt_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)
	`)

	if err != nil {
		return err
	}

	result, err := stmt.Exec(
		common.Now(),
		r.ExchangeOrderID,
		r.ID,
		r.ClientOrderID,
		r.OrderID,
		r.Symbol,
		r.Side,
		r.Type,
		r.OrderStatus,
		r.TimeInForce,
		r.OriginalQuantity,
		r.OriginalPrice,
		r.AveragePrice,
		r.StopPrice,
		r.ExecutionType,
		r.OrderLastFilledQuantity,
		r.OrderFilledAccumQuantity,
		r.LastFilledPrice,
		r.CommissionAsset,
		r.Commission,
		r.OrderTradeTime,
		r.TraceID,
		r.BidsNotional,
		r.AskNotional,
		r.IsMakerSide,
		r.IsReduceOnly,
		r.StopPriceWorkingType,
		r.OriginalOrderType,
		r.PositionSide,
		r.IsCloseConditional,
		r.ActivationPrice,
		r.CallbackRate,
		r.RealizedProfit,
		r.IptTime,
		r.UptTime,
	)

	if err != nil {
		return err
	}

	if cnt, _ := result.RowsAffected(); cnt <= 0 {
		return errors.New("No rows affected.")
	}

	return nil
}

func (r *OrderResult) LoadLog() error {
	if r.ID <= 0 {
		return errors.New("ID is mandatory")
	}

	var ID int64
	stmt := "SELECT * FROM public.t_exchange_order_log WHERE exchange_result_id = $1"
	db.QueryRow(stmt, r.ID).Scan(
		&ID, &r.ExchangeOrderID, &r.ID, &r.ClientOrderID, &r.OrderID, &r.Symbol, &r.Side, &r.Type,
		&r.OrderStatus, &r.TimeInForce, &r.OriginalQuantity, &r.OriginalPrice, &r.AveragePrice,
		&r.StopPrice, &r.ExecutionType, &r.OrderLastFilledQuantity, &r.OrderFilledAccumQuantity,
		&r.LastFilledPrice, &r.CommissionAsset, &r.Commission, &r.OrderTradeTime, &r.TraceID,
		&r.BidsNotional, &r.AskNotional, &r.IsMakerSide, &r.IsReduceOnly, &r.StopPriceWorkingType,
		&r.OriginalOrderType, &r.PositionSide, &r.IsCloseConditional, &r.ActivationPrice,
		&r.CallbackRate, &r.RealizedProfit, &r.IptTime, &r.UptTime,
	)

	return nil
}

func (r *OrderResult) SendSlack(logger *log.Entry) error {
	status := r.OrderStatus
	if status == "NEW" && r.IsReduceOnly {
		status = "CLOSE"
	} else if status == "NEW" && !r.IsReduceOnly {
		status = "OPEN"
	}

	price := r.StopPrice
	if r.OriginalPrice > 0.0 {
		price = r.OriginalPrice
	} else if r.AveragePrice > 0.0 {
		price = r.AveragePrice
	}

	profit := ""
	if r.RealizedProfit != 0.0 {
		profit = fmt.Sprintf(" -> $ %f", r.RealizedProfit)
	}

	// text에는 주문의 내용을 요약해서 전달한다.
	text := fmt.Sprintf("%s %s %s %s %f @ %f%s",
		r.StrategyName, r.Symbol, status, r.Side, r.OriginalQuantity, price, profit)

	channel := config.GetSlackChannel()
	b, err := json.Marshal(r)
	if err != nil {
		logger.WithError(err).WithFields(log.Fields{"channel": channel, "result": r}).Error("Failed to marshal.")
		return err
	}

	// 공용 채널에 메시지 전송
	{
		req := SlackRequest{
			Channel: channel,
			Text:    text,
			Reply:   string(b),
		}

		pb, err := json.Marshal(req)
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to marshal.")
			return err
		}
		buff := bytes.NewBuffer(pb)

		res, err := http.Post(config.Get().Slack.Url, "application/json", buff)
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to send.")
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			logger.WithFields(log.Fields{"channel": channel, "req": req}).Error("slack api error.")
			return err
		}
	}

	// author별 채널에 메시지 전송
	isLive := config.Get().Server.Env == "live"
	author := strings.TrimSpace(r.Author)
	if isLive && author != "" {
		channel += "-" + author
		req := SlackRequest{
			Channel: channel,
			Text:    text,
			Reply:   string(b),
		}

		pb, err := json.Marshal(req)
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to marshal.")
			return err
		}
		buff := bytes.NewBuffer(pb)

		res, err := http.Post(config.Get().Slack.Url, "application/json", buff)
		if err != nil {
			logger.WithError(err).WithFields(log.Fields{"channel": channel, "req": req}).Error("Failed to send.")
			return err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			logger.WithFields(log.Fields{"channel": channel, "req": req}).Error("slack api error.")
			return err
		}
	}

	return nil
}

func (o *Cancel) CheckValid() error {
	if o.Symbol == "" {
		return errors.New("Symbol is mandatory")
	}

	return nil
}
