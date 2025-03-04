package main

import (
	"arques.com/order/api"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

func main() {

	e := echo.New()
	e.Use(middleware.Logger())

	e.GET("/positions", api.GetPositions)
	e.GET("/open_orders", api.GetOpenOrders)
	e.GET("/funding_rate", api.GetFundingRate)
	e.GET("/account", api.GetAccount)
	e.GET("/time_offsets", api.GetTimeOffsets)

	e.POST("/order", api.NewOrder)
	e.POST("/cancel", api.CancelOrder)
	e.POST("/close", api.CloseAll)
	e.POST("/leverage", api.PostLeverage)
	e.POST("/request_load_symbol_properties", api.PostRequestLoadSymbolProperties)

	// Exchange Alias 업데이트 호출 API
	e.POST("/exchange_alias", api.ExchangeAlias)

	// 전략의 author를 설정한다
	e.POST("/update_author", api.UpdateAuthor)

	e.Logger.Fatal(e.Start(":80"))
}
