module arques.com/order

go 1.15

require (
	github.com/adshao/go-binance/v2 v2.0.0-00010101000000-000000000000
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/go-redis/redis/v8 v8.4.2
	github.com/labstack/echo v3.3.10+incompatible
	github.com/labstack/gommon v0.3.0 // indirect
	github.com/lib/pq v1.9.0
	github.com/sirupsen/logrus v1.7.0
	github.com/stretchr/testify v1.6.1
	golang.org/x/crypto v0.0.0-20201203163018-be400aefbc4c // indirect
	gopkg.in/yaml.v2 v2.3.0
)

replace (
	github.com/adshao/go-binance => ./go-binance
	github.com/adshao/go-binance/v2 => ./go-binance/v2
)
