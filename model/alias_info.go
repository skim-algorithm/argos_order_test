package model

import (
	"database/sql"
	"fmt"
)

type (
	// AliasInfo 는 DB의 t_exchange_alias 정보에 해당된다.
	AliasInfo struct {
		ExchangeName  string
		ExchangeAlias string
		APIKey        string
		SecretKey     string
	}
)

// LoadAliasInfos 는 DB에서 alias 정보를 가져와 map 형태로 리턴한다.
func LoadAliasInfos() map[string]*AliasInfo {
	/*
		var use_type string
		if config.Get().Server.Env == "live" {
			use_type = "alpha"
		} else {
			use_type = "alpha_dev"
		}
	*/

	sqlStatement := `
		SELECT exchange_name, exchange_alias, alias_key, alias_secret
		FROM public.t_exchange_alias
		WHERE use_type in ('alpha', 'alpha_dev')`

	rows, err := db.Query(sqlStatement) //, use_type)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	infos := make(map[string]*AliasInfo)

	for rows.Next() {
		var info AliasInfo
		switch err := rows.Scan(&info.ExchangeName, &info.ExchangeAlias,
			&info.APIKey, &info.SecretKey); err {
		case sql.ErrNoRows:
			fmt.Println("No row returned")
		case nil:
			// fmt.Printf("Alias info loaded: alias=%s\n", info.exchangeAlias)
			infos[info.ExchangeAlias] = &info
		default:
			panic(err)
		}
	}

	return infos
}
