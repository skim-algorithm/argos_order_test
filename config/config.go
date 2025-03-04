package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server struct {
		Env string `yaml:"env"`
	} `yaml:"server"`
	Sql struct {
		Url     string
		UrlLive string `yaml:"url_live"`
		UrlDev  string `yaml:"url_dev"`
		Port    int    `yaml:"port"`
		Db      string `yaml:"db"`
		DbAdmin string `yaml:"db_admin"`
		Id      string `yaml:"id"`
		Pw      string `yaml:"pw"`
	} `yaml:"sql"`
	API struct {
		Url   string `yaml:"url"`
		Token string
	} `yaml:"api"`
	Redis struct {
		Url  string `yaml:"url"`
		Port int    `yaml:"port"`
	} `yaml:"redis"`
	Slack struct {
		Url         string `yaml:"url"`
		LiveChannel string `yaml:"live_channel"`
		DevChannel  string `yaml:"dev_channel"`
	} `yaml:"slack"`
}

var config Config

func rootDir() string {
	_, b, _, _ := runtime.Caller(0)
	d := path.Join(path.Dir(b))
	return filepath.Dir(d)
}

func init() {
	dir := rootDir() + "/config.yml"
	data, err := ioutil.ReadFile(dir)
	if err != nil {
		fmt.Printf("(%s)Failed to open config file: %s\n", err, dir)
		return
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		fmt.Printf("(%s) Failed to unmarshall\n", err)
		return
	}

	if config.Server.Env == "live" {
		config.Sql.Url = config.Sql.UrlLive
	} else {
		config.Sql.Url = config.Sql.UrlDev
	}

	{
		dir := rootDir() + "/api_key.json"
		bytes, err := ioutil.ReadFile(dir)
		if err != nil {
			fmt.Printf("(%s)Failed to open config file: %s\n", err, dir)
			return
		}
		var objMap map[string]string
		if err = json.Unmarshal(bytes, &objMap); err != nil {
			fmt.Printf("(%s) Failed to unmarshall\n", err)
			return
		}

		config.API.Token = objMap["bearer_token"]
	}

	fmt.Printf("Config loaded from %s\n", dir)
}

func Get() *Config {
	return &config
}

func GetSlackChannel() string {
	if config.Server.Env == "live" {
		return config.Slack.LiveChannel
	} else {
		return config.Slack.DevChannel
	}
}
