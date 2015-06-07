package main

import (
	"errors"
	"net/url"
	"sync"

	"github.com/garyburd/redigo/redis"
)

const (
	SELECT = "SELECT"
	AUTH   = "AUTH"
	BLPOP  = "BLPOP"
	RPUSH  = "RPUSH"
	SADD   = "SADD"
)

type Conn struct {
	mu sync.Mutex
	redis.Conn
}

func (c *Conn) DO(cmd string, args ...interface{}) (reply interface{}, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply, err = c.Do(cmd, args...)
	return
}

func (c *Conn) SELECT(db string) error {
	_, err := c.DO(SELECT, db)
	return err
}

func (c *Conn) AUTH(pass string) error {
	_, err := c.DO(AUTH, pass)
	return err
}

func (c *Conn) BLPOP(args ...interface{}) (reply interface{}, err error) {
	reply, err = c.DO(BLPOP, args...)
	return
}

func (c *Conn) RPUSH(args ...interface{}) error {
	_, err := c.DO(RPUSH, args...)
	return err
}

type ConnConfig struct {
	Host string
	Auth string
	DB   string
}

func parseURL(ru string) (conf ConnConfig, err error) {
	conf = ConnConfig{}
	if ru == "" {
		err = errors.New("empty url")
		return
	}

	var u *url.URL

	u, err = url.Parse(ru)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = errors.New("url scheme is not redis")
		return
	}

	conf.Host = u.Host
	if u.User != nil {
		if pass, ok := u.User.Password(); ok {
			conf.Auth = pass
		}
	}
	if u.Path != "" {
		conf.DB = u.Path
	} else {
		conf.DB = "0"
	}
	return
}

func NewConn(ru string) (conn Conn, err error) {
	var conf ConnConfig
	conf, err = parseURL(ru)
	if err != nil {
		return
	}
	conn = Conn{}
	if conn.Conn, err = redis.Dial("tcp", conf.Host); err != nil {
		return
	}
	if conf.Auth != "" {
		if err = conn.AUTH(conf.Auth); err != nil {
			return
		}
	}

	conn.SELECT(conf.DB)
	return
}
