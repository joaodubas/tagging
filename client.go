package main

import (
	"github.com/fzzy/radix/redis"
	"time"
)

type Client struct {
	Client *redis.Client
	pipe []pipe
}

func NewClient(cs string) (*Client, error) {
	client, err := redis.DialTimeout("tcp", cs, time.Second * 10)

	if err != nil {
		return nil, err
	}

	return &Client{client, []pipe{}}, nil
}

// keys interface --------------------------------------------------------------

func (c *Client) Keys(key string) (*redis.Reply, error) {
	return c.cmd("keys", key)
}

// string interface ------------------------------------------------------------

func (c *Client) Get(key string) (*redis.Reply, error) {
	return c.cmd("get", key)
}

func (c *Client) Set(key string, value interface{}) (*redis.Reply, error) {
	return c.cmd("set", key, value)
}

// set interface ---------------------------------------------------------------

func (c *Client) Sadd(key string, args ...interface{}) (*redis.Reply, error) {
	return c.cmd("sadd", key, args)
}

func (c *Client) Srem(key string, args ...interface{}) (*redis.Reply, error) {
	return c.cmd("srem", key, args)
}

func (c *Client) Scard(key string) (*redis.Reply, error) {
	return c.cmd("scard", key)
}

func (c *Client) Smembers(key string) (*redis.Reply, error) {
	return c.cmd("smembers", key)
}

// pub/sub interface -----------------------------------------------------------

func (c *Client) Publish(channel string, value interface{}) (*redis.Reply, error) {
	return c.cmd("publish", channel, value)
}

// batch interface -------------------------------------------------------------

type pipe struct {
	cmd string
	args []interface{}
}

func (c *Client) Multi() {
	c.cleanPipe()
}

func (c *Client) Add(cmd string, args ...interface{}) {
	c.pipe = append(c.pipe, pipe{cmd, args})
}

func (c *Client) Exec() (*redis.Reply, error) {
	c.cmd("multi")
	for _, p := range c.pipe {
		c.cmd(p.cmd, p.args...)
	}
	c.cleanPipe()
	return c.cmd("exec")
}

func (c *Client) cleanPipe() {
	c.pipe = []pipe{}
}

// utility ---------------------------------------------------------------------

func (c *Client) cmd(cmd string, args ...interface{}) (*redis.Reply, error) {
	r := c.Client.Cmd(cmd, args)
	return r, r.Err
}

func (c *Client) Close() error {
	return c.Client.Close()
}
