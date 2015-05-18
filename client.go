package main

import (
	"github.com/fzzy/radix/redis"
	"time"
)

type Client struct {
	Client *redis.Client
	pipe   []*pipe
}

func NewClient(cs string) (*Client, error) {
	client, err := redis.DialTimeout("tcp", cs, time.Second*10)

	if err != nil {
		return nil, err
	}

	return &Client{client, []*pipe{}}, nil
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

func (c *Client) Multi() {
	c.cleanPipe()
}

func (c *Client) Add(cmd string, args ...interface{}) asyncReply {
	reply := make(asyncReply)
	c.pipe = append(c.pipe, &pipe{cmd, args, reply})
	return reply
}

func (c *Client) Exec() (*redis.Reply, error) {
	defer c.cleanPipe()

	c.cmd("multi")
	c.execPipe()
	r, err := c.cmd("exec")

	if err != nil {
		return r, err
	}

	for i, e := range r.Elems {
		c.pipe[i].send(e)
	}

	return r, err
}

func (c *Client) Discard() (*redis.Reply, error) {
	c.cmd("Multi")
	c.execPipe()
	c.cleanPipe()
	return c.cmd("discard")
}

func (c *Client) cleanPipe() {
	c.pipe = []*pipe{}
}

func (c *Client) execPipe() {
	for _, p := range c.pipe {
		c.cmd(p.cmd, p.args...)
	}
}

// utility ---------------------------------------------------------------------

func (c *Client) cmd(cmd string, args ...interface{}) (*redis.Reply, error) {
	r := c.Client.Cmd(cmd, args)
	return r, r.Err
}

func (c *Client) Close() error {
	return c.Client.Close()
}

type asyncReply chan *redis.Reply

type pipe struct {
	cmd   string
	args  []interface{}
	reply asyncReply
}

func (p *pipe) send(r *redis.Reply) {
	go func() {
		p.reply <- r
	}()
}
