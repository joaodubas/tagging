package main

import (
	"github.com/fzzy/radix/extra/pubsub"
)

type PSClient struct {
	Client *Client
	*pubsub.SubClient
}

func NewPSClient(client *Client) *PSClient {
	ps := pubsub.NewSubClient(client.Client)
	return &PSClient{client, ps}
}

func (c *PSClient) Close() error {
	return c.Client.Close()
}
