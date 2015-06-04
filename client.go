package main

import "encoding/json"

// Examples:
//
// q.Connect("redis://localhost:6379")
// q.Enqueue(NewJob("queue-name", "any message"))
type Client struct {
        conn Conn
}

var client = &Client{}

func (c *Client) Enqueue(job *Job) error {
        queue := "queue:" + job.Queue
        j, err := json.Marshal(job)
        if err != nil {
                return err
        }
        return c.conn.RPUSH(queue, j)
}

func (c *Client) Connect(ru string) error {
        var err error
        c.conn, err = NewConn(ru)
        return err
}

func Enqueue(job *Job) error  { return client.Enqueue(job) }
func Connect(ru string) error { return client.Connect(ru) }
