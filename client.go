package main

import "encoding/json"

// Examples:
//
// client, _ := NewClient("redis://localhost:6379")
// client.Enqueue(NewJob("queue-name", "any message"))
type Client struct {
        conn Conn
        u    string
}

func (c *Client) Enqueue(job *Job) error {
        queue := job.Queue()
        j, err := json.Marshal(job)
        if err != nil {
                return err
        }

        c.conn.mu.Lock()
        defer c.conn.mu.Unlock()
        c.conn.Send(SADD, "queues", queue)
        c.conn.Send(RPUSH, queue, j)
        return c.conn.Flush()
}

func NewClient(ru string) (client *Client, err error) {
        client = &Client{u: ru}
        client.conn, err = NewConn(ru)
        return
}
