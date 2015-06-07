package main

import (
        "errors"
        "fmt"
        "log"
        "os"
)

func main() {
        switch os.Args[1] {
        case "work":
                work(os.Args[2:])
        case "msg":
                client, err := NewClient("redis://localhost:6379")
                if err != nil {
                        log.Println("Connection failed:", err)
                        os.Exit(1)
                }
                err = client.Enqueue(NewJob(os.Args[2], os.Args[3]))
                log.Println(err)
        }
}

func info(j *Job) error {
        if j.Message == "KILLME" {
                return errors.New("KILLED")
        }
        fmt.Println(j.CreatedAt, "-", j.Queue(), ":", j.Message)
        return nil
}

func work(queues []string) {
        handler := HandleFunc(info)
        for _, queue := range queues {
                Handle(queue, handler)
        }
        log.Println("Queues: ", queues)
        log.Println("Listen: redis://localhost:6379")
        ListenAndServe("redis://localhost:6379")
}
