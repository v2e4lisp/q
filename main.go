package main

import (
        "errors"
        "fmt"
        "log"
        "os"
        "time"
)

// redis
//
// (list) queue:job
// (list) queue:job_failed

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
        if j.Topic == "long" {
                ticker := time.Tick(5 * time.Second)
                count := 0
                for {
                        <-ticker
                        count++
                        if count == 6 {
                                break
                        }
                        log.Println("working...")
                }
                log.Println("job done")
        }
        fmt.Println(j.CreatedAt, "-", j.Queue(), ":", j.Message)
        return nil
}

func work(queues []string) {
        handler := HandleFunc(info)
        for _, queue := range queues {
                worker.Handle(queue, handler)
        }
        log.Println(worker.Name())
        log.Println("Listen: redis://localhost:6379")
        ListenAndServe("redis://localhost:6379")
}
