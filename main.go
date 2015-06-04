package main

import (
        "fmt"
        "log"
        "os"
)

// var (
// mu     sync.Mutex
// queues = make(map[string](chan *Job))
// )

func main() {
        switch os.Args[1] {
        case "work":
                work(os.Args[2:])
        case "msg":
                if err := Connect("redis://localhost:6379"); err != nil {
                        log.Println("Connection failed:", err)
                        os.Exit(1)
                }
                Enqueue(NewJob(os.Args[2], os.Args[3]))
        }

        // conn, err := NewConn("redis://localhost:6379")
        // if err != nil {
        //         log.Println(err)
        //         os.Exit(1)
        // }

        // job := &Job{Msg: "hello q"}
        // // Enqueue("test", job)

        // j, err := json.Marshal(job)
        // if err != nil {
        //         fmt.Println(err)
        // } else {
        //         fmt.Println(string(j))
        // }
}

// func enqueue(queue, msg string) {
//         log.Println("Enqueue:", queue, "<-", msg)
//         conn, err := NewConn("redis://localhost:6379")
//         if err != nil {
//                 fmt.Println("Connection error:", err)
//         }
//         job := &Job{
//                 Queue:   queue,
//                 Message: msg,
//         }
//         j, err := json.Marshal(job)
//         if err != nil {
//                 fmt.Println("Enqueue error:", err)
//         }
//         queue = "queue:" + queue
//         conn.RPUSH(queue, string(j))
// }

func info(j *Job) error {
        fmt.Println(j.Queue, ":", j.Message)
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

// func push(queue, j interface{}) error {
//         mu.Lock()
//         defer mu.Unlock()
//         if err := conn.Send("SADD", "queues", queue); err != nil {
//                 return err
//         }
//         if err := conn.Send("RPUSH", queue, j); err != nil {
//                 return err
//         }
//         return conn.Flush()
// }

type Job struct {
        Message string
        Queue   string
        Err     string
}

func NewJob(queue, msg string) *Job {
        return &Job{
                Message: msg,
                Queue:   queue,
        }
}
