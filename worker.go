package main

import (
        "encoding/json"
        "fmt"
        "log"
        "math/rand"
        "os"
        "os/signal"
        "strings"
        "sync"
        "syscall"
)

// Exmaples:
//
// package main
//
// func handleWelcomeEmail(j *job) error {
//         if err := sendWelcomeEmail(j.Msg); err != nil {
//                 return err
//         }
//         return nil
// }
//
// func handlerResizeAvatar(j *job) error {
//         if err := resizeAvatar(j.Msg); err != nil {
//                 return err
//         }
//         return nil
// }
//
// func main() {
//         q.Handle("welcome-email", q.HandleFunc(handleWelcomeEmail))
//         q.Handle("resize-avatar", q.HandleFunc(handleResizeAvatar))
//         q.ListenAndServe("redis://localhost:6379")
// }
type Handler interface {
        ServeJob(*Job) error
}
type HandleFunc func(*Job) error

func (f HandleFunc) ServeJob(j *Job) error { return f(j) }

type Worker struct {
        rconn    Conn
        wconn    Conn
        once     sync.Once
        done     chan bool
        handlers map[string]Handler
}

func NewWorker() *Worker {
        return &Worker{
                done:     make(chan bool),
                handlers: make(map[string]Handler),
        }
}

var (
        worker         = NewWorker()
        Handle         = worker.Handle
        ListenAndServe = worker.ListenAndServe
)

func (w *Worker) handleSignals() {
        sigs := make(chan os.Signal, 1)
        signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
        go func() {
                s := <-sigs
                log.Println("Signal received:", s)
                w.Done()
        }()
}

func (w *Worker) Name() (name string, err error) {
        queues := []string(nil)
        for queue, _ := range w.handlers {
                queues = append(queues, queue[6:])
        }
        qname := strings.Join(queues, ",")

        var host string
        host, err = os.Hostname()
        if err != nil {
                return
        }
        pid := os.Getpid()
        name = fmt.Sprintf("%s:%d:%s:%d", host, pid, qname, rand.Int())
        return
}

func (w *Worker) ListenAndServe(url string) error {
        var err error
        w.rconn, err = NewConn(url)
        if err != nil {
                return err
        }
        w.wconn, err = NewConn(url)
        if err != nil {
                return err
        }
        w.handleSignals()
        w.loop()
        return nil
}

func (w *Worker) Handle(queue string, h Handler) { w.handlers[JobQueueName(queue)] = h }
func (w *Worker) Done()                          { w.once.Do(func() { close(w.done) }) }

func (w *Worker) accept() (jobs chan *Job) {
        jobs = make(chan *Job)
        args := []interface{}(nil)
        for queue, _ := range w.handlers {
                args = append(args, queue)
        }
        args = append(args, "0")
        go func() {
                for {
                        select {
                        default:
                                r, err := w.rconn.BLPOP(args...)
                                if err != nil {
                                        w.Done()
                                        break
                                }
                                job := &Job{}
                                re := r.([]interface{})
                                if err := json.Unmarshal(re[1].([]byte), job); err != nil {
                                        break
                                }
                                jobs <- job
                        case <-w.done:
                                goto exit
                        }
                }
        exit:
                log.Println("Stop accepting jobs")
        }()
        return
}

func (w *Worker) fail(f *Failure) {
        w.wconn.RPUSH(f.Queue(), f)
        log.Println("Fail to serve job:", f.Job)
}

func (w *Worker) loop() {
        jobs := w.accept()
        for {
                select {
                case j := <-jobs:
                        h, ok := w.handlers[j.Queue()]
                        if !ok {
                                break
                        }
                        if err := h.ServeJob(j); err != nil {
                                w.fail(j.Failed(err))
                        }
                case <-w.done:
                        goto exit
                }
        }
exit:
        log.Println("Stop serving jobs.")
}
