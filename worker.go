package main

import "encoding/json"

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
        block    int
        done     chan bool
        handlers map[string]Handler
        queues   []string
}

var (
        worker = &Worker{
                done:     make(chan bool),
                handlers: make(map[string]Handler),
                queues:   []string(nil),
        }
        Handle         = worker.Handle
        ListenAndServe = worker.ListenAndServe
)

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
        w.loop()
        return nil
}

func (w *Worker) Handle(queue string, h Handler) {
        w.handlers[queue] = h
        w.queues = append(w.queues, "queue:"+queue)
}

func (w *Worker) receive() (jobs chan *Job, exit chan bool) {
        jobs = make(chan *Job)
        exit = make(chan bool)
        args := append(w.queues[0:], "0")
        nargs := make([]interface{}, len(args))
        for i, arg := range args {
                nargs[i] = arg
        }
        go func() {
                for {
                        select {
                        default:
                                r, err := w.rconn.BLPOP(nargs...)
                                if err != nil {
                                        continue
                                }
                                job := &Job{}
                                re := r.([]interface{})
                                json.Unmarshal(re[1].([]byte), job)
                                jobs <- job
                        case <-w.done:
                                close(exit)
                                goto exit
                        }
                }
        exit:
        }()
        return
}

func (w *Worker) fail(j *Job) {}

func (w *Worker) loop() {
        jobs, exit := w.receive()
        for {
                select {
                case j := <-jobs:
                        h, ok := w.handlers[j.Queue]
                        if !ok {
                                w.fail(j)
                        }
                        if err := h.ServeJob(j); err != nil {
                                w.fail(j)
                        }
                case <-exit:
                        goto exit
                }
        }
        exit:
        //log.Println("Exit Worker")
}
