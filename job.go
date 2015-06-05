package main

import "time"

type Failure struct {
        Job
        FailedAt time.Time
        Err      error
}

func NewFailure(job *Job, err error) *Failure {
        return &Failure{
                Job:      *job,
                FailedAt: time.Now(),
                Err:      err,
        }
}

func (f *Failure) Queue() string { return f.Job.Queue() + ":failed" }

type Job struct {
        Message   string
        Topic     string
        CreatedAt time.Time
}

func NewJob(topic, msg string) *Job {
        return &Job{
                Message:   msg,
                Topic:     topic,
                CreatedAt: time.Now(),
        }
}

func (job *Job) Queue() string             { return JobQueueName(job.Topic) }
func (job *Job) Failed(err error) *Failure { return NewFailure(job, err) }

func JobQueueName(topic string) string { return "queue:" + topic }
