package main

type Worker struct {
	Tasks []Task
}

type Task struct {
	input string
	output string
	operation Operation
}

type Operation interface {
	Process(row Row) *Row
}

type Row struct {
	Elements map[string, any]
}

type Filter1 struct {}

func (f Filter1) Process(row Row, ch) {
	if row lo que sea
	return row
}


func (w Worker) Run() {
	select {
		case msg<-q1:
			w.Tasks.Process(q1)

		case msg<-q2:
				w.Tasks.Process(q1)
		case msg<-q3:
				w.Tasks.Process(q1)
		case msg<-q1:
				w.Tasks.Process(q1)
		case msg<-q1:
				w.Tasks.Process(q1)
	}
}
