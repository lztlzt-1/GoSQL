package Queue

import "GoSQL/src/msg"

type Queue[T int] struct {
	data_ []T
}

func NewQueue[T int]() Queue[T] {
	data := make([]T, 0)
	return Queue[T]{data_: data}
}

func (this *Queue[T]) Push(value T) int {
	this.data_ = append(this.data_, value)
	return msg.Success
}

func (this *Queue[T]) Pop() int {
	if len(this.data_) == 0 {
		return msg.NotFound
	}
	this.data_ = this.data_[1:]
	return msg.Success
}

func (this *Queue[T]) GetLength() int {
	return len(this.data_)
}

func (this *Queue[T]) GetData() []T {
	return this.data_
}
