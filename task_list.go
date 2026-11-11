package xxl

import "sync"

type taskList[K comparable, V any] struct {
	data sync.Map
}

// newTaskHeadList 初始化兼容的任务头列表。
func newTaskHeadList() *taskList[string, taskHead] {
	return &taskList[string, taskHead]{}
}

// newTaskList 初始化兼容的任务运行列表。
func newTaskList() *taskList[int64, *Task] {
	return &taskList[int64, *Task]{}
}

// Set 存储指定任务键对应的值。
func (t *taskList[K, V]) Set(key K, val V) {
	t.data.Store(key, val)
}

// Get 读取任务值并报告是否存在。
func (t *taskList[K, V]) Get(key K) (v V, ok bool) {
	var a any
	a, ok = t.data.Load(key)
	if ok {
		v = a.(V)
	}
	return
}

// Del 删除任务键对应的值。
func (t *taskList[K, V]) Del(key K) {
	t.data.Delete(key)
}

// LoadAndDel 原子读取并删除任务值。
func (t *taskList[K, V]) LoadAndDel(key K) (v V, ok bool) {
	var a any
	a, ok = t.data.LoadAndDelete(key)
	if ok {
		v = a.(V)
	}
	return
}
