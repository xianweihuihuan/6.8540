package main

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import "6.5840/mr"
import "unicode"
import "strings"
import "strconv"

// map 函数对于每个输入文件调用一次。第一个参数是输入文件的名称，第二个是文件的完整内容。
// 你应该忽略输入文件名，只关注内容参数。返回值是一个键值对的切片。

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}
// reduce 函数对于 map 任务生成的每个键调用一次，
// 每次调用时，会传入该键对应的所有值，这些值是由任何 map 任务生成的。
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
