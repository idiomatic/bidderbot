package main

import (
	"sort"
)

type FloatMap map[float64]interface{}

func (f FloatMap) Keys() []float64 {
	var keys []float64
	for k := range f {
		keys = append(keys, k)
	}
	sort.Float64s(keys)
	return keys
}
