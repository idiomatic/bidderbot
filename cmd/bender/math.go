package main

import (
	"math"
	"time"
)

func Round(value float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return math.Floor(value*shift+0.5) / shift
}

func Floor(value float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return math.Floor(value*shift) / shift
}

func Ceil(value float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return math.Ceil(value*shift) / shift
}

func Inflation(age time.Duration, rate float64) float64 {
	return math.Pow(1+rate, age.Hours()/24)
}
