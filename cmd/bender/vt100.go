package main

import (
	"fmt"
)

func DisplayAttributes(attrs string, format string, args ...interface{}) string {
	return "[" + attrs + "m" + fmt.Sprintf(format, args...) + "[m"
}

func Bold(format string, args ...interface{}) string {
	return DisplayAttributes("1", format, args...)
}

func Dim(format string, args ...interface{}) string {
	return DisplayAttributes("2", format, args...)
}

func Red(format string, args ...interface{}) string {
	return DisplayAttributes("31", format, args...)
}

func Green(format string, args ...interface{}) string {
	return DisplayAttributes("32", format, args...)
}

func Yellow(format string, args ...interface{}) string {
	return DisplayAttributes("31", format, args...)
}

func White(format string, args ...interface{}) string {
	return DisplayAttributes("37", format, args...)
}

func RedBackground(format string, args ...interface{}) string {
	return DisplayAttributes("41", format, args...)
}

func GreenBackground(format string, args ...interface{}) string {
	return DisplayAttributes("42", format, args...)
}

func YellowBackground(format string, args ...interface{}) string {
	return DisplayAttributes("43", format, args...)
}

func WhiteBackground(format string, args ...interface{}) string {
	return DisplayAttributes("47", format, args...)
}
