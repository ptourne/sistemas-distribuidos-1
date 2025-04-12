package logger

import (
	"hash/fnv"
	"time"

	"github.com/fatih/color"
)

type LogLevel int

const (
	Debug LogLevel = iota
	Info
	Warn
	Error
	Fatal
	Panic
)

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
	Tracef(format string, args ...interface{})
}

var TimeColor = color.New(color.BgWhite).Add(color.Faint)
var DebugColor = color.New(color.BgGreen)
var InfoColor = color.New(color.BgBlue)
var WarnColor = color.New(color.BgYellow)
var ErrorColor = color.New(color.BgRed)
var FatalColor = color.New(color.BgRed).Add(color.Bold)

type ConsoleLogger struct {
	name  string
	level LogLevel
	color *color.Color
}

func NewConsoleLogger(name string, level LogLevel) *ConsoleLogger {
	return &ConsoleLogger{
		name:  name,
		level: level,
		color: getColor(name),
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func hslToRgb(h, s, l int) (r, g, b int) {
	hf := float64(h) / 255.0
	sf := float64(s) / 255.0
	lf := float64(l) / 255.0
	var rf, gf, bf float64

	if s == 0 {
		rf = lf // achromatic
		gf = lf
		bf = lf
	} else {
		var q float64
		if lf < 0.5 {
			q = lf * (1 + sf)
		} else {
			q = lf + sf - lf*sf
		}
		var p = 2*lf - q
		rf = hueToRgb(p, q, hf+1/3)
		gf = hueToRgb(p, q, hf)
		bf = hueToRgb(p, q, hf-1/3)
	}

	return int(rf * 255), int(gf * 255), int(bf * 255)
}

func hueToRgb(p, q, t float64) float64 {
	if t < 0 {
		t += 1
	}
	if t > 1 {
		t -= 1
	}
	if t < 1/6 {
		return p + (q-p)*6*t
	}
	if t < 1/2 {
		return q
	}
	if t < 2/3 {
		return p + (q-p)*(2/3-t)*6
	}
	return p
}

func getColor(name string) *color.Color {
	hash := hash(name)
	h := (hash & 0xFF0000) >> 16
	s := (hash & 0x00FF00) >> 8
	l := hash & 0x0000FF
	s = uint32((float64(s)/255.0)*10 + 150)
	s = uint32((float64(s)/255.0)*10 + 150)
	r, g, b := hslToRgb(int(h), int(s), int(l))

	return color.RGB(r, g, b)
}

func (l *ConsoleLogger) Logf(level LogLevel, format string, args ...interface{}) {
	if l.level <= level {
		now := time.Now()
		TimeColor.Printf("%s ", now.Format("2006-01-02 15:04:05"))
		l.color.Printf("%s ", l.name)
		switch level {
		case Debug:
			DebugColor.Printf("DEBUG: ")
		case Info:
			InfoColor.Printf("INFO: ")
		case Warn:
			WarnColor.Printf("WARN: ")
		case Error:
			ErrorColor.Printf("ERROR: ")
		case Fatal:
			FatalColor.Printf("FATAL: ")
		}
	}
}

func (l *ConsoleLogger) Debugf(format string, args ...interface{}) {
	l.Logf(Debug, format, args...)
}

func (l *ConsoleLogger) Infof(format string, args ...interface{}) {
	l.Logf(Info, format, args...)
}

func (l *ConsoleLogger) Warnf(format string, args ...interface{}) {
	l.Logf(Warn, format, args...)
}

func (l *ConsoleLogger) Errorf(format string, args ...interface{}) {
	l.Logf(Error, format, args...)
}

func (l *ConsoleLogger) Fatalf(format string, args ...interface{}) {
	l.Logf(Fatal, format, args...)
}
