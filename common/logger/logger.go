package logger

import (
	"bytes"
	"crypto/sha1"
	"io"
	"math"
	"os"
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

var TimeColor = color.New(color.BgBlack).Add(color.FgWhite).Add(color.Faint)
var DebugColorTag = color.New(color.BgBlack).Add(color.BgGreen)
var DebugColorText = color.New(color.BgBlack).Add(color.FgGreen)
var InfoColorTag = color.New(color.BgBlack).Add(color.BgBlue)
var InfoColorText = color.New(color.BgBlack).Add(color.FgBlue)
var WarnColorTag = color.New(color.BgBlack).Add(color.BgYellow)
var WarnColorText = color.New(color.BgBlack).Add(color.FgYellow)
var ErrorColorTag = color.New(color.BgBlack).Add(color.BgRed)
var ErrorColorText = color.New(color.BgBlack).Add(color.FgRed)
var FatalColorTag = color.New(color.BgBlack).Add(color.BgRed).Add(color.Bold)
var FatalColorText = color.New(color.BgBlack).Add(color.FgRed).Add(color.Bold)

type ConsoleLogger struct {
	name      string
	level     LogLevel
	nameColor *color.Color
	w         io.Writer
}

func NewConsoleLogger(name string, level LogLevel) *ConsoleLogger {
	return &ConsoleLogger{
		name:      name,
		level:     level,
		nameColor: getColor(name),
		w:         os.Stderr,
	}
}

func hash(input string) [20]byte {
	hash := sha1.Sum([]byte(input))
	return hash
}

func HSLToRGB(h, s, l float64) (ra, ga, ba int) {
	h = h / 360.0
	var r, g, b float64

	if s == 0 {
		r, g, b = l, l, l // achromatic
	} else {
		var q float64
		if l < 0.5 {
			q = l * (1 + s)
		} else {
			q = l + s - l*s
		}
		p := 2*l - q
		r = hueToRGB(p, q, h+1.0/3.0)
		g = hueToRGB(p, q, h)
		b = hueToRGB(p, q, h-1.0/3.0)
	}

	return int(math.Round(r * 255)),
		int(math.Round(g * 255)),
		int(math.Round(b * 255))
}

func hueToRGB(p, q, t float64) float64 {
	if t < 0 {
		t += 1
	}
	if t > 1 {
		t -= 1
	}
	if t < 1.0/6.0 {
		return p + (q-p)*6*t
	}
	if t < 1.0/2.0 {
		return q
	}
	if t < 2.0/3.0 {
		return p + (q-p)*(2.0/3.0-t)*6
	}
	return p
}

func getColor(name string) *color.Color {
	hash := hash(name)
	hi := int8(hash[0])
	si := int8(hash[1])
	li := int8(hash[2])
	h := (float64(hi) / 255.0) * 360
	s := (float64(si)/255.0)/10 + 0.9
	l := (float64(li)/255.0)/10 + 0.75
	r, g, b := HSLToRGB(h, s, l)

	return color.New(color.Underline).AddRGB(r, g, b)
}

func (l *ConsoleLogger) Logf(level LogLevel, format string, args ...interface{}) {
	if l.level > level {
		return
	}
	now := time.Now()
	buf := new(bytes.Buffer)

	TimeColor.Fprintf(buf, "%s ", now.Format("15:04:05.000000"))
	l.nameColor.Fprintf(buf, " %-*s ", 11, l.name)
	switch level {
	case Debug:
		DebugColorTag.Fprintf(buf, " DEBUG ")
		DebugColorText.Fprint(buf, " ")
		DebugColorText.Fprintf(buf, format, args...)
	case Info:
		InfoColorTag.Fprintf(buf, " INFO  ")
		InfoColorText.Fprint(buf, " ")
		InfoColorText.Fprintf(buf, format, args...)
	case Warn:
		WarnColorTag.Fprintf(buf, " WARN  ")
		WarnColorText.Fprint(buf, " ")
		WarnColorText.Fprintf(buf, format, args...)
	case Error:
		ErrorColorTag.Fprintf(buf, " ERROR ")
		ErrorColorText.Fprint(buf, " ")
		ErrorColorText.Fprintf(buf, format, args...)
	case Fatal:
		FatalColorTag.Fprintf(buf, " FATAL ")
		FatalColorText.Fprint(buf, " ")
		FatalColorText.Fprintf(buf, format, args...)
	}
	buf.Write([]byte("\n"))
	output := buf.String()
	l.w.Write([]byte(output))
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
