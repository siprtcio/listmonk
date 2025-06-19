package logger

import (
	"fmt"
	"log/syslog"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	beego "github.com/beego/beego/v2/server/web"
	logrus_sentry "github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"

	logrus_syslog "github.com/sirupsen/logrus/hooks/syslog"
)

var logLevel = map[string]logrus.Level{
	"panic": logrus.PanicLevel,
	"fatal": logrus.FatalLevel,
	"error": logrus.ErrorLevel,
	"warn":  logrus.WarnLevel,
	"info":  logrus.InfoLevel,
	"debug": logrus.DebugLevel,
	"trace": logrus.TraceLevel,
}

var faciltiyLevel = map[string]syslog.Priority{
	"local0": syslog.LOG_LOCAL0,
	"local1": syslog.LOG_LOCAL1,
	"local2": syslog.LOG_LOCAL2,
	"local3": syslog.LOG_LOCAL3,
}

var logger *logrus.Logger
var contextLogger *logrus.Entry

func InitLogger() {
	var (
		Level    = "debug"
		Facility = "local1"
		Tag      = "faxservice"
		Format   = "json"
		Sentry   = ""
		Syslog   = "127.0.0.1:514"
	)

	logger, err := NewLogger(
		Level,
		Facility,
		Tag,
		Format,
		Sentry,
		Syslog,
	)
	if err != nil {
		return
	}

	hostname, err := os.Hostname()

	if err != nil {
		return
	}

	logging_tag, _ := beego.AppConfig.String("logging_tag")

	contextLogger = logger.WithFields(logrus.Fields{
		"version": "1.0",
		"host":    hostname,
		"tag":     logging_tag,
	})

	if err != nil {
		return
	}
}

func GuardCritical(msg string, err error) {
	if err != nil {
		fmt.Printf("CRITICAL: %s: %v\n", msg, err)
		os.Exit(-1)
	}
}

type LogFields map[string]interface{}

func filefuncName() logrus.Fields {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "?"
		line = 0
	}

	fn := runtime.FuncForPC(pc)
	var fnName string
	if fn == nil {
		fnName = "?()"
	} else {
		dotName := filepath.Ext(fn.Name())
		fnName = strings.TrimLeft(dotName, ".") + "()"
	}

	fileName := fmt.Sprintf("%s:%d", filepath.Base(file), line)

	return logrus.Fields{"file": fileName, "func": fnName}

}

func Panic(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Panic(msg)
}

func Fatal(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Fatal(msg)
}

func Error(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Error(msg)
}

func Warn(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Warn(msg)
}

func Info(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Info(msg)
}

func Debug(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Debug(msg)
}

func Trace(msg string, fields LogFields) {
	contextLogger.WithFields(logrus.Fields(fields)).
		WithFields(filefuncName()).
		Trace(msg)
}

func NewLogger(level string, facility string, tag string, format string, sentry string, syslogAddr string) (*logrus.Logger, error) {
	l := logrus.New()

	fmt.Println("Log level is ", level)
	ll, ok := logLevel[level]
	if !ok {
		fmt.Println("Unsupported loglevel, falling back to debug!")
		ll = logLevel["debug"]
	}
	l.Level = ll

	if sentry != "" {
		hostname, err := os.Hostname()
		GuardCritical("determining hostname failed", err)

		tags := map[string]string{
			"tag":      tag,
			"hostname": hostname,
		}

		sentryLevels := []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		}
		sentHook, err := logrus_sentry.NewWithTagsSentryHook(sentry, tags, sentryLevels)
		GuardCritical("configuring sentry failed", err)

		l.Hooks.Add(sentHook)
	}

	if syslogAddr != "" {
		lf, ok := faciltiyLevel[facility]
		if !ok {
			fmt.Println("Unsupported log facility, falling back to local0")
			lf = faciltiyLevel["local0"]
		}
		sysHook, err := logrus_syslog.NewSyslogHook("udp", syslogAddr, lf, tag)
		if err != nil {
			return l, err
		}
		l.Hooks.Add(sysHook)
		if format == "json" {
			l.SetFormatter(&logrus.JSONFormatter{})
		}
	}
	return l, nil
}
