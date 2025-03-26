package log

import (
	"log"
)

var (
	// Stupid log abstraction.  It's expected that the consumer
	// of this library override these methods like
	//
	//  log.Infof = logrus.Infof
	//  log.Errorf = logrus.Errorf
	//
	//  or you can call SetLogger

	Infof = func(message string, obj ...any) {
		//log.Printf("INFO: "+message+"\n", obj...)
	}
	Warnf = func(message string, obj ...any) {
		log.Printf("WARN [NAH]: "+message+"\n", obj...)
	}
	Errorf = func(message string, obj ...any) {
		log.Printf("ERROR[NAH]: "+message+"\n", obj...)
	}
	Fatalf = func(message string, obj ...any) {
		log.Fatalf("FATAL[NAH]: "+message+"\n", obj...)
	}
	Debugf = func(message string, obj ...any) {
		//log.Printf("DEBUG: "+message+"\n", obj...)
	}
)

type Logger interface {
	Infof(message string, obj ...any)
	Warnf(message string, obj ...any)
	Errorf(message string, obj ...any)
	Fatalf(message string, obj ...any)
	Debugf(message string, obj ...any)
}

func SetLogger(logger Logger) {
	Debugf = logger.Debugf
	Infof = logger.Infof
	Warnf = logger.Warnf
	Errorf = logger.Errorf
	Fatalf = logger.Fatalf
}
