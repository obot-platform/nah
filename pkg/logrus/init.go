package logrus

import (
	"github.com/bombsimon/logrusr/v4"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/sirupsen/logrus"
	klogv2 "k8s.io/klog/v2"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
)

func init() {
	l := logrusr.New(logrus.StandardLogger())
	klogv2.SetLogger(l)
	crlog.SetLogger(l)
	log.SetLogger(logrus.StandardLogger())
}
