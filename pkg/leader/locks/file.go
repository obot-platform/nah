package locks

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type lock struct {
	id    string
	file  string
	lease *resourcelock.LeaderElectionRecord
}

func NewFile(id, file string) resourcelock.Interface {
	return &lock{
		id:   id,
		file: file,
	}
}

func (l *lock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	file, err := os.OpenFile(l.file, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// The leader election only understands Kubernetes not-found errors.
			return nil, nil, apierrors.NewNotFound(schema.GroupResource{}, l.file)
		}
		return nil, nil, err
	}
	defer file.Close()

	byteData, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	var ler resourcelock.LeaderElectionRecord
	if err = json.Unmarshal(byteData, &ler); err != nil {
		return nil, nil, err
	}

	l.lease = &ler

	return &ler, byteData, nil
}

func (l *lock) Create(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.lease = new(resourcelock.LeaderElectionRecord)
	if err := l.writeToFile(ler, os.O_CREATE|os.O_EXCL|os.O_WRONLY); err != nil {
		l.lease = nil
		return err
	}

	return nil
}

func (l *lock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	if l.lease == nil {
		return errors.New("lease not initialized, call get or create first")
	}

	return l.writeToFile(ler, os.O_WRONLY|os.O_TRUNC)
}

func (l *lock) writeToFile(ler resourcelock.LeaderElectionRecord, flag int) error {
	file, err := os.OpenFile(l.file, flag, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err = json.NewEncoder(file).Encode(ler); err != nil {
		return err
	}

	*l.lease = ler

	return nil
}

// RecordEvent doesn't record events.
func (l *lock) RecordEvent(string) {}

func (l *lock) Identity() string {
	return l.id
}

func (l *lock) Describe() string {
	return l.file
}
