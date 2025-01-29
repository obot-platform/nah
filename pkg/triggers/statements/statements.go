package statements

import (
	"bytes"
	"embed"
	"strings"
	"sync"
)

//go:embed *.sql
var fs embed.FS

type Statements struct {
	lock             sync.RWMutex
	statements       map[string]string
	triggersSpecific map[string]map[string]string
}

func New(handlerName string) *Statements {
	handlerName = strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(handlerName), "-", "_"), " ", "_")
	s := &Statements{
		statements:       map[string]string{},
		triggersSpecific: map[string]map[string]string{},
	}
	entries, err := fs.ReadDir(".")
	if err != nil {
		panic("failed to read sql files: " + err.Error())
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sql, err := fs.ReadFile(entry.Name())
		if err != nil {
			panic("failed to read sql file: " + err.Error())
		}
		s.statements[entry.Name()] = strings.ReplaceAll(string(bytes.TrimSpace(sql)), "handler_name", handlerName)
	}

	return s
}

func (s *Statements) CreateRevisionsTable() string {
	return s.statements["create_revisions_table.sql"]
}

func (s *Statements) UpdateLatestRevision() string {
	return s.statements["update_latest_revision.sql"]
}

func (s *Statements) GetLatestRevision() string {
	return s.statements["get_latest_revision.sql"]
}

func (s *Statements) CreateMatchersTable(kind string) string {
	lowerKind := strings.ToLower(kind) + "_"
	s.lock.RLock()
	_, ok := s.triggersSpecific[kind]
	s.lock.RUnlock()
	if ok {
		return ""
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok = s.triggersSpecific[kind]
	if ok {
		return ""
	}

	s.triggersSpecific[kind] = make(map[string]string)

	s.triggersSpecific[kind]["create_matchers_table.sql"] = strings.ReplaceAll(s.statements["create_matchers_table.sql"], "placeholder_", lowerKind)
	s.triggersSpecific[kind]["clear_old_matchers.sql"] = strings.ReplaceAll(s.statements["clear_old_matchers.sql"], "placeholder_", lowerKind)
	s.triggersSpecific[kind]["get_matchers.sql"] = strings.ReplaceAll(s.statements["get_matchers.sql"], "placeholder_", lowerKind)
	s.triggersSpecific[kind]["insert_matcher.sql"] = strings.ReplaceAll(s.statements["insert_matcher.sql"], "placeholder_", lowerKind)
	s.triggersSpecific[kind]["delete_matchers.sql"] = strings.ReplaceAll(s.statements["delete_matchers.sql"], "placeholder_", lowerKind)
	s.triggersSpecific[kind]["delete_exact_matchers.sql"] = strings.ReplaceAll(s.statements["delete_exact_matchers.sql"], "placeholder_", lowerKind)
	return s.triggersSpecific[kind]["create_matchers_table.sql"]
}

func (s *Statements) GetMatchers(kind string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.triggersSpecific[kind]["get_matchers.sql"]
}

func (s *Statements) InsertMatcher(kind string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.triggersSpecific[kind]["insert_matcher.sql"]
}

func (s *Statements) ClearOldMatchers(kind string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.triggersSpecific[kind]["clear_old_matchers.sql"]
}

func (s *Statements) DeleteMatchers() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	states := make([]string, 0, len(s.triggersSpecific))
	for kind := range s.triggersSpecific {
		states = append(states, s.triggersSpecific[kind]["delete_matchers.sql"])
	}
	return states
}

func (s *Statements) DeleteExactMatchers(kind string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.triggersSpecific[kind]["delete_exact_matchers.sql"]
}
