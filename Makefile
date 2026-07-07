GOLANGCI_LINT_VERSION ?= v2.12.2
setup-ci-env:
	if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Could not find golangci-lint, installing version $(GOLANGCI_LINT_VERSION)."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	fi

validate-ci:
	go generate
	go mod tidy
	if [ -n "$$(git status --porcelain --untracked-files=no)" ]; then \
		git status --porcelain --untracked-files=no; \
		echo "Encountered dirty repo!"; \
		exit 1 \
	;fi

validate:
	golangci-lint run

test:
	go test ./...
