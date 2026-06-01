.PHONY: test test-watch

test:
	go test ./...

test-watch:
	@command -v watchexec >/dev/null 2>&1 || (echo "watchexec is required. Install with: brew install watchexec" && exit 1)
	watchexec -e go -- "go test ./..."
