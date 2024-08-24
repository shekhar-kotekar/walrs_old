export CONFIG_FILE_PATH := config.toml

IMAGE_REGISTRY := localhost:5001
k8s_context := kind-kind

.PHONY: prepare test build dockerize set_kind_context install_git_hooks

install_git_hooks:
	@echo "Installing git hooks"
	cp hooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit

set_kind_context:
	kubectl config use-context ${k8s_context}

prepare: set_kind_context
	@if [ -z "$(PACKAGE)" ]; then \
        echo "Error: PACKAGE variable is not set"; \
        exit 1; \
    fi
	@echo "Preparing $(PACKAGE) package"
	cargo fmt && cargo clippy && cargo check

build: prepare
	cargo build --package $(PACKAGE)

test: prepare
	cargo test --package $(PACKAGE)

release: test
	cargo build --release --package $(PACKAGE)

dockerize:
	docker build -t walrs_core:latest .
	