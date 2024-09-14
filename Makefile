IMAGE_REGISTRY := localhost:5001
k8s_context := kind-kind

.PHONY: prepare test build dockerize set_kind_context install_git_hooks release deploy teardown

install_git_hooks:
	@echo "Installing git hooks"
	cp ./hooks/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Git hooks installed successfully! Use git add and git commit to commit your changes"

set_kind_context:
	kubectl config use-context ${k8s_context}

prepare: set_kind_context
	@if [ -z "$(PACKAGE)" ]; then \
        echo "Error: PACKAGE variable is not set"; \
		echo "Usage: make prepare PACKAGE=<package_name>"; \
		echo "Example: make prepare PACKAGE=core"; \
        exit 1; \
    fi
	@echo "Preparing $(PACKAGE) package"
	cargo fmt && cargo clippy && cargo check

build: prepare
	cargo build --package $(PACKAGE)

test: prepare
	export RUST_LOG=DEBUG
	cargo test --package $(PACKAGE)

test_module:
	export RUST_LOG=DEBUG
	cargo test $(MODULE) -- --nocapture

release: test
	cargo build --release --package $(PACKAGE)

dockerize: set_kind_context
	@echo "INFO: Building $(PACKAGE) package"
	
	docker build --tag ${IMAGE_REGISTRY}/walrs_$(PACKAGE):latest -f $(PACKAGE)/Dockerfile .
	docker push ${IMAGE_REGISTRY}/walrs_$(PACKAGE):latest
	
	@echo "INFO: $(PACKAGE) built successfully!"
	docker images

deploy: dockerize
	@echo "INFO: Deploying $(PACKAGE) package"
	kubectl apply -f k8s/prerequisites.yml
	kubectl apply -f k8s/${PACKAGE}.yml

teardown: set_kind_context
	@echo "INFO: Undeploying $(PACKAGE) package"
	kubectl delete -f k8s/${PACKAGE}.yml
	kubectl delete -f k8s/prerequisites.yml
