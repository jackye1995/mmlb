.PHONY: install-java clean-java test-java install-python sync run

# Build Java modules, skipping tests
install-java:
	cd java && ./mvnw clean package -DskipTests

# Clean Java build artifacts
clean-java:
	cd java && ./mvnw clean

# Run Java tests
test-java:
	cd java && ./mvnw test

# Sync Python dependencies with uv
sync:
	uv sync

# Install Python package in development mode with uv
install-python:
	uv sync

# Run mmlb CLI with uv
run:
	uv run mmlb $(ARGS)

# Build everything
all: install-java install-python
