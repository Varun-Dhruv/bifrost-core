.PHONY: install start-core deps clean deploy

install:
	@echo "Installing dependencies..."
	@pip install -r src/requirements.txt

start-core:
	@echo "Starting core..."
	@docker compose --profile dev up -d --build
deps:
	@echo "generating dependencies..."
	@pip-compile -v src/requirements.in

deploy:
	@okteto context use "https://cloud.okteto.com"
	@okteto deploy -n varun-dhruv --build

clean:
