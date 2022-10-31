NAME := lakedrive

RUN := poetry run


.PHONY: pre-commit
pre-commit:
	$(RUN) pre-commit run --all-files

.PHONY: docs
docs:
	$(RUN) make -C docs html

.PHONY: .docker_env
.docker_env:
	@$(eval COMPOSE_CMD = $(shell which docker-compose || printf "docker compose"))
	@$(COMPOSE_CMD) version --short 2>/dev/null |grep -cq "^2\..*" \
		|| (echo "docker-compose v2 required"; exit 1)

.PHONY: .docker_up
.docker_up:
	cd docker \
		&& $(COMPOSE_CMD) -p $(NAME) --profile devel up -t0 -d

.PHONY: .docker_build
.docker_build:
	cd docker \
		&& $(COMPOSE_CMD) -p $(NAME) build $(NAME)-tests minio-dev $(NAME)-docs

# (re-)build and (re-)create docker services
# -- used for manual debugging docker parts
.PHONY: docker
docker: .docker_env .docker_build
	cd docker \
		&& $(COMPOSE_CMD) -p $(NAME) --profile devel up --force-recreate -t0 -d

# run make commands via docker (e.g.: make docker/tests)
# compatible targets: tests, coverage
.PHONY: docker/%
docker/%: .docker_env .docker_build .docker_up
	docker exec -t \
		$(NAME)-tests poetry run make -f Makefile-tests $(*) || exit 1

.PHONY: install
install: clean
	poetry build
	python -m pip install --force-reinstall dist/$(NAME)-*.whl

.PHONY: clean
clean:
	[ -d ./dist ] && rm -rf ./dist || exit 0
	[ -d ./.mypy_cache ] && rm -rf ./.mypy_cache || exit 0
	[ -d ./.cache ] && rm -rf ./.cache || exit 0
	[ -d ./build ] && rm -rf ./build || exit 0
	find ./ -type f -name '*.pyc' -delete -o -type d -name __pycache__ -delete
