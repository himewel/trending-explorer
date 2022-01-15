SHELL:=/bin/bash

.PHONY: start
start:
	docker-compose up --detach

.PHONY: stop
stop:
	docker-compose stop

.PHONY: test
test:
	docker exec \
		--interactive \
		--tty \
		trending-explorer_airflow-scheduler_1 \
		python3 -m unittest discover --start-directory ./test
