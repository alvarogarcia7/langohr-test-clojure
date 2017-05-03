
.PHONY: rabbit
rabbit:
	docker run -d -p 5672:5672 --hostname my-rabbit --name some-rabbit rabbitmq:3
