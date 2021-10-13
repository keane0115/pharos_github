build:
	GOOS=linux CGO_ENABLED=0 go build -o target/main main.go
clean:
	rm -rf target/*

build-image: build
	docker build .	--tag pharos

deploy:
	kubectl apply -f Deployment.yaml