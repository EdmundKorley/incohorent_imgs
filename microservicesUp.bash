#!/usr/bin/env bash

go run src/kVService.go &
go run src/taskService.go 127.0.0.1:3001 127.0.0.1:3000 &
go run src/storageService.go 127.0.0.1:3002 127.0.0.1:3000 &
go run src/masterService.go 127.0.0.1:3003 127.0.0.1:3000 &
