#!/usr/bin/env bash

go run kVService.go &
go run taskService.go 127.0.0.1:3001 127.0.0.1:3000 &
go run storageService.go 127.0.0.1:3002 127.0.0.1:3000 &
go run masterService.go 127.0.0.1:3003 127.0.0.1:3000 &
