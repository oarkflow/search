SHELL := /bin/bash
.PHONY: setup build install build-binary db-up db-down migrate migrate-rollback

SOURCE := .
BRANCH := main
ifneq ($(B),)
	BRANCH = $(B)
endif
# CHECKOUT := $(shell cd $(SOURCE) && git stash && git checkout $(BRANCH))
TAG_COMMIT := $(shell cd $(SOURCE) && git rev-list --abbrev-commit --tags --max-count=1)
TAG := $(shell cd $(SOURCE) && git describe --abbrev=0 --tags ${TAG_COMMIT} 2>/dev/null || true)
COMMIT := $(shell cd $(SOURCE) && git rev-parse --short HEAD)
DATE := $(shell cd $(SOURCE) && git log -1 --format=%cd --date=format:"%Y%m%d%H%M%S")
VERSION := $(DATE).$(COMMIT)
BLACK        := $(shell tput -Txterm setaf 0)
RED          := $(shell tput -Txterm setaf 1)
GREEN        := $(shell tput -Txterm setaf 2)
YELLOW       := $(shell tput -Txterm setaf 3)
LIGHTPURPLE  := $(shell tput -Txterm setaf 4)
PURPLE       := $(shell tput -Txterm setaf 5)
BLUE         := $(shell tput -Txterm setaf 6)
WHITE        := $(shell tput -Txterm setaf 7)
RESET := $(shell tput -Txterm sgr0)
AppType := worker

PWD := $(shell pwd)
BUILD_PATH := $(PWD)/build
RELEASE_PATH := $(BUILD_PATH)/releases

CURRENT_RELEASE := "dev"
PREVIOUS_RELEASE := "dev"

APPLICATION_NAME := app

ifneq ($(shell git status --porcelain),)
	VERSION := $(VERSION).wip
endif

ifneq ($(TAG:v%=%),)
	VERSION := v$(TAG:v%=%).$(VERSION)
endif

ifneq ($(APP_TYPE),)
	AppType := $(APP_TYPE)
endif
module := $(shell grep "module " go.mod)
moduleName := $(subst module ,,$(module))/config.AppType

FLAGS := -ldflags "-s -w -X 'main.Version=$(VERSION)' -X 'main.AppName=$(APPLICATION_NAME)' -X '$(moduleName)=$(AppType)'"
ifneq ("$(wildcard $(CURRENT_PATH)/CURRENT-RELEASE)","")
    CURRENT_RELEASE := $(shell cat $(CURRENT_PATH)/CURRENT-RELEASE)
endif
ifneq ("$(wildcard $(CURRENT_PATH)/PREVIOUS-RELEASE)","")
    PREVIOUS_RELEASE := $(shell cat $(CURRENT_PATH)/PREVIOUS-RELEASE)
endif

ROLLBACK_RELEASE := $(RELEASE_PATH)/$(RELEASE_TAG)

LATEST_RELEASE := $(APPLICATION_NAME)-$(VERSION)
LATEST_RELEASE_ZIP := $(APPLICATION_NAME)-$(VERSION).zip
LATEST_RELEASE_PATH := $(RELEASE_PATH)/$(LATEST_RELEASE)

GOARCH		= amd64
GOOSWIN		= windows
GOOSX		= darwin
GOOSLINUX	= linux
GOMOD		= on
CGO_ENABLED 	= 0

WINBIN 		= $(LATEST_RELEASE_PATH)/$(APPLICATION_NAME)-win-$(GOARCH).exe
OSXBIN 		= $(LATEST_RELEASE_PATH)/$(APPLICATION_NAME)-darwin-$(GOARCH)
LINUXBIN 	= $(LATEST_RELEASE_PATH)/$(APPLICATION_NAME)-linux-$(GOARCH)

CC 		= go build
CFLAGS		= -trimpath
LDFLAGS		= all=-w -s
GCFLAGS 	= all=
ASMFLAGS 	= all=

.PHONY: build-binaries
build-binaries: darwin linux

.PHONY: darwin
darwin: $(OSXBIN)

.PHONY: $(OSXBIN)
$(OSXBIN):
	$(info $(YELLOW)Building Darwin amd64... $(RESET))
	$(shell GO111MODULE=$(GOMOD) GOARCH=$(GOARCH) GOOS=$(GOOSX) CGO_ENABLED=$(CGO_ENABLED) $(CC) $(CFLAGS) -o $(OSXBIN) -ldflags="$(LDFLAGS)" -gcflags="$(GCFLAGS)" -asmflags="$(ASMFLAGS)")

.PHONY: linux
linux: $(LINUXBIN)

.PHONY: $(LINUXBIN)
$(LINUXBIN):
	$(info $(YELLOW)Building Linux amd64... $(RESET))
	$(shell GO111MODULE=$(GOMOD) GOARCH=$(GOARCH) GOOS=$(GOOSLINUX) CGO_ENABLED=$(CGO_ENABLED) $(CC) $(CFLAGS) -o $(LINUXBIN) -ldflags="$(LDFLAGS)" -gcflags="$(GCFLAGS)" -asmflags="$(ASMFLAGS)")

build: build-binaries

setup: install db-up migrate

install:
	$(info $(YELLOW)Installing dependencies... $(RESET))
	$(shell go install github.com/cosmtrek/air@latest)
	$(shell go install github.com/swaggo/swag/cmd/swag@latest)
	$(shell rm -rf vendor)
	$(shell go mod tidy)

swag:
	swag fmt
	swag init --md ./docs --parseInternal --parseDependency --parseDepth 2 -g main.go

db-up:
	docker compose up -d --force-recreate

db-down:
	docker compose down -v

cp-env:
	$(info $(YELLOW)Copying env... $(RESET))
	$(shell cp $(SOURCE)/.env.make $(LATEST_RELEASE_PATH)/.env)

cp-config:
	$(info $(YELLOW)Copying config from storage/config... $(RESET))
	$(shell mkdir -p $(LATEST_RELEASE_PATH)/storage && cp -r $(SOURCE)/storage/config $(LATEST_RELEASE_PATH)/storage/)

cp-migration:
	$(info $(YELLOW)Copying config from database/migrations... $(RESET))
	$(shell mkdir -p $(LATEST_RELEASE_PATH)/database && cp -r $(SOURCE)/database/migrations $(LATEST_RELEASE_PATH)/database/)

cp-views:
	$(info $(YELLOW)Copying config from resources... $(RESET))
	$(shell cp -r $(SOURCE)/resources $(LATEST_RELEASE_PATH)/)

cp-assets:
	$(info $(YELLOW)Copying config from assets... $(RESET))
	$(shell cp -r $(SOURCE)/dist $(LATEST_RELEASE_PATH)/)

migrate:
	go run . artisan migrate

migrate-rollback:
	go run . artisan migrate:rollback

install-alignment:
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest
	cp $(echo $GOPATH)/bin/fieldalignment $(echo $GOPATH)/bin/sizeof

fix-size:
	find . -type f -name '*.go' -print -exec sh -c 'sizeof --fix {}' \;
