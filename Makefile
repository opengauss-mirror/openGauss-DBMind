# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
export PYTHONPATH:=$(CURDIR)/3rd:${PYTHONPATH}

PYTHON := $(shell which python3)

ifeq (, $(PYTHON))
  $(error "Not found PYTHON($(PYTHON)) in $(PATH).")
endif

include ./constant
PYTHON_VERSION=$(shell $(PYTHON) -c 'import sys; print("%d.%d"% sys.version_info[0:2])' )
PYTHON_VERSION_OK=$(shell $(PYTHON) -c 'import sys;\
  print(int(eval($(MIN_PYTHON_VERSION)) <= sys.version_info[0:2] <= eval($(MAX_PYTHON_VERSION))))' )

ifeq ($(PYTHON_VERSION_OK),0)
  $(error "Not supported Python version. At least $(MIN_PYTHON_VERSION)")
endif

installer_name := $(shell echo "dbmind-installer-"`uname -m`"-python$(PYTHON_VERSION).sh")
installer_name_without_3rd := "dbmind-installer-without-dependencies.sh"

.PHONY: dbmind ui ui-clean ui-dev ui-lint ui-test check-node

# Detect if NODE_OPTIONS can accept --openssl-legacy-provider
# 1 = allowed, 0 = not allowed (or node not present)
ALLOW_OPENSSL_LEGACY := $(shell sh -c 'node --version >/dev/null 2>&1 || exit 1; NODE_OPTIONS="--openssl-legacy-provider" node -e "process.exit(0)" >/dev/null 2>&1 && echo 1 || echo 0')

default: help

all: ui dbmind

help:
	@echo "Available commands:"
	@echo "  dbmind           - Build DBMind Python backend"
	@echo "  ui               - Build UI components (requires Node.js 16.x)"
	@echo "  ui-clean         - Clean UI build artifacts"
	@echo "  ui-dev           - Start UI development server"
	@echo "  ui-lint          - Run UI linting"
	@echo "  ui-test          - Run UI tests"
	@echo "  check-node       - Check Node.js version compatibility"
	@echo "  test             - Run Python tests"
	@echo "  coverage         - Run tests with coverage"
	@echo "  lint             - Run Python linting"
	@echo "  clean            - Clean all build artifacts"
	@echo "  package          - Create full installation package"
	@echo "  all              - Build both UI and DBMind"
	@echo ""
	@echo "Requirements:"
	@echo "  - Python $(MIN_PYTHON_VERSION)-$(MAX_PYTHON_VERSION)"
	@echo "  - Node.js 16.x (for UI development)"

tox:
	$(PYTHON) -m tox

test:
	$(PYTHON) -m pip install pytest
	$(PYTHON) -m pytest tests

coverage:
	$(PYTHON) -m coverage run -m pytest tests
	$(PYTHON) -m coverage report --omit=3rd*

dbmind:
	@echo "DBMind"

ui: check-node
	@echo "Starting to compile UI components..."
ifeq ($(ALLOW_OPENSSL_LEGACY),1)
	cd ui && NODE_OPTIONS="--openssl-legacy-provider" npm install
	cd ui && NODE_OPTIONS="--openssl-legacy-provider" npm run build
else
	cd ui && npm install
	cd ui && npm run build
endif
	@echo "UI build completed successfully!"

ui-clean:
	@echo "Cleaning UI build artifacts..."
	rm -rf ui/build ui/node_modules
	@echo "UI cleanup completed!"

ui-dev: check-node
	@echo "Starting UI development server..."
ifeq ($(ALLOW_OPENSSL_LEGACY),1)
	cd ui && NODE_OPTIONS="--openssl-legacy-provider" npm install
	cd ui && NODE_OPTIONS="--openssl-legacy-provider" npm start
else
	cd ui && npm install
	cd ui && npm start
endif

ui-lint: check-node
	@echo "Running UI linting..."
	cd ui && npm run lint

ui-test: check-node
	@echo "Running UI tests..."
	cd ui && CI=true npm test

check-node:
	@echo "Checking Node.js version compatibility..."
	@NODE_VERSION=$$(node --version 2>/dev/null | sed 's/v//'); \
	if [ -z "$$NODE_VERSION" ]; then \
		echo "❌ Node.js not found. Please install Node.js 16.x"; \
		echo "   Download Node.js 16.x from: https://nodejs.org/download/release/"; \
		exit 1; \
	fi; \
	NODE_MAJOR=$$(echo $$NODE_VERSION | cut -d. -f1); \
	if [ "$$NODE_MAJOR" -eq 16 ]; then \
		echo "✅ Node.js version $$NODE_VERSION is compatible"; \
	else \
		echo "❌ Node.js version $$NODE_VERSION is not supported"; \
		echo "   Required: Node.js 16.x only"; \
		echo "   Current version: $$NODE_VERSION"; \
		echo "   Please install Node.js 16.x from: https://nodejs.org/download/release/"; \
		exit 1; \
	fi

bins:
	@echo "Starting to generate bins for individual calls"
	$(PYTHON) -m pip install pyinstaller
	pyinstaller dbmind/components/index_advisor/index_advisor_workload.py --onefile --paths dbmind/components/index_advisor --clean --distpath output -n index_advisor


clean:
	find . -type d -name '__pycache__' -exec rm -rf {} +
	rm -rf dbmind-installer*.sh
	rm -rf payload.tar*
	rm -rf build *.spec
	rm -rf 3rd
	rm -rf ui/build ui/node_modules

package: clean ui dbmind 3rd
	@echo "Full packaging..."
	tar --exclude='ui' --exclude='tests' --exclude='Makefile' --exclude='decompress' --exclude='tox.ini' --exclude='node-*' --exclude='miniconda.sh' -cf payload.tar *
	tar --append --file=payload.tar ui/build
	gzip payload.tar
	cat decompress payload.tar.gz > $(installer_name)
	chmod +x $(installer_name)
	@echo Successfully generated DBMind installation package $(installer_name).

package_without_3rd: clean ui dbmind
	@echo "Packaging without third-party dependencies..."
	tar --exclude='ui' --exclude='tests' --exclude='Makefile' --exclude='decompress' --exclude='tox.ini' --exclude='node-*' --exclude='miniconda.sh' -cf payload.tar *
	tar --append --file=payload.tar ui/build
	gzip payload.tar
	cat decompress payload.tar.gz > $(installer_name_without_3rd)
	chmod +x $(installer_name_without_3rd)
	@echo Successfully generated DBMind installation package $(installer_name_without_3rd).

3rd: third_party_basic
	find 3rd -type d -name '__pycache__' -exec rm -rf {} +

third_party_basic:
ifeq ($(shell uname -m | grep x86 | wc -m), 0)
	@echo "Downloading third-party dependencies for DBMind..."
	$(PYTHON) -m pip install -r requirements-aarch64.txt --target=3rd --prefer-binary
else
	@echo "Downloading third-party dependencies for DBMind..."
	$(PYTHON) -m pip install -r requirements-x86.txt --target=3rd --prefer-binary
endif


third_party_total: third_party_basic
	@echo "Downloading more optional dependencies..."
	$(PYTHON) -m pip install -r requirements-optional.txt --target=3rd --prefer-binary

lint:
	@echo "Starting to insepect code..."
	@$(PYTHON) -m flake8 --config flake8.conf --statistics dbmind
