#!/usr/bin/env bash
linterVersion=v1.23.8
echo "INSTALLING: Linter"
echo ""
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s $linterVersion