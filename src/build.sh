#!/bin/bash

ANTLR=${ANTLR_CMD:-antlr4}
GRAMMAR_DIR=gobapi/graphql_streaming/graphql2sql/grammar

${ANTLR} -Dlanguage=Python3 ${GRAMMAR_DIR}/GraphQL.g4 -visitor -no-listener