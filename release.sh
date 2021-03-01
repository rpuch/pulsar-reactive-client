#!/bin/sh

./mvnw release:prepare --batch-mode -Prelease && ./mvnw release:perform -Prelease
