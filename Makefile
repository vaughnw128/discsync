p# Simple Makefile for common tasks

.PHONY: setup-debian build run

setup-debian:
	@echo "[setup] Installing Debian/Ubuntu prerequisites..."
	sudo apt update && sudo apt install -y build-essential pkg-config libsqlite3-dev curl git
	@echo "[setup] Done. Optional: install ONNX Runtime and set ORT_DYLIB_PATH (see README)."

build:
	cargo build --release

# Example run with env vars already set in your shell
run:
	./target/release/sbux

