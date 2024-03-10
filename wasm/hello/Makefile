

#zig build-exe -O ReleaseSmall -target wasm32-wasi -femit-bin=./hello.wasm ./hello.zig
build:
	zig build-lib hello.zig -target wasm32-freestanding -dynamic -femit-bin=hello.wasm

# zig build-lib -dynamic -target wasm32-freestanding hello.zig
# zig build-lib hello.zig -target wasm32-freestanding -dynamic -femit-bin=hello.wasm
run:
	zig build-lib hello.zig -target wasm32-freestanding -dynamic -rdynamic
	go run main.go
