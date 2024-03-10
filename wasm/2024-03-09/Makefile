

#zig build-obj actor.zig -O ReleaseSafe -target wasm32-unknown-unknown --emit=wasm --output=actor.wasm
#zig build-obj ./zig/store.zig -O ReleaseSafe -target wasm32-unknown-unknown --emit=wasm --output=./wasm/store.wasm
#zig build-exe ./zig/main.zig -O ReleaseSafe -target wasm32-wasi 
#zig build-exe -O ReleaseSmall -target wasm32-wasi ./zig/main.zig 
#zig build-exe -O ReleaseSmall -target wasm32-wasi --output ./wasm/output.wasm ./zig/main.zig
# zig build-lib ./zig/actor.zig -target wasm32-wasi --name actor 
# doesn't work well
#zig build-exe -O ReleaseSmall -target wasm32-wasi -femit-bin=./wasm/store.wasm ./zig/store.zig
#zig build-exe -O ReleaseSmall -target wasm32-wasi -femit-bin=./wasm/actor.wasm ./zig/actor.zig
build:
	mkdir -p ./wasm
	zig build-lib ./zig/store.zig -target wasm32-freestanding -dynamic -rdynamic
	zig build-lib ./zig/actor.zig -target wasm32-freestanding -dynamic -rdynamic


run:
	go run main.go
