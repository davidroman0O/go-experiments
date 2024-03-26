build:
	elm make src/Main.elm --output=main.js
	tailwindcss -i tainwind.css -o style.css --minify
	go build -o ./bin/web .

css:
	tailwindcss -i tainwind.css -o output.css --minify --watch

dev:
	elm reactor

