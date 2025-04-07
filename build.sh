#!/usr/bin/bash

flags=(
	"-ggdb"
	"-Og"
)

if [ $1 == "-asan" ]; then
	flags+=("-fsanitize=address")
fi
shift
gcc $@ ${flags[@]} -o async_io.exe -lglib-2.0 -I /usr/include/glib-2.0 -I /usr/lib/x86_64-linux-gnu/glib-2.0/include -lrt
