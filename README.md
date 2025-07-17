## candyFlow

`candyFlow` is a ....

## Setup

To setup `candyFlow` and it's dependencies, begin by making sure that you have `docker` installed, or any **Linux** release version that contains `apt`, such as `Ubuntu` or `Debian`

We suggest first begin with `docker` before you are familiar with `candyFlow` .

### Docker

make sure you have installed `Docker` and `Docker` is running

#### Windows

```
cd <PATH_TO_REPO>/setup
./start_win.bat
```

#### Linux

```
cd <PATH_TO_REPO>/setup
./start.sh
```

### Linux with apt

check the dependencies in <PATH_TO_REPO>/setup/Dockerfile, and build your env

## candyFlow: examples

run the following commands to generate examples

```
cmake -B build
cmake --build build -j $(nproc)
```
This will generate the examples in the `build/bin` directory.
You can run the examples with:

```
./build/bin/itopk
```