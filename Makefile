CXXFLAGS = --std=c++17
CFLAGS = -g -Wall -Wno-format-truncation
CFLAGS += -I/TDengine/include -I/usr/include
ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)
CFLAGS += -I"$(ERLANG_PATH)" -Ic_src -fPIC
LIB_NAME = priv/taos_nif.so
LDFLAGS = -L/usr/lib -ltaos

NIF_SRC=\
	c_src/msg.cpp c_src/worker.cpp c_src/taos_nif.cpp

all: $(LIB_NAME)

$(LIB_NAME): $(NIF_SRC)
	mkdir -p priv
	$(CXX) $(CXXFLAGS) $(CFLAGS) -shared $^ $(LDFLAGS) -o $@

clean:
	rm -f $(LIB_NAME)

.PHONY: all clean