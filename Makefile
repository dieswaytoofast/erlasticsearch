APPLICATION := erlasticsearch

ERL := erl
EPATH := -pa ebin -pz deps/*/ebin
TEST_EPATH := -pa .eunit -pz deps/*/ebin
PLT = .erlastic_search_plt
ERL_LIB_DIR := $(shell if [ -d /usr/lib/erlang/lib ] ; then echo /usr/lib/erlang/lib ; else echo /usr/local/lib/erlang/lib ; fi)
PLT_APPS := $(shell ls $(ERL_LIB_DIR) | grep -v interface | sed -e 's/-[0-9.]*//')
REBAR := ./rebar

.PHONY: all build-plt compile console deps doc clean depclean distclean dialyze release telstart test test-console

all: \
	clean \
	deps \
	compile \
	dialyze \
	test

compile: deps
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

doc:
	@$(REBAR) skip_deps=true doc

clean:
	@$(REBAR) skip_deps=true clean

depclean:
	@$(REBAR) clean

distclean:
	@$(REBAR) delete-deps

build-plt:
	@dialyzer --build_plt --apps kernel stdlib sasl crypto ssl inets tools xmerl runtime_tools compiler syntax_tools mnesia public_key

dialyze: compile
	@dialyzer -r ebin -r deps/proper -r deps/thrift \
		-r deps/poolboy -Wno_undefined_callbacks

test: compile
	@$(REBAR) skip_deps=true ct verbose=1

console:
	$(ERL) -sname $(APPLICATION) $(EPATH) -config app

test-console: test
	$(ERL) -sname $(APPLICATION)_test $(TEST_EPATH) -config app

travis-check-logs:
	test -z "$(shell find apps/*/logs -name '*.log' -exec egrep '(init|end)_per_.* (failed|crashed)' {} +)" && \
	test -z "$(shell find ct_log/*/log -name '*.log' -exec egrep '(init|end)_per_.* (failed|crashed)' {} +)" && \
	test -z "$(shell find logs/* -name '*.log' -exec egrep '(init|end)_per_.* (failed|crashed)' {} +)"
