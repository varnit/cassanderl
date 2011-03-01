PREFIX:=../
DEST:=$(PREFIX)$(PROJECT)

REBAR=./rebar

all:
	@$(REBAR) get-deps compile

clean:
	@$(REBAR) clean