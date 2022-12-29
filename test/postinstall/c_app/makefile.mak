PROGRAM = c_app
OBJECTS = $(addsuffix .o,$(PROGRAM))
TESTS = \
  test_libpath.sh \
  test_transform.sh \
  test_searchpath.sh \
  test_version.sh

override CFLAGS += -g -Wall -Werror $(shell ${PKG_CONFIG} proj --cflags)

ifeq ($(BUILD_MODE),static)
  UNAME_S := $(shell uname -s)
  _ldflags := $(shell ${PKG_CONFIG} proj --libs --static)
  ifeq ($(UNAME_S),Linux)
    # force static linking to libproj
    _ldflags := $(shell echo $(_ldflags) | sed 's/-lproj/-Wl,-Bstatic -lproj -Wl,-Bdynamic/')
  endif
  override LDFLAGS += $(_ldflags)
else  # default is shared
  override LDFLAGS += $(shell ${PKG_CONFIG} proj --libs)
endif

all: $(PROGRAM)

$(PROGRAM): $(OBJECTS)
	$(CC) -o $@ $< $(LDFLAGS)

test: $(PROGRAM)
	set -e; for t in $(TESTS); do ./$$t; done

clean:
	$(RM) $(PROGRAM) $(OBJECTS)

.PHONY: test clean
