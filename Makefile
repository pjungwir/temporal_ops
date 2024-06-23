EXTENSION = temporal_ops
EXTENSION_VERSION = 1.0.0
DATA = $(EXTENSION)--$(EXTENSION_VERSION).sql

REGRESS = setup \
					semijoin \
					antijoin

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
REGRESS_OPTS = --dbname=$(EXTENSION)_regression # This must come *after* the include since we override the build-in --dbname.

test:
	echo "Run make installcheck to run tests"
	exit 1

README.html: README.md
	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

release:
	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master

.PHONY: test release
