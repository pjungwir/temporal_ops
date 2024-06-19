EXTENSION = temporal_ops
EXTENSION_VERSION = 1.0.0
DATA = $(EXTENSION)--$(EXTENSION_VERSION).sql

REGRESS = setup \
					semijoin \
					antijoin

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
