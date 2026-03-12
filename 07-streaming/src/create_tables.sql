CREATE TABLE processed_events_aggregated (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);

CREATE TABLE processed_events_sessioned (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID)
);

CREATE TABLE processed_events_tumbling (
    window_start TIMESTAMP,
    tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);