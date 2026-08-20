CREATE TABLE "flow_records" (
    "id" UUID DEFAULT uuidv7() PRIMARY KEY,
    "src_ip" INET NOT NULL,
    "dst_ip" INET NOT NULL,
    "src_port" INT NOT NULL,
    "dst_port" INT NOT NULL,
    "protocol" SMALLINT NOT NULL,
    "byte_count" BIGINT NOT NULL,
    "packet_count" BIGINT NOT NULL,
    "first_seen" TIMESTAMPTZ NOT NULL,
    "last_seen" TIMESTAMPTZ NOT NULL,
    UNIQUE (src_ip, dst_ip, src_port, dst_port, protocol, first_seen)
);
CREATE INDEX ON "flow_records" ("src_ip");
CREATE INDEX ON "flow_records" ("dst_ip");
CREATE INDEX ON "flow_records" ("first_seen", "byte_count");