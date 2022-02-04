# mcfly

**NOTE: this is a partially-working proof of concept**

mcfly is a tool for rewinding MySQL changes via the binlog


### Requirements

- MySQL with binlog/`log-bin` enabled
- `binlog_row_image` set to `FULL`
