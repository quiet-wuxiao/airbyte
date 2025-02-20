# End-to-End Testing Destination

This destination is for testing of Airbyte connections. It can be set up as a source message logger, a `/dev/null`, or to mimic specific behaviors (e.g. exception during the sync). Please use it with discretion. This destination may log your data, and expose sensitive information.

## Features

| Feature | Supported  | Notes |
| :--- | :--- | :--- |
| Full Refresh Sync | Yes | |
| Incremental Sync | Yes | |
| Replicate Incremental Deletes | No | |
| SSL connection | No | |
| SSH Tunnel Support | No | |

## Mode

### Silent (`/dev/null`)

**This is the only mode allowed on Airbyte Cloud.**

This mode works as `/dev/null`. It does nothing about any data from the source connector. This is usually only useful for performance testing of the source connector.

### Logging

This mode logs the data from the source connector. It will log at most 1,000 data entries.

There are the different logging modes to choose from:

| Mode | Notes | Parameters |
| :--- | :--- | :--- |
| First N entries  | Log the first N number of data entries for each data stream. | N: how many entries to log. |
| Every N-th entry | Log every N-th entry for each data stream. When N=1, it will log every entry. When N=2, it will log every other entry. Etc. | N: the N-th entry to log. Max entry count: max number of entries to log. |
| Random sampling | Log a random percentage of the entries for each data stream. | Sampling ratio: a number in range of `[0, 1]`. Optional seed: default to system epoch time. Max entry count: max number of entries to log. |

### Throttling

This mode mimics a slow data sync. You can specify the time (in millisecond) of delay between each message from the source is processed.

### Failing

This mode throws an exception after receiving a configurable number of messages.

## CHANGELOG

### OSS (E2E Testing Destination)

| Version | Date       | Pull Request                                             | Subject |
| :------ | :--------- | :------------------------------------------------------- | :--- |
| 0.2.1   | 2021-12-19 | [\#8824](https://github.com/airbytehq/airbyte/pull/8905) | Fix documentation URL. |
| 0.2.0   | 2021-12-16 | [\#8824](https://github.com/airbytehq/airbyte/pull/8824) | Add multiple logging modes. |
| 0.1.0   | 2021-05-25 | [\#3290](https://github.com/airbytehq/airbyte/pull/3290) | Create initial version. |

### Cloud (E2E Testing (`/dev/null`) Destination)

| Version | Date       | Pull Request                                             | Subject |
| :------ | :--------- | :------------------------------------------------------- | :--- |
| 0.1.1   | 2021-12-19 | [\#8824](https://github.com/airbytehq/airbyte/pull/8905) | Fix documentation URL. |
| 0.1.0   | 2021-12-16 | [\#8824](https://github.com/airbytehq/airbyte/pull/8824) | Create initial version. |
