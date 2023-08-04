# s3-migration - a CLI for migrating AWS S3 bucket to Züs dStorage 


s3-migration is a command line interface (CLI) tool that helps migrate files from S3 bucket to Züs dStorage.

- [Züs Overview](#züs-overview)
- [Building s3 migration](#building-s3-migration)
- [Running s3 migration](#running-s3-migration)
- [Global Flags](#global-flags)
- [Migration Commands](#migrate-command)
- [Running Test Cases](#running-test-cases)
  
## Züs Overview
[Züs](https://zus.network/) is a high-performance cloud on a fast blockchain offering privacy and configurable uptime. It is an alternative to traditional cloud S3 and has shown better performance on a test network due to its parallel data architecture. The technology uses erasure code to distribute the data between data and parity servers. Züs storage is configurable to provide flexibility for IT managers to design for desired security and uptime, and can design a hybrid or a multi-cloud architecture with a few clicks using [Blimp's](https://blimp.software/) workflow, and can change redundancy and providers on the fly.

For instance, the user can start with 10 data and 5 parity providers and select where they are located globally, and later decide to add a provider on-the-fly to increase resilience, performance, or switch to a lower cost provider.

Users can also add their own servers to the network to operate in a hybrid cloud architecture. Such flexibility allows the user to improve their regulatory, content distribution, and security requirements with a true multi-cloud architecture. Users can also construct a private cloud with all of their own servers rented across the globe to have a better content distribution, highly available network, higher performance, and lower cost.

[The QoS protocol](https://medium.com/0chain/qos-protocol-weekly-debrief-april-12-2023-44524924381f) is time-based where the blockchain challenges a provider on a file that the provider must respond within a certain time based on its size to pass. This forces the provider to have a good server and data center performance to earn rewards and income.

The [privacy protocol](https://zus.network/build) from Züs is unique where a user can easily share their encrypted data with their business partners, friends, and family through a proxy key sharing protocol, where the key is given to the providers, and they re-encrypt the data using the proxy key so that only the recipient can decrypt it with their private key.

Züs has ecosystem apps to encourage traditional storage consumption such as [Blimp](https://blimp.software/), a S3 server and cloud migration platform, and [Vult](https://vult.network/), a personal cloud app to store encrypted data and share privately with friends and family, and [Chalk](https://chalk.software/), a zero upfront cost permanent storage solution for NFT artists.

Other apps are [Bolt](https://bolt.holdings/), a wallet that is very secure with air-gapped 2FA split-key protocol to prevent hacks from compromising your digital assets, and it enables you to stake and earn from the storage providers; [Atlus](https://atlus.cloud/), a blockchain explorer and [Chimney](https://demo.chimney.software/), which allows anyone to join the network and earn using their server or by just renting one, with no prior knowledge required.

## Building s3-migration

Prerequisites: [Go](https://go.dev/doc/install) 
```
git clone https://github.com/0chain/s3-migration.git
cd s3-migration
go build .
```
## Running s3-migration

When you run the `./s3migration` command in terminal with no arguments inside the s3-migration directory, it will list all the available commands and the global flags.

**Command**|**Description**
:-----:|:-----:
[help](#help)|Help about any command
[migrate](#migrate)|Migrate user data from S3 bucket to dStorage

### Global Flags

Global Flags are parameters in s3igration that can be used with any command to override the default configuration supports the following global parameters.

| Flags                      | Description                                                  | Usage                                             |
| -------------------------- | ------------------------------------------------------------ | ------------------------------------------------- |
| --config string            | Specify configuration file (default is [$HOME/.zcn/config.yaml](https://github.com/0chain/zboxcli/blob/staging/network/config.yaml)) | ./s3migration migrate --config config1.yaml              |
| --configDir string         | Specify a configuration directory (default is $HOME/.zcn) | ./s3migration migrate --configDir /$HOME/.zcn2           |
| -h, --help                 | Gives more information about a particular command.           | ./s3migration migrate --help                             |
| --network string           | Specify a network file to overwrite the network details(default is [$HOME/.zcn/network.yaml](https://github.com/0chain/zwalletcli#override-network)) | s3migration migrate --network network1.yaml            |
| --wallet string            | Specify a wallet file or 2nd wallet (default is $HOME/.zcn/wallet.json) | ./s3migration migrate --wallet wallet2.json              |
| --wallet_client_id string  | Specify client id. If wallet key is also provided then wallet_private_key is required | ./s3migration migrate --wallet_client_id "wallet client id" |
| --wallet_client_key string | Specify a wallet client_key (By default client_key specified in $HOME/.zcn/wallet.json is used) | ./s3migration migrate --wallet_client_key  "client key" |
| --wallet_private_key string | Specify wallet private key| ./s3migration migrate --wallet_private_key "wallet private key"|

## Migrate Command

`migrate` command is used to migrate files from s3 buckets to some remote directory(default is /) by using aws-s3-sdk and 0chain gosdk. All the objects from the bucket will be migrated.
However users can specify some prefix to migrate only the files with those prefix. Also if there is name conflict within dStorage file and bucket file use can
specify whether to skip, replace or duplicate them. Migration state is maintained in some file so user can also resume migration operation if some error had
occurred in previous migration session. User can also specify whether to delete migrated file. Note the defaults.

	Note: Addition of new object or modification of existing file while migrating is not recommended, as it cannot track such changes and you might lose your data.

| Flags          | Required | Description                                               | Default        | Valid Values |
|--------------------|----------|-----------------------------------------------------------|----------------|--------------|
| access-key | yes if aws-cred-path not provided | access-key of aws               |  | string    |
| secret-key | yes if aws-cred-path not provided | secret-key of aws               |  | string    |
| aws-cred-path | yes if access/ secret key not provided | File Path to aws credentials                |  | file path    |
| allocation | yes if alloc-path not provided | allocation ID for dStorage (required if alloc path is not provided)               |  | string    |
| alloc-path | yes if allocation not provided | File Path to allocation text                |  | file path    |
| bucket | yes | Bucket to migrate                |  | string    |
| concurrency |  | number of concurrent files to process concurrently during migration                | 10 | int    |
| delete-source |  | Delete object in s3 that is migrated to dStorage               | false | boolean    |
| dup-suffix |  | Duplicate suffix to use for migrated file               | _copy | string    |
| encrypt |  | pass this option to encrypt and upload the file               | false | boolean    |
| migrate-to |  | Remote path where buckets will be migrated to               | / | string    |
| owner-pays |  | Read payment source(Default: owner pays               | true | boolean    |
| newer-than |  | eg; 7d10h --> migrate objects that is newer than 7 days 10 hours               |  | string    |
| older-than |  | eg; 7d10h --> migrate objects that is newer than 7 days 10 hours               |  | string    |
| prefix |  | Migrate objects starting with this prefix               |  | string    |
| region |  | AWS S3 Bucket location               | us-east-2 | string    |
| resume |  | pass this option to resume migration from previous state               | false | boolean    |
| skip |  | 0 --> Replace existing files; 1 --> Skip migration; 2 --> Duplicate               | 1 | int    |
| wd |  | Working directory               | $HOME/.s3migration | string    |

Sample Command:
```
./s3migration migrate --allocation $ALLOC --access-key $AWS_ACCESS_KEY --secret-key $AWS_SECRET_KEY --bucket s3migfiles
```
Note: The bucket will be listed as directory in dStorage.

## Running Test cases

`go test ./...`

## BenchMark

**Data Shard**|**Parity**|**Upload Size**|**File count**|**Time Taken**|**Network Speed**
:----------:|:---------:|:-----:|:-----:|:------:|:------:
4|2|20 gb|199|1 hr 25min|15 MBps








