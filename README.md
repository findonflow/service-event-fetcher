# Spork indexer

## Prepare
 - Download protocolDBArchive for the spork and unpack. NB large files. advice to use lftp in parallel
 - Update to the correct flow-go/cadence versions according to spork json
 - bash crypto_setup.sh
 - go build  --tags relic

## Run
./service-event-fetcher /this/is/badger/db/ startHeight endHeight /this/is/outputdir

