# ITCH

ITCH specification http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf 

ITCH files can be downloaded at ftp://emi.nasdaq.com/ITCH

To parse these files from bytes into a readable format the itch parser can be run using the following:

```q itchparsernasdaq.q -init 1 -exit 1 -itchfile 20170728.PSX_ITCH_50 -cutsize 20000 -save 1 -saveto HDB```

This will create tables for each message in a directory named HDB. This file extracts the date for the data from the ITCH file name. If the file name is not in the YYYYMMDD format then the flag `-datefunc` can be used. For example if the filename is saved with MMDDYYYY then the following can is able to extract the date.

```-datefunc "{neg[14h]\$ string[x][4 5 6 7 0 1 2 3]}"```

A book can then be built from these messages by running the following:

```q itchbookbuildernasdaq.q -init 1 -date 2017.07.28 -size 50 -hdb HDB -tablename book ```

This will create the orderbook inside the HDB for a set date. In order to reduce memory the size flag will set the number of stocks to build the orderbook at any one time.

To view the usage statement for either file include the flag `-usage`. 
