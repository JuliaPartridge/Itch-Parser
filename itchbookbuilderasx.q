/############################### User inputs ###############################
p:.Q.def[`init`date`size`hdb`tablename`stock!(1b;.z.d;100;`HDB;`book;enlist `)].Q.opt .z.x

usage:{-1
  "
  ####################################### ITCH bookbuilder ################################################\n
  This script is used with the tables created by itchparser.q to build an orderbook for a day's trading.   \n
  The sample usage is as follows:                                                                          \n
  q itchbookbuilderasx.q -init 1 -date 2018.03.04 -size 50 -hdb HDBasx -tablename book -stock BB           \n
  init is a boolean which tells q to build and save the orderbook automatically. The dafault value is 1    \n
  date will default to today's date if none is provided                                                    \n
  size is the number of stocks to build the orderbook of at any one time. This is done to prevent memory   \n
  issues. It defaults to 100 stocks which was fast when tested on a machine with 16048 MB of RAM and 8     \n
  cores.                                                                                                   \n
  stock is the list of stock to build the orderbook, the default is all                                    \n
  hdb is the location of the parsed itch files. The orderbook will save in this directory. The default 	   \n
  argument is HDB/							                                                               \n
  tablename is what you wish to call the orderbook within the hdb. The default argument is book	           \n"
  ;exit 0}
if[`usage in key p;usage[]]

/############################### Create pidmapping ###############################
gettables:{[o]
  system"l ",string[o`hdb],"/";                                                                     /Obtain data for in memory table operations
  getinst::distinct select `$instrument,instrumid from futuresd;                                    /Table of each instrument id and corresponding instrument
  instd::![x;{exec instrumid from getinst where instrument = x}each x:distinct exec instrument from getinst]; /Dictionary of each instrument and its ids
  instdr::![exec instrumid from getinst;exec instrument from getinst];                              /Dictionary of instrument id and corresponding instrument 
 }
 
/############################### Building the orderbook ###############################

bookbuild:{[t;act;ref;sd;sz;px]
  t:@[t;sd;,;
    $[act in "EXCe";`orderid`size!(ref;0|0^t[sd][ref;`size]-sz);                                    /If the action is exec remove the correct number of shares, else preform an upsert.
      `orderid`size`price!(ref;sz;px)]];
  if[0=t[sd][ref;`size];                                                                            /If the number of shares in an order is zero, delete it. This takes care if the case
    t:@[t;sd;_;ref]]; 
  t
 };                                                                                                 /when the action is delete.

getdatapieces:{[ijid](                                                                              /Since there is no stock to select, use ijid -based on symids above- to select by stock.
  select seqno,timestamp,instrumid,side,orderid,size:0 from odelete where instrumid in ijid;
  select seqno,timestamp,instrumid,side,orderid,size from oexecuted where instrumid in ijid;        /List of tables which will be assigned actions based on which table they came from  
  select seqno,timestamp,instrumid,side,orderid,size from ocancel where instrumid in ijid;          /Tables later used in the bookbuilder function
  select seqno,timestamp,instrumid,side,orderid,size from aoexecuted where instrumid in ijid;
  select seqno,timestamp,instrumid,side,orderid,size from coexecuted where instrumid in ijid;
  select seqno,timestamp,instrumid,side,orderid,size:0 from iodelete where instrumid in ijid)
 };

sortbook:{[bidaskbook;bidbook;askbook]
      
  update aids:aids@'o,aprcs:aprcs@'o,asizes:asizes@'o,                                              /Set up positions of prices so the sizes can be matched correctly
    bids:bids@'v,bprcs:bprcs@'v,bsizes:bsizes@'v                                                    /to their price after being put in ascending/descending order.
 
    from update v:idesc'[bprcs],o:iasc'[aprcs]
      
      from (select timestamp,instrumid,action,seqno from bidaskbook)                                /Join bid and askbook along with the time and stock.
            ,'bidbook
            ,'askbook
 };

aggregate:{[book]
  update bsizes:`int${sum each x} each b,bno:`short${count each x} each b,                          /Aggregate bid sizes and numbers for matching prices
    asizes:`int${sum each x} each a,ano:`short${count each x} each a                                /Same for ask 

    from update a:(exec asizes from book)'[til count book;(value each exec group each aprcs from book)],
      b:(exec bsizes from book)'[til count book;(value each exec group each bprcs from book)]       /Group prices and index sizes for each row of the order book
      
      from tbook:update bbid:first each bprcs,bask:first each aprcs,                                /Get top of book
        bprcs:distinct each bprcs,aprcs:distinct each aprcs from book
 };
          
                                        
bookbuilder:{[d;syms]                                                                               /Create orderbook at each stage.
  bookbuildschema:([orderid:`long$()]price:`float$();size:`int$());
 
  ijid:raze instd c;                                                                                /Get instrument ids for relevent instruments

  itchdatapieces:getdatapieces[ijid];
 
  addordertable:select seqno,timestamp,instrumid,side,orderid,size,price from oadd where instrumid in ijid;
  addiordertable:select seqno,timestamp,instrumid,side,orderid,size,price from ioadd where instrumid in ijid;
  addioreplacetable:select seqno,timestamp,instrumid,side,orderid,size,price from ioreplace where instrumid in ijid;

  addordertable:delete pricefrac from update price:price%pricefrac from                             /Update price from each table by dividing the price by the 
    ij[addordertable;1!select instrumid,pricefrac from futuresd];                                   /fractional denominator to yield the floating point price
  addiordertable:delete pricefrac from update price:price%pricefrac from
    ij[addiordertable;1!select instrumid,pricefrac from futuresd];
  addioreplacetable:delete pricefrac from update price:price%pricefrac from                         /Fractional denominator taken from stockdirectory for each id 
    ij[addioreplacetable;1!select instrumid,pricefrac from futuresd];

  itchdata:update `g#action,`g#instrumid,`g#side from `seqno xasc select from                       /Apply g attributes to speed reading for the bookbuilding
    uj/[
      {[x;t]update action:x from t}'["ADEXCekjl";                                                   /Give tables their actions
        enlist[addordertable],itchdatapieces,enlist[addiordertable],enlist addioreplacetable]];     

  offset:([]seqno:til exec max seqno from itchdata);                                                /Table containing all the sequence numbers
  offtab:aj[`seqno;offset;select from timemsg];                                                     /Join relevent time offset to each seqence number

  bidaskbook:update book:bookbuild\[("BS"!2#enlist bookbuildschema);action;orderid;side;size;price] /Build two books, the bidbook and askbook.
    by instrumid from itchdata;
 
  bidbook:(`bids`bprcs`bsizes xcol flip each 0!'(value'[exec book from bidaskbook])[;0]);           /Extract the first book from the book col.
  askbook:(`aids`aprcs`asizes xcol flip each 0!'(value'[exec book from bidaskbook])[;1]);           /Same for the second.

  sbook:sortbook[bidaskbook;bidbook;askbook];

  book:update stock:instdr instrumid from                                                           /Add instrument name to the book
    select timestamp,instrumid,bbid,bbsize,bask,basize,action,seqno,bprcs,bsizes,bno,bids,aprcs,asizes,ano,aids /Select relevent columns for the book
      from update bbsize:first each bsizes,basize:first each asizes                                 /Get best bid and ask
        from aggregate[sbook];

  book:update timestamp:`timestamp$1970.01.01+(second+timestamp%1000000000)%86400 from              /Add offset to timestamp and convert time 
    aj[`seqno;book;offtab];                                                                         /Join time offset to each order book message

  book:`date`timestamp`stock xcols delete instrumid,second from book                                /Reorder and delete columns
  ;book
 };

savebook:{[d;tablename;stock] hsym[`$"/" sv string(d;tablename;`)] 
  upsert .Q.en[`:.;uj/[bookbuilder[d;] peach stock]];.Q.gc[]};	                                    /Upsert each orderbook into an on disk table. This function will take in the hdb, 
                                                                                                    /a date and a list of syms. Books can be built
                                                                                                    /using multiple threads. Garbage collection is used so these books can be 
                                                                                                    /"forgotten" thus freeing memory for the next batch.
                                                    
/############################### Runtime function ###############################

init:{[o]
  gettables[o];                                                                                     /Load HDB
  $[any `=o`stock;                                                                                  /Check if stock specified in command line
    c::distinct exec `$instrument from select from futuresd;                                        /Get all stock
    c::(),o`stock];                                                                                 /Get specified stock for the order book
  savebook[o`date;o`tablename;] each o[`size] cut c;
  `stock`seqno`timestamp xasc hsym `$"/" sv string (`.;o`date;o`tablename);                         /Sort book by stock, sequence number and time
  @[hsym `$"/" sv string (`.;o`date;o`tablename;`);`stock;`p#];                                     /Add p attribute for fast lookup 
  if[not `noexit in key o;exit[0]]
  };

if[p`init;@[init;p;{[p;x] -1 string[.z.p]," A fatal error occurred due to ",raze string x;
  if[not `noexit in key p;exit[0]]}p]];

