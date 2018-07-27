/############################### User inputs ###############################
p:.Q.def[`init`date`size`hdb`tablename`stock!(1b;.z.d;100;`HDB;`book;enlist `)].Q.opt .z.x

usage:{-1
  "
  ####################################### ITCH bookbuilder ################################################\n
  This script is used with the tables created by itchparser.q to build an orderbook for a day's trading.   \n
  The sample usage is as follows:                                                                          \n
  q itchbookbuildernasdaq.q -init 1 -date 2017.07.28 -size 50 -hdb HDB -tablename book -stock SPY          \n
  init is a boolean which tells q to build and save the orderbook automatically. The dafault value is 1    \n
  date will default to today's date if none is provided                                                    \n
  size is the number of stocks to build the orderbook of at any one time. This is done to prevent memory   \n
  issues. It defaults to 100 stocks which was fast when tested on a machine with 16048 MB of RAM and 8     \n
  cores.                                                                                                   \n
  stock is the list of stock to build the orderbook, the default is all                                    \n
  hdb is the location of the parsed itch files. The orderbook will save in this directory. The default 	   \n
  argument is HDB/							                                   \n
  tablename is what you wish to call the orderbook within the hdb. The default argument is book	           \n"
  ;exit[0]}
if[`usage in key p;usage[]]

/############################### Create pidmapping ###############################
gettables:{[o]
  system"l ",string[o`hdb],"/";
  replacetab::select from oreplace where date=o`date;	                                            /Obtain data for in memory table operations, i.e. union join.
  addtab::select from oadd where date=o`date;
  addmpidtab::select from oaddmpid where date=o`date;
  deletetab::select from odelete where date=o`date;
  executedtab::select from oexecuted where date=o`date;
  canceltab::select from ocancel where date=o`date;
  executedwptab::select from oexecutedwp where date=o`date;
 }
 
tableprep:{
  dict:exec(`u#origorderref)!neworderref from replacetab;                                           /Create a mapping where original order references are mapped to their new id.
  notreplaced:(uj/[{[x;y](select ids:orderref,stock,pid:orderref,side                               /Get those ids which are never replaced -and therefore don't appear in the oreplace table. 
    from x where not orderref in y)}[;key dict] each `addtab`addmpidtab]);
  idmapper:-1_'@[dict]\'[`u#exec origorderref except neworderref from replacetab];                  /Make lists starting with the parent id and all child ids oldest to newest.
  pidmapping:
  (
  ungroup											                                                                       
    {[a];([pid:a[;0]]ids:a) lj 
      1!uj/[{[x]select pid:orderref,stock,side from x} each (addtab;addmpidtab)]}idmapper           /Join the stock and side to the idmapper, as these do not appear anywhere but the add tables.
  )                                                                                                 /Ungroup is used to obtain a table with every id alongside it's parent id, stock and side.
  uj notreplaced                                                                                    /Join this to those ids which do not get replaced.
  ;pidmapping
 }

/############################### Building the orderbook ###############################

bookbuild:{[t;act;ref;sd;sz;px;tm]
  t:@[t;sd;,;
    $[act in "EXC";`pid`shares!(ref;0^t[sd][ref;`shares]-sz);                                       /If the action is exec remove the correct number of shares, else preform an upsert.
      `pid`shares`price!(ref;sz;px)]];
  if[0=t[sd][ref;`shares];                                                                          /If the number of shares in an order is zero, delete it. This takes care if the case
    t:@[t;sd;_;ref]];
  t
 };                                                                                                 /when the action is delete.

getdatapieces:{[ijid](                                                                              /Since there is no stock to select, use ijid -based on symids above- to select by stock.
  select seqno,time,shares:0,ids:`u#orderref from deletetab where orderref in ijid;
  select seqno,time,shares,price,ids:`u#origorderref from replacetab where origorderref in ijid;    /This is a list of tables which we will then assign actions based on which table they came from.
  select seqno,time,shares,ids:orderref from executedtab where orderref in ijid;                    /This is used in the bookbuilder function.
  select seqno,time,shares:cancelled,ids:orderref from canceltab where orderref in ijid;
  select seqno,time,shares,ids:orderref from executedwptab where orderref in ijid)
 };

sortbook:{[bidaskbook;bidbook;askbook]
      
  update apids:apids@'o,aprcs:aprcs@'o,asizes:asizes@'o,                                            /Set up positions of the prices so the sizes can be matched correctly
    bpids:bpids@'v,bprcs:bprcs@'v,bsizes:bsizes@'v                                                  /to their price after being put in ascending/descending order.
 
    from update v:idesc'[bprcs],o:iasc'[aprcs]
      
      from (select time,stock,action,seqno from bidaskbook)                                         /Join bid and askbook along with the time and stock.
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
  bookbuildschema:([pid:`long$()]price:`float$();shares:`int$());
 
  symids:`ids xkey select from pidmapping where stock in syms;                                      /Select relevant entries from the pidmapping for use with an inner join.
 
  ijid:exec ids from symids;

  itchdatapieces:getdatapieces[ijid];

  addordertable:select seqno,time,ids:orderref,side,shares,stock,price from addtab where stock in syms;
  addmpidordertable:select seqno,time,ids:orderref,side,shares,stock,price from addmpidtab where stock in syms;
  addtables:enlist[addordertable],enlist addmpidordertable;

  itchdata:update `g#action,`g#stock,`g#side from `seqno xasc delete id from                        /Apply g attributes to speed reading for the bookbuilding.
    uj/[
      {[x;t]update action:x from t}'["AFDUEXC";                                                     /Give tables their actions
    ij\:[addtables,itchdatapieces;symids]]                                                          /Join the stock col to each of the tables found in the itchdatapieces. 
                                                                                                    /The add table is added to this list -no need to 
  ];	                                             											    /add the stock col here as it is already on this table.
 
 
  bidaskbook:update book:bookbuild\[("BS"!2#enlist bookbuildschema);action;pid;side;shares;price;time]  /Build two books, the bidbook and askbook.
    by stock from itchdata;
 
  bidbook:(`bpids`bprcs`bsizes xcol flip each 0!'(value'[exec book from bidaskbook])[;0]);          /Extract the first book from the book col.
  askbook:(`apids`aprcs`asizes xcol flip each 0!'(value'[exec book from bidaskbook])[;1]);          /Same for the second.

  sbook:sortbook[bidaskbook;bidbook;askbook];

  book:select time,stock,bbid,bbsize,bask,basize,action,seqno,bprcs,bsizes,bno,bpids,aprcs,asizes,ano,apids 
    from update bbsize:first each bsizes,basize:first each asizes 
      from aggregate[sbook] 
  ;book
 };

savebook:{[d;tablename;stock] hsym[`$"/" sv string(d;tablename;`)] 
  upsert .Q.en[`:.;update value stock from uj/[bookbuilder[d;] peach stock]];.Q.gc[]};	            /Upsert each orderbook into an on disk table. This function will take in the hdb, 
                                                                                                    /a date and a list of syms. Books can be built
                                                                                                    /using multiple threads. Garbage collection is used so these books can be 
                                                                                                    /"forgotten" thus freeing memory for the next batch.
                                                    
/############################### Runtime function ###############################

init:{[o] 
  gettables[o];                                                                                     /Load HDB.
  pidmapping::tableprep[];                                                                          /Create and save pidmapping.
  .Q.dpft[hsym `:.;o`date;`stock;`pidmapping];
  $[any `=o`stock;
    c:value exec distinct stock from select stock from pidmapping;
    c:(),o`stock];                                                                                  /Obtains stocks in which to build orderbooks.
  savebook[o`date;o`tablename;] each o[`size] cut c;
  `stock`seqno`time xasc hsym `$"/" sv string (`.;o`date;o`tablename);
  @[hsym `$"/" sv string (`.;o`date;o`tablename;`);`stock;`p#];                                     /Adds the p attribute for fast lookup.
  if[not `noexit in key o;exit[0]]
  };

if[p`init;@[init;p;{[p;x] -1 string[.z.p]," A fatal error occurred due to ",raze string x;
  if[not `noexit in key p;exit[0]]}p]];
